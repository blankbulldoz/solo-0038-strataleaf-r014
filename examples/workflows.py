import json
import sys
import tempfile
from pathlib import Path
from strataleaf import Database, Archives, Conflict, InvalidDocument, InvalidCursor, ReadOnly, SnapshotTooOld, ReplicationError, jsonable

SCHEMA = {"fields": {"region": "string", "sequence": "integer", "code": "string"}, "required": ["region", "sequence"]}


def seed(db):
    db.catalog.create("segments", SCHEMA)
    batch = db.batch("initial_segments")
    for index in range(6):
        batch.put("segments", f"seg_{index:02}", {"region": "east" if index % 2 == 0 else "west", "sequence": index, "code": "c" + str(index)})
    return batch.commit().revision


def rejected(call, error):
    try:
        call()
    except error as exc:
        return {"error": type(exc).__name__, "message": str(exc)}
    raise RuntimeError("operation unexpectedly succeeded")


def run(flow):
    with tempfile.TemporaryDirectory(prefix="strataleaf-example-") as directory:
        folder = Path(directory)
        with Database(folder / "source.db") as db:
            first = seed(db)
            if flow == "catalog":
                output = {"collections": db.catalog.list(), "duplicate": rejected(lambda: db.catalog.create("segments"), Conflict)}
            elif flow == "schema":
                updated = {**SCHEMA, "fields": {**SCHEMA["fields"], "note": "string"}}
                generation = db.catalog.evolve("segments", updated, 1)
                failure = rejected(lambda: db.catalog.evolve("segments", {**updated, "required": ["note"]}, generation), InvalidDocument)
                output = {"generation": generation, "after_rejection": db.catalog.describe("segments"), "incompatible": failure}
            elif flow == "commit":
                db.catalog.create("marks")
                result = db.batch("dual_write").put("segments", "seg_00", {"region": "south", "sequence": 7}, expected=first).put("marks", "m0", {"segment": "seg_00"}).commit()
                replay = db.batch("dual_write").put("segments", "seg_00", {"region": "south", "sequence": 7}, expected=first).put("marks", "m0", {"segment": "seg_00"}).commit()
                conflict = rejected(lambda: db.batch().put("marks", "discard", {}).put("segments", "seg_01", {"region": "x", "sequence": 1}, expected=0).commit(), Conflict)
                if db.get("marks", "discard") is not None or not replay.replayed:
                    raise RuntimeError("atomicity or replay observation failed")
                output = {"commit": result.__dict__, "replay": replay.__dict__, "conflict": conflict}
            elif flow == "snapshot":
                db.batch().delete("segments", "seg_00", expected=first).commit()
                output = {"past": db.get("segments", "seg_00", first).__dict__, "present": db.get("segments", "seg_00"),
                          "future": rejected(lambda: db.get("segments", "seg_00", db.revision + 1), InvalidDocument)}
            elif flow == "query":
                query = {"field": "sequence", "op": "ge", "value": 0}
                page = db.query("segments", query, limit=2)
                db.batch().delete("segments", "seg_02").put("segments", "seg_07", {"region": "west", "sequence": 7}).commit()
                keys = [row.key for row in page.records]
                cursor = page.cursor
                while cursor:
                    next_page = db.query("segments", query, limit=2, cursor=cursor)
                    keys.extend(row.key for row in next_page.records)
                    cursor = next_page.cursor
                if keys != [f"seg_{i:02}" for i in range(6)]:
                    raise RuntimeError("read revision changed during pagination")
                output = {"revision": page.revision, "keys": keys, "foreign_query": rejected(lambda: db.query("segments", cursor=page.cursor), InvalidCursor)}
            elif flow == "index":
                db.indexes.create("segments", "by_region", "region")
                db.indexes.create("segments", "unique_code", "code", unique=True)
                predicate = {"field": "region", "op": "eq", "value": "east"}
                indexed = db.query("segments", predicate)
                scanned = db.query("segments", predicate, force_scan=True)
                if indexed.records != scanned.records:
                    raise RuntimeError("index and scan differ")
                collision = rejected(lambda: db.batch().put("segments", "new", {"region": "north", "sequence": 8, "code": "c0"}).commit(), Conflict)
                output = {"plan": indexed.plan, "keys": [row.key for row in indexed.records], "collision": collision, "integrity": db.diagnostics.inspect(True)}
            elif flow == "feed":
                db.feed.register("mirror", 0)
                db.batch().delete("segments", "seg_00").commit()
                commits = db.feed.read(0, limit=1)
                db.feed.acknowledge("mirror", commits[-1]["revision"])
                output = {"complete_commit": commits, "ack": db.feed.position("mirror"),
                          "backward": rejected(lambda: db.feed.acknowledge("mirror", 0), Conflict)}
            elif flow == "backup":
                backup = db.archives.backup(folder / "backup.db")
                db.batch().delete("segments", "seg_00").commit()
                bad = rejected(lambda: Archives.restore(folder / "backup.db", folder / "invalid.db", "wrong"), Exception)
                Archives.restore(folder / "backup.db", folder / "restored.db", backup["sha256"])
                with Database(folder / "restored.db") as restored:
                    output = {"backup": backup, "restored": restored.diagnostics.fingerprint(), "bad_checksum": bad}
                    if restored.revision != first:
                        raise RuntimeError("backup read boundary changed")
            elif flow == "replica":
                db.archives.backup(folder / "base.db")
                Archives.restore(folder / "base.db", folder / "replica.db", follower=True)
                db.batch().put("segments", "seg_00", {"region": "north", "sequence": 19}).commit()
                db.batch().delete("segments", "seg_01").commit()
                db.indexes.create("segments", "by_region", "region")
                with Database(folder / "replica.db") as replica:
                    part = db.replication.export(replica.revision, limit=1)
                    applied = replica.replication.apply(part)
                    tail = db.replication.export(replica.revision)
                    replica.replication.apply(tail)
                    repeat = replica.replication.apply(tail)
                    readonly = rejected(lambda: replica.batch().delete("segments", "seg_00").commit(), ReadOnly)
                    if replica.diagnostics.fingerprint() != db.diagnostics.fingerprint() or not replica.diagnostics.inspect(True)["ok"]:
                        raise RuntimeError("replica differs from source")
                    output = {"first_packet": applied, "repeat": repeat, "readonly": readonly, "fingerprint": replica.diagnostics.fingerprint()}
            elif flow == "compact":
                db.retention.pin("reader", first)
                db.feed.register("slow_copy", first)
                db.batch().put("segments", "seg_00", {"region": "north", "sequence": 8}).commit()
                blocked = db.retention.compact(db.revision)
                old = db.get("segments", "seg_00", first)
                db.retention.unpin("reader")
                db.feed.acknowledge("slow_copy", db.revision)
                advanced = db.retention.compact(db.revision)
                expired = rejected(lambda: db.get("segments", "seg_00", first), SnapshotTooOld)
                replay = db.batch("initial_segments")
                for index in range(6):
                    replay.put("segments", f"seg_{index:02}", {"region": "east" if index % 2 == 0 else "west", "sequence": index, "code": "c" + str(index)})
                output = {"blocked": blocked, "old_value": old.document, "advanced": advanced, "expired_read": expired, "replay_after_compaction": replay.commit().__dict__}
            elif flow == "expire":
                written = db.batch().put("segments", "timed", {"region": "north", "sequence": 10}, expires_at=100).commit()
                before = db.get("segments", "timed")
                swept = db.retention.sweep(100)
                if db.get("segments", "timed") is not None or db.get("segments", "timed", written.revision) is None:
                    raise RuntimeError("expiry did not preserve historical visibility")
                output = {"before": before.__dict__, "sweep": swept, "feed": db.feed.read(written.revision), "idle": db.retention.sweep(100)}
            elif flow == "aggregate":
                output = db.aggregates.group("segments", "region", "sequence", revision=first)
                if sum(item["count"] for item in output["groups"]) != 6:
                    raise RuntimeError("aggregation lost visible documents")
                output["invalid"] = rejected(lambda: db.aggregates.group("unknown", "region"), Exception)
            elif flow == "patch":
                result = db.patch("segments", "seg_00", {"code": None, "detail": {"block": 4}}, expected=first)
                output = {"patch": result.__dict__, "document": db.get("segments", "seg_00").document,
                          "stale": rejected(lambda: db.patch("segments", "seg_00", {"sequence": 8}, expected=first), Conflict)}
            elif flow == "inspect":
                db.indexes.create("segments", "by_code", "code")
                output = {"inspection": db.diagnostics.inspect(deep=True), "fingerprint": db.diagnostics.fingerprint(), "checkpoint": db.checkpoint()}
                if not output["inspection"]["ok"]:
                    raise RuntimeError("integrity inspection failed")
            elif flow == "transfer":
                exported = db.transfer.export_lines("segments")
                db.catalog.create("copy_segments", SCHEMA)
                imported = db.transfer.import_lines("copy_segments", exported["lines"])
                output = {"revision": exported["revision"], "imported": imported.__dict__,
                          "duplicate": rejected(lambda: db.transfer.import_lines("copy_segments", exported["lines"]), Conflict)}
                if len(db.query("copy_segments").records) != 6:
                    raise RuntimeError("logical import differs")
            elif flow == "projection":
                db.catalog.create("readings", {"fields": {"region": "string"}, "required": ["region"]})
                rows = [
                    ("r01", {"region": "east", "sensor": {"celsius": 20}, "count": 4, "total": 10}),
                    ("r02", {"region": "west", "sensor": {"celsius": None}, "count": 0, "total": 5}),
                    ("r03", {"region": "east", "count": 2, "total": "oops"}),
                    ("r04", {"region": "north", "sensor": {}, "count": 3, "total": 9}),
                    ("r05", {"region": "east", "sensor": {"celsius": 100}, "count": 1, "total": 1,
                             "huge": 10 ** 1000}),
                ]
                write = db.batch("seed_readings")
                for key, document in rows:
                    write.put("readings", key, document)
                write.commit()
                projection = {"aliases": [
                    {"name": "zone", "expression": {"field": "region"}},
                    {"name": "fahrenheit", "expression": {"op": "add",
                        "left": {"op": "multiply", "left": {"field": "sensor.celsius"}, "right": {"literal": 1.8}},
                        "right": {"literal": 32}}},
                    {"name": "bucket", "expression": {"case": [
                        {"when": {"field": "sensor.celsius", "op": "ge", "value": 50}, "then": {"literal": "hot"}},
                        {"when": {"field": "sensor.celsius", "op": "exists", "value": True}, "then": {"literal": "cold"}},
                        {"default": {"literal": "unknown"}}]}},
                    {"name": "per_count", "expression": {"op": "divide",
                        "left": {"field": "total"}, "right": {"field": "count"}}},
                    {"name": "unavailable", "expression": {"op": "add",
                        "left": {"field": "sensor.celsius"}, "right": {"literal": 1}}},
                    {"name": "raw_celsius", "expression": {"field": "sensor.celsius"}},
                    {"name": "oversized", "expression": {"op": "multiply",
                        "left": {"field": "huge"}, "right": {"field": "huge"}}},
                ]}
                page = db.query("readings", projection=projection, limit=3)
                second = db.query("readings", projection=projection, limit=3, cursor=page.cursor)
                rendered = [
                    {"key": item.key, "values": jsonable(item.values), "errors": list(item.errors)}
                    for item in [*page.records, *second.records]
                ]
                expected_states = {
                    "r01": {"fahrenheit": 68.0, "bucket": "cold", "per_count": 2.5, "unavailable": 21, "raw_celsius": 20},
                    "r02": {"fahrenheit": {"$state": "not_applicable"}, "bucket": "cold",
                            "per_count": {"$state": "not_applicable"}, "unavailable": {"$state": "not_applicable"},
                            "raw_celsius": None},
                    "r03": {"per_count": {"$state": "not_applicable"},
                            "unavailable": {"$state": "missing"},
                            "raw_celsius": {"$state": "missing"}},
                    "r04": {"fahrenheit": {"$state": "missing"}, "bucket": "unknown",
                            "per_count": 3.0, "unavailable": {"$state": "missing"},
                            "raw_celsius": {"$state": "missing"}},
                    "r05": {"fahrenheit": 212.0, "bucket": "hot", "per_count": 1.0, "unavailable": 101,
                            "raw_celsius": 100, "oversized": {"$state": "not_applicable"}},
                }
                by_key = {item["key"]: item for item in rendered}
                for key, expected in expected_states.items():
                    for alias, want in expected.items():
                        if by_key[key]["values"][alias] != want:
                            raise RuntimeError(f"projection mismatch for {key}.{alias}")
                if not page.complete or second.complete:
                    raise RuntimeError("page completeness must follow per-record evaluation failures")
                if page.failures or second.failures[0]["key"] != "r05" or second.failures[0]["alias"] != "oversized":
                    raise RuntimeError("failure details must stay attached to their page and record")
                # Filtering, key ordering and projection share one fixed read view.
                snapshot = page.revision
                db.batch().delete("readings", "r01").commit()
                east = db.query("readings", {"field": "region", "op": "eq", "value": "east"},
                                projection=projection, revision=snapshot, limit=10)
                if east.revision != snapshot or [item.key for item in east.records] != ["r01", "r03", "r05"]:
                    raise RuntimeError("projected query did not honor the fixed read view")
                # A cursor is bound to its full query fingerprint, projection included.
                foreign = rejected(lambda: db.query("readings", limit=3, cursor=page.cursor), InvalidCursor)
                deep = {"op": "add", "left": {"literal": 1}, "right": {"literal": 1}}
                for _ in range(130):
                    deep = {"op": "add", "left": deep, "right": deep}
                invalid = [
                    lambda: db.query("readings", projection={"aliases": [
                        {"name": "bad", "expression": {"field": "sensor..celsius"}}]}),
                    lambda: db.query("readings", projection={"aliases": [
                        {"name": "bad", "expression": deep}]}),
                    lambda: db.query("readings", projection={"aliases": [
                        {"name": "bad", "expression": {"eval": "__import__('os')"}}]}),
                ]
                rejections = [rejected(call, InvalidDocument) for call in invalid]
                # The projection-less query path is unchanged and remains complete.
                plain = db.query("readings", limit=10, revision=snapshot)
                if not plain.complete or plain.failures or any(not hasattr(item, "document") for item in plain.records):
                    raise RuntimeError("plain query behavior changed")
                output = {"revision": snapshot, "records": rendered,
                          "first_page_complete": page.complete, "first_page_failures": page.failures,
                          "second_page_complete": second.complete, "failures": second.failures,
                          "plan": {**page.plan, "failures": page.plan["failures"]},
                          "foreign_cursor": foreign, "invalid_expressions": rejections,
                          "plain_keys": [item.key for item in plain.records]}
            else:
                raise ValueError("unknown example workflow: " + flow)
            print(json.dumps({"workflow": flow, "result": output}, ensure_ascii=False, sort_keys=True, default=str))


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "commit")
