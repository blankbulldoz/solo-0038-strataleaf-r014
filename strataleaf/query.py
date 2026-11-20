from dataclasses import dataclass, field as dataclass_field
from .codec import digest
from .cursors import decode_cursor, encode_cursor
from .errors import InvalidCursor, InvalidDocument, NotFound
from .indexes import equality_candidates
from .mvcc import checked_revision, record, visible_rows, latest_row
from .predicates import equality_terms, matches, validate_predicate
from .projection import project, validate_projection


@dataclass(frozen=True)
class Page:
    records: list
    revision: int
    cursor: str | None
    plan: dict
    complete: bool = True
    failures: list = dataclass_field(default_factory=list)


@dataclass(frozen=True)
class ProjectedRecord:
    key: str
    revision: int
    values: dict
    errors: tuple = ()


class QueryEngine:
    def __init__(self, store):
        self.store = store

    def select(self, collection, predicate=None, projection=None, limit=100, cursor=None,
               revision=None, force_scan=False):
        validate_predicate(predicate)
        aliases = validate_projection(projection) if projection is not None else None
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise InvalidDocument("page size must be between 1 and 1000")
        with self.store.read() as connection:
            if not connection.execute("SELECT 1 FROM collections WHERE name=?", (collection,)).fetchone():
                raise NotFound("unknown collection")
            fingerprint = digest({"collection": collection, "predicate": predicate, "projection": projection})
            after = None
            if cursor is not None:
                payload = decode_cursor(self.store, cursor)
                if payload.get("query") != fingerprint or (revision is not None and revision != payload.get("revision")):
                    raise InvalidCursor("cursor does not belong to this query/read revision")
                revision, after = payload.get("revision"), payload.get("after")
                if not isinstance(after, str):
                    raise InvalidCursor("cursor has no key boundary")
            revision = checked_revision(self.store, revision)
            plan = {"access": "version_scan", "index": None, "projected": aliases is not None}
            candidates = None
            if not force_scan and revision == int(self.store.get("head")):
                for path, value in equality_terms(predicate):
                    indexed = equality_candidates(connection, collection, path, value)
                    if indexed is not None:
                        index_name, candidates = indexed
                        plan = {"access": "equality_index", "index": index_name, "projected": aliases is not None}
                        break
            if candidates is None:
                rows = visible_rows(connection, collection, revision)
            else:
                rows = [latest_row(connection, collection, key, revision) for key in sorted(candidates)]
            output = []
            failures = []
            examined = 0
            for row in rows:
                if row is None or row["body"] is None or (after is not None and row["key"] <= after):
                    continue
                examined += 1
                item = record(row)
                if not matches(item.document, predicate):
                    continue
                if aliases is None:
                    output.append(item)
                else:
                    # Filtering, key ordering and projection all consume this
                    # same visible document at the fixed read revision.
                    values, errors = project(item.document, aliases)
                    projected_record = ProjectedRecord(item.key, item.revision, values, tuple(errors))
                    output.append(projected_record)
                    for error in errors:
                        failures.append({"key": item.key, **error})
                if len(output) > limit:
                    break
            continuation = None
            if len(output) > limit:
                output = output[:limit]
                continuation = encode_cursor(self.store, {"database": self.store.get("database_id"),
                    "query": fingerprint, "revision": revision, "after": output[-1].key})
            plan = {**plan, "examined": examined, "failures": len(failures)}
            return Page(output, revision, continuation, plan, complete=not failures, failures=failures)
