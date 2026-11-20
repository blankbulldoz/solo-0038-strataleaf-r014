"""Declarative projection expressions.

Projections are plain JSON-compatible dictionaries, never Python source: they
are validated against a fixed node vocabulary and interpreted here. No eval,
exec or compile is used anywhere on caller-supplied expressions.

Three non-value states are kept distinct end to end:

* MISSING  - a field path is absent from the document (or a case has no match);
* JSON null (``None``) - the field exists and explicitly holds null;
* NA       - an operation is not applicable (non-numeric arithmetic, division
             by zero, an integer result exceeding the resource boundary).

Both MISSING and NA survive projection evaluation and are handed back to the
caller unchanged; ``jsonable`` renders them as explicit tagged strings so they
cannot be confused with one another or with null.
"""
from .codec import MISSING, dumps, field
from .errors import InvalidDocument
from .predicates import matches, validate_predicate

MAX_ALIASES = 64
MAX_EXPRESSION_BYTES = 16 * 1024
MAX_NODES = 256
MAX_DEPTH = 16
MAX_CASE_BRANCHES = 32
# Computed integers may not exceed this many bits; the cap bounds per-record
# work and memory regardless of how operatively nested an expression is.
MAX_RESULT_BITS = 4096

ARITHMETIC_OPS = {"add", "subtract", "multiply", "divide", "modulo"}
NA = object()


def validate_projection(projection):
    """Validate and normalize a projection request; return the alias list."""
    if not isinstance(projection, dict) or set(projection) != {"aliases"}:
        raise InvalidDocument("projection must be an object with an aliases array")
    aliases = projection["aliases"]
    if not isinstance(aliases, list) or not 1 <= len(aliases) <= MAX_ALIASES:
        raise InvalidDocument(f"projection requires 1..{MAX_ALIASES} aliases")
    if len(dumps(projection).encode()) > MAX_EXPRESSION_BYTES:
        raise InvalidDocument("projection expression exceeds 16 KiB")
    names = set()
    for entry in aliases:
        if not isinstance(entry, dict) or set(entry) != {"name", "expression"}:
            raise InvalidDocument("each alias needs a name and an expression")
        name = entry["name"]
        if not isinstance(name, str) or not name or "." in name or len(name.encode()) > 256:
            raise InvalidDocument("alias names are nonempty strings without dots, at most 256 bytes")
        if name in names:
            raise InvalidDocument("duplicate projection alias: " + name)
        names.add(name)
        _validate_expression(entry["expression"], depth=0, nodes=[0])
    return aliases


def _validate_expression(expression, depth, nodes):
    nodes[0] += 1
    if nodes[0] > MAX_NODES:
        raise InvalidDocument(f"projection exceeds {MAX_NODES} expression nodes")
    if depth > MAX_DEPTH or not isinstance(expression, dict):
        raise InvalidDocument("invalid or too deeply nested projection expression")
    keys = set(expression)
    if keys == {"literal"}:
        try:
            dumps(expression["literal"])
        except InvalidDocument:
            raise InvalidDocument("projection literal must be a finite JSON value")
        return
    if keys == {"field"}:
        field({}, expression["field"])
        return
    if keys == {"op", "left", "right"}:
        op = expression["op"]
        if not isinstance(op, str) or op not in ARITHMETIC_OPS:
            raise InvalidDocument("unsupported arithmetic operator: " + str(op))
        _validate_expression(expression["left"], depth + 1, nodes)
        _validate_expression(expression["right"], depth + 1, nodes)
        return
    if keys == {"case"}:
        branches = expression["case"]
        if not isinstance(branches, list) or not 1 <= len(branches) <= MAX_CASE_BRANCHES:
            raise InvalidDocument(f"case requires 1..{MAX_CASE_BRANCHES} branches")
        for branch in branches:
            if not isinstance(branch, dict) or set(branch) not in ({"when", "then"}, {"default"}):
                raise InvalidDocument("case branches are when/then pairs optionally followed by default")
            if "when" in branch:
                validate_predicate(branch["when"])
                _validate_expression(branch["then"], depth + 1, nodes)
            else:
                _validate_expression(branch["default"], depth + 1, nodes)
        if sum("default" in branch for branch in branches) > 1:
            raise InvalidDocument("case has at most one default branch")
        return
    raise InvalidDocument("unrecognized projection expression node")


def _is_number(value):
    # Booleans are never numeric, matching predicate and schema semantics.
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _arithmetic(op, left, right):
    if not _is_number(left) or not _is_number(right):
        return NA
    try:
        if op == "add":
            result = left + right
        elif op == "subtract":
            result = left - right
        elif op == "multiply":
            result = left * right
        elif op == "divide":
            if right == 0:
                return NA
            result = left / right
        else:  # modulo
            if right == 0:
                return NA
            result = left % right
    except (OverflowError, ZeroDivisionError, ArithmeticError):
        return NA
    if isinstance(result, float) and not result == result:  # NaN guard
        return NA
    if isinstance(result, int) and result.bit_length() > MAX_RESULT_BITS:
        raise InvalidDocument(f"integer projection result exceeds {MAX_RESULT_BITS} bits")
    return result


def _evaluate(expression, document):
    if "literal" in expression:
        return expression["literal"]
    if "field" in expression:
        # MISSING propagates as its own state; null and values pass through.
        return field(document, expression["field"])
    if "op" in expression:
        left = _evaluate(expression["left"], document)
        right = _evaluate(expression["right"], document)
        # Absence is transparent: a missing operand means the whole derived
        # value is absent, distinct from an operation that exists but cannot
        # apply (null/non-numeric operands, zero division), which becomes NA.
        if left is MISSING or right is MISSING:
            return MISSING
        if left is NA or right is NA:
            return NA
        return _arithmetic(expression["op"], left, right)
    branches = expression["case"]
    for branch in branches:
        if "default" in branch:
            return _evaluate(branch["default"], document)
        if matches(document, branch["when"]):
            return _evaluate(branch["then"], document)
    return MISSING


def project(document, aliases):
    """Evaluate validated aliases against one document.

    Returns (values, errors). An error on one alias never aborts the others:
    the failed alias yields NA and is recorded in errors so the caller can
    tell a complete page from a partial one.
    """
    values, errors = {}, []
    for entry in aliases:
        name = entry["name"]
        try:
            values[name] = _evaluate(entry["expression"], document)
        except InvalidDocument as exc:
            values[name] = NA
            errors.append({"alias": name, "reason": str(exc)})
    return values, errors


def jsonable(value):
    """Render projection output, including the two sentinel states, as JSON."""
    if value is MISSING:
        return {"$state": "missing"}
    if value is NA:
        return {"$state": "not_applicable"}
    if isinstance(value, dict):
        return {key: jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [jsonable(item) for item in value]
    return value
