from uuid import uuid4

import pytest

from ae5_tools.identifier import Identifier

VALID_SLUGS = (("a0", "projects"), ("a1", "sessions"), ("a2", "deployments"), ("a3", "channels"))

INVALID_SLUG = "ff"


def _assert_invalid_id(idstr):
    with pytest.raises(ValueError) as excinfo:
        Identifier.id_type(idstr)
    assert "Invalid identifier" in str(excinfo.value)
    assert idstr in str(excinfo.value)


def _assert_valid_id(idstr, type=None):
    assert Identifier.id_type(idstr) == type or type is None


def _assert_empty_fields(ident, *fields):
    for field in fields:
        assert getattr(ident, field) == "", (ident, field)


def _assert_round_trip(ident):
    return Identifier.from_string(str(ident)) == ident


def test_id_type_with_valid_ids():
    for prefix, type in VALID_SLUGS:
        for attempt in range(10):
            suffix = uuid4().hex
            _assert_valid_id(f"{prefix}-{suffix}", type)


def test_id_type_with_invalid_structure():
    for idstr in ("", "hello", "123-456-890", str(uuid4()), uuid4().hex, "b4-" + uuid4().hex):
        _assert_invalid_id(idstr)


def test_id_type_with_invalid_prefixes():
    all_prefixes = set(prefix for prefix, _ in VALID_SLUGS)
    for attempt in range(10):
        prefix = uuid4().hex[:2]
        while prefix in all_prefixes:
            prefix = uuid4().hex[:2]
        suffix = uuid4().hex
        _assert_invalid_id(f"{prefix}-{suffix}")


def test_id_type_with_invalid_suffixes():
    for prefix, _ in VALID_SLUGS:
        for suffix in (str(uuid4()), "abcdefghijklmnopqrstuvwxyzabcd", "0123456789"):
            _assert_invalid_id(f"{prefix}-{suffix}")


def test_id_prefix_with_valid_types():
    for prefix, type in VALID_SLUGS:
        assert Identifier.id_prefix(type) == prefix


def _build_idstr(prefixes, valid=True):
    prefixes = prefixes.split("/")
    suffixes = [uuid4().hex for _ in prefixes]
    # Valid id pairs do not have two different ids with the same type.
    if valid and len(prefixes) == 2 and prefixes[0] == prefixes[1]:
        suffixes[0] = suffixes[1]
    ids = [f"{p}-{s}" for p, s in zip(prefixes, suffixes)]
    idstr = "/".join(ids)
    if len(ids) == 1:
        id = ids[0]
        pid = id if Identifier.id_type(id, quiet=True) == "projects" else ""
    elif Identifier.id_type(ids[0], quiet=True) == "projects":
        pid, id = ids
    else:
        id, pid = ids
    return idstr, pid, id


def _build_string(owner, name, prefixes, revision, valid):
    if owner and not name or not (owner or name or prefixes):
        return None, None, None
    if prefixes:
        idstr, pid, id = _build_idstr(prefixes, valid)
    else:
        idstr = pid = id = ""
    if owner and name:
        label = f"{owner}/{name}"
    elif name:
        label = name
    else:
        label = ""
    if idstr and label:
        value = f"{label}/{idstr}"
    else:
        value = idstr or label
    if revision:
        value = f"{value}:{revision}"
    return value, pid, id


def test_from_string_valids():
    def _valid_id_sets():
        yield None
        for p1, t1 in VALID_SLUGS:
            yield p1
            for p2, t2 in VALID_SLUGS:
                if ((t1 == "projects") + (t2 == "projects")) >= 1:
                    yield f"{p1}/{p2}"

    for prefixes in _valid_id_sets():
        for owner in (None, "*", "x", "x*"):
            for name in (None, "*", "y", "y*"):
                for revision in (None, "*", "z", "z*"):
                    idstr, pid, id = _build_string(owner, name, prefixes, revision, True)
                    if idstr:
                        ident = Identifier.from_string(idstr)
                        assert ident.owner == ("" if owner in (None, "*") else owner), idstr
                        assert ident.name == ("" if name in (None, "*") else name), idstr
                        assert ident.revision == ("" if revision in (None, "*") else revision), idstr
                        assert ident.id == id, idstr
                        assert ident.pid == pid, idstr


def test_from_string_invalids():
    def _invalid_id_sets():
        yield INVALID_SLUG
        for p1, t1 in VALID_SLUGS:
            yield f"{INVALID_SLUG}/{p1}"
            yield f"{p1}/{INVALID_SLUG}"
            for p2, t2 in VALID_SLUGS:
                if ((t1 == "projects") + (t2 == "projects")) != 1:
                    yield f"{p1}/{p2}"

    for prefixes in _invalid_id_sets():
        for owner in (None, "*", "x", "x*"):
            for name in (None, "*", "y", "y*"):
                for revision in (None, "*", "z", "z*"):
                    idstr, pid, id = _build_string(owner, name, prefixes, revision, False)
                    if idstr:
                        ident = Identifier.from_string(idstr, quiet=True)
                        assert ident is None, idstr
