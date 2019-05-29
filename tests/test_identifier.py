import pytest

from ae5_tools.identifier import Identifier
from itertools import product
from uuid import uuid4


VALID_SLUGS = (('a0', 'projects'),
               ('a1', 'sessions'),
               ('a2', 'deployments'),
               ('a3', 'channels'))

INVALID_SLUG = 'ff'


def _assert_invalid_id(idstr):
    with pytest.raises(ValueError) as excinfo:
        Identifier.id_type(idstr)
    assert 'Invalid identifier' in str(excinfo)
    assert idstr in str(excinfo)


def _assert_valid_id(idstr, type=None):
    assert Identifier.id_type(idstr) == type or type is None


def _assert_empty_fields(ident, *fields):
    for field in fields:
        assert getattr(ident, field) == '', (ident, field)


def _assert_round_trip(ident):
    return Identifier.from_string(str(ident)) == ident


def test_id_type_with_valid_ids():
    for prefix, type in VALID_SLUGS:
        for attempt in range(10):
            suffix = uuid4().hex
            _assert_valid_id(f'{prefix}-{suffix}', type)


def test_id_type_with_invalid_structure():
    for idstr in ('', 'hello', '123-456-890', str(uuid4()), uuid4().hex, 'b4-'+ uuid4().hex):
        _assert_invalid_id(idstr)


def test_id_type_with_invalid_prefixes():
    all_prefixes = set(prefix for prefix, _ in VALID_SLUGS)
    for attempt in range(10):
        prefix = uuid4().hex[:2]
        while prefix in all_prefixes:
            prefix = uuid4().hex[:2]
        suffix = uuid4().hex
        _assert_invalid_id(f'{prefix}-{suffix}')


def test_id_type_with_invalid_suffixes():
    for prefix, _ in VALID_SLUGS:
        for suffix in (str(uuid4()), 'abcdefghijklmnopqrstuvwxyzabcd', '0123456789'):
            _assert_invalid_id(f'{prefix}-{suffix}')


def test_id_prefix_with_valid_types():
    for prefix, type in VALID_SLUGS:
        assert Identifier.id_prefix(type) == prefix


def test_valid_id_pair():
    for p1, t1 in VALID_SLUGS:
        for p2, t2 in VALID_SLUGS:
            if (t1 == 'projects') + (t2 == 'projects') == 1:
                s1, s2 = uuid4().hex, uuid4().hex
                idstr = f'{p1}-{s1}/{p2}-{s2}'
                ident = Identifier.from_string(idstr)
                if t1 == 'projects':
                    assert str(ident) == idstr, (t1, t2)


def test_invalid_id_pair():
    for p1, t1 in VALID_SLUGS:
        for p2, t2 in VALID_SLUGS:
            if (t1 == 'projects') + (t2 == 'projects') != 1:
                s1, s2 = uuid4().hex, uuid4().hex
                while p1 == p2 and s1 == s2:
                    s2 = uuid4().hex
                with pytest.raises(ValueError) as excinfo:
                    Identifier.from_string(f'{p1}-{s1}/{p2}-{s2}')


def _valid_id_sets():
    yield ''
    for p1, t1 in VALID_SLUGS:
        yield p1
        for p2, t2 in VALID_SLUGS:
            if ((t1 == 'projects') + (t2 == 'projects')) >= 1:
                yield f'{p1}/{p2}'


def _invalid_id_sets():
    yield INVALID_SLUG
    for p1, t1 in VALID_SLUGS:
        yield f'{INVALID_SLUG}/{p1}'
        yield f'{p1}/{INVALID_SLUG}'
        for p2, t2 in VALID_SLUGS:
            if ((t1 == 'projects') + (t2 == 'projects')) != 1:
                yield f'{p1}/{p2}'


def _build_idstr(prefixes, valid=True):
    prefixes = prefixes.split('/')
    suffixes = [uuid4().hex for _ in prefixes]
    # Valid id pairs do not have two different ids with the same type.
    if valid and len(prefixes) == 2 and prefixes[0] == prefixes[1]:
        suffixes[0] = suffixes[1]
    print(prefixes, suffixes, valid)
    ids = [f'{p}-{s}' for p, s in zip(prefixes, suffixes)]
    idstr = '/'.join(ids)
    if len(ids) == 1:
        id = ids[0]
        pid = id if Identifier.id_type(id, quiet=True) == 'projects' else ''
    elif Identifier.id_type(ids[0], quiet=True) == 'projects':
        pid, id = ids
    else:
        id, pid = ids
    return idstr, pid, id


@pytest.mark.parametrize('revision', ('None', '*', 'z', 'z*'))
@pytest.mark.parametrize('prefixes', list(_valid_id_sets()))
@pytest.mark.parametrize('name', ('None', 'Empty', '*', 'y', 'y*'))
@pytest.mark.parametrize('owner', ('None', 'Empty', '*', 'x', 'x*'))
def test_from_string_valids(owner, name, prefixes, revision):
    revision = None if revision == 'None' else revision
    owner = None if owner == 'None' else ('' if owner == 'Empty' else owner)
    name = None if name == 'None' else ('' if name == 'Empty' else name)
    id = pid = ''
    if prefixes:
        idstr, pid, id = _build_idstr(prefixes, True)
        if owner is not None:
            idstr = f'{owner}/{name or ""}/{idstr}'
        elif name is not None:
            idstr = f'{name}/{idstr}'
    elif owner is not None:
        idstr = f'{owner}/{name or ""}'
    else:
        idstr = name or ''
    if revision:
        idstr = f'{idstr}:{revision}'
    ident = Identifier.from_string(idstr)
    assert ident.owner == ('' if owner in (None, '*') else owner), idstr
    assert ident.name == ('' if name in (None, '*') else name), idstr
    assert ident.revision == ('' if revision in (None, '*') else revision), idstr
    assert ident.id == id, idstr
    assert ident.pid == pid, idstr


@pytest.mark.parametrize('revision', ('None', '*', 'z', 'z*'))
@pytest.mark.parametrize('prefixes', list(_invalid_id_sets()))
@pytest.mark.parametrize('name', ('None', 'Empty', '*', 'y', 'y*'))
@pytest.mark.parametrize('owner', ('None', 'Empty', '*', 'x', 'x*'))
def test_from_string_invalids(owner, name, prefixes, revision):
    revision = None if revision == 'None' else revision
    owner = None if owner == 'None' else ('' if owner == 'Empty' else owner)
    name = None if name == 'None' else ('' if name == 'Empty' else name)
    id = pid = ''
    if prefixes:
        idstr, pid, id = _build_idstr(prefixes, False)
        if owner is not None:
            idstr = f'{owner}/{name or ""}/{idstr}'
        elif name is not None:
            idstr = f'{name}/{idstr}'
    elif owner is not None:
        idstr = f'{owner}/{name or ""}'
    else:
        idstr = name or ''
    if revision:
        idstr = f'{idstr}:{revision}'
    print(idstr)
    with pytest.raises(ValueError) as excinfo:
        Identifier.from_string(idstr)
