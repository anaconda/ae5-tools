import pytest

from .utils import _cmd


@pytest.fixture(scope='module')
def project_list(user_session):
    return _cmd('project list')


@pytest.fixture(scope='module')
def project_dup_names(project_list):
    counts = {}
    for p in project_list:
        counts[p['name']] = counts.get(p['name'], 0) + 1
    return sorted(p for p, v in counts.items() if v > 1)


def test_owner(user_session, project_list):
    uname = user_session.username
    first_list = None
    for cmd in (f'project list {uname}/*',
                f'project list --filter owner={uname}'):
        plist = _cmd(cmd)
        if first_list is None:
            assert all(p['owner'] == uname for p in plist)
            first_list = plist
        else:
            assert plist == first_list


def test_name(project_dup_names):
    pname = project_dup_names[0]
    first_list = None
    for cmd in (f'project list {pname}',
                f'project list */{pname}',
                f'project list --filter name={pname}'):
        plist = _cmd(cmd)
        if first_list is None:
            assert len(plist) > 1
            assert all(p['name'] == pname for p in plist)
            first_list = plist
        else:
            assert plist == first_list


def test_owner_name(user_session, project_dup_names):
    uname = user_session.username
    pname = project_dup_names[0]
    first_list = None
    for cmd in (f'project list {uname}/{pname}',
                f'project list {uname}/* --filter name={pname}',
                f'project list --filter name={pname} {uname}/*',
                f'project list */{pname} --filter owner={uname}',
                f'project list --filter owner={uname} */{pname}',
                f'project list {pname} --filter owner={uname}',
                f'project list --filter owner={uname} --filter name={pname}',
                f'project list --filter name={pname} --filter owner={uname}',
                f'project list --filter owner={uname},name={pname}',
                f'project list --filter "owner={uname}&name={pname}"'):
        plist = _cmd(cmd)
        if first_list is None:
            assert len(plist) == 1
            assert all(p['name'] == pname or p['owner'] == uname for p in plist)
            first_list = plist
        else:
            assert plist == first_list


def test_boolean_pipe(user_session, project_dup_names):
    uname = user_session.username
    pname = project_dup_names[0]
    plist = _cmd(f'project list --filter "name={pname}|owner={uname}"')
    first_list = None
    for cmd in (f'project list --filter "name={pname}|owner={uname}"',
                f'project list --filter "owner={uname}|name={pname}"'):
        plist = _cmd(cmd)
        if first_list is None:
            assert(p['name'] == pname or p['owner'] == uname for p in plist)
            assert(any(p['name'] != pname for p in plist))
            assert(any(p['owner'] != uname for p in plist))
            first_list = plist
        else:
            assert plist == first_list


def test_boolean_comma(user_session, project_dup_names):
    uname = user_session.username
    pname = project_dup_names[0]
    pname2 = project_dup_names[1]
    first_list = None
    # The , has lower priority than the |, and equal to multiple --filter commands
    for cmd in (f'project list --filter "name={pname}|name={pname2},owner={uname}"',
                f'project list --filter "owner={uname},name={pname}|name={pname2}"',
                f'project list --filter "name={pname}|name={pname2}" --filter owner={uname}',
                f'project list --filter owner={uname} --filter "name={pname}|name={pname2}"'):
        plist = _cmd(cmd)
        if first_list is None:
            assert(p['name'] in (pname, pname2) and p['owner'] == uname for p in plist)
            first_list = plist
        else:
            assert plist == first_list


def test_boolean_ampersand(user_session, project_dup_names):
    uname = user_session.username
    pname = project_dup_names[0]
    pname2 = project_dup_names[1]
    first_list = None
    # The & has higher priority than the |
    for cmd in (f'project list --filter "name={pname}|name={pname2}&owner={uname}"',
                f'project list --filter "name={pname}|owner={uname}&name={pname2}"',
                f'project list --filter "owner={uname}&name={pname2}|name={pname}"',
                f'project list --filter "name={pname2}&owner={uname}|name={pname}"'):
        plist = _cmd(cmd)
        if first_list is None:
            assert(p['name'] == pname or (p['name'] == pname2 and p['owner'] == uname) for p in plist)
            first_list = plist
        else:
            assert plist == first_list


def test_columns(user_session):
    uname = user_session.username
    for cmd in (f'project list --columns name,editor,id --filter owner={uname}',
                f'project list --columns name,editor,id'):
        plist = _cmd(cmd)
        assert(list(p) == ['name', 'editor', 'id'] for p in plist)


def test_sort(user_session, project_dup_names):
    name_filter = '|'.join(f'name={n}' for n in project_dup_names)
    plist1 = _cmd(f'project list --filter "{name_filter}" --sort name,owner')
    plist2 = _cmd(f'project list --filter "{name_filter}" --sort name,-owner')
    plist3 = _cmd(f'project list --filter "{name_filter}" --sort -name,owner')
    plist4 = _cmd(f'project list --filter "{name_filter}" --sort -name,-owner')
    assert plist1 == plist4[::-1]
    assert plist2 == plist3[::-1]
    assert [p['name'] for p in plist1] == [p['name'] for p in plist2]
    slist1 = [(p['name'], p['owner']) for p in plist1]
    assert slist1 == sorted(slist1)
    slist2 = [(p['name'], p['owner']) for p in plist2]
    assert slist2 != sorted(slist2)


def test_filter_comparison(project_list):
    owners = sorted(set(p['owner'] for p in project_list))
    plist1 = _cmd(f'project list --sort owner,name --filter "owner<{owners[1]}"')
    plist2 = _cmd(f'project list --sort owner,name --filter "owner<={owners[0]}"')
    assert plist1 == plist2
    plist3 = _cmd(f'project list --sort owner,name --filter "owner>={owners[1]}"')
    plist4 = _cmd(f'project list --sort owner,name --filter "owner>{owners[0]}"')
    plist5 = _cmd(f'project list --sort owner,name --filter "owner!={owners[0]}"')
    assert plist3 == plist4
    assert plist3 == plist5
    plist6 = _cmd(f'project list --sort owner,name')
    assert plist1 + plist3 == plist6
