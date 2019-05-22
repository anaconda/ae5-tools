import re
from collections import namedtuple

# from anaconda_platform/ui/base.py
RE_ID = r'a[0-3]-[a-f0-9]{32}'
SLUG_MAP = {'a0': 'projects', 'a1': 'sessions', 'a2': 'deployments', 'a3': 'channels'}
REVERSE_SLUG_MAP = {v: k for k, v in SLUG_MAP.items()}


class Identifier(namedtuple('Identifier', ['owner', 'name', 'id', 'pid', 'revision'])):
    @classmethod
    def id_type(cls, idstr):
        if re.match(RE_ID, idstr):
            return SLUG_MAP[idstr.split('-', 1)[0]]
        else:
            raise ValueError(f'Invalid identifier: {idstr}')

    @classmethod
    def id_prefix(cls, type):
        try:
            return REVERSE_SLUG_MAP[type]
        except KeyError:
            return ValueError(f'Invalid identifier type: {type}')

    @classmethod
    def from_string(self, idstr):
        rev_parts = idstr.rsplit(':', 1)
        if len(rev_parts) == 1 or rev_parts[1] == '*':
            revision = ''
        else:
            revision = rev_parts[1]
        id_parts = rev_parts[0].split('/')
        name, owner, id, pid = '', '', '', ''
        if id_parts and re.match(RE_ID, id_parts[-1]):
            pid = id_parts.pop()
        if id_parts and re.match(RE_ID, id_parts[-1]):
            id = id_parts.pop()
        if id and pid:
            if self.id_type(id) == 'projects' and self.id_type(pid) != 'projects':
                id, pid = pid, id
            if self.id_type(pid) != 'projects' or self.id_type(id) == 'projects' and id != pid:
                raise ValueError(f'Invalid identifier: {idstr}')
        elif pid and self.id_type(pid) == 'projects':
            id = pid
        else:
            id, pid = pid, ''
        if id_parts:
            name = id_parts.pop()
            if name == '*':
                name = ''
        if id_parts:
            owner = id_parts.pop()
            if owner == '*':
                owner = ''
        if id_parts:
            raise ValueError(f'Invalid identifier: {idstr}')
        return Identifier(owner, name, id, pid, revision)

    @classmethod
    def from_record(self, record, ignore_revision=False):
        rev = '' if ignore_revision else (record.get('revision') or '')
        id = record['id']
        pid = id if self.id_type(id) == 'projects' else record.get('project_id', '')
        return Identifier(record['owner'], record['name'], id, pid, rev)

    def project_filter(self, session=False):
        parts = []
        if self.name and self.name != '*':
            parts.append(f'name={self.name}')
        if self.owner and self.owner != '*':
            parts.append(f'owner={self.owner}')
        if self.pid and self.pid != self.id:
            parts.append(f'project_id={self.pid}')
        if self.id:
            parts.append(f'id={self.id}')
        if parts:
            return ','.join(parts)

    def revision_filter(self):
        if self.revision and self.revision != '*':
            return f'name={self.revision}'

    def to_string(self, drop_pid=False, drop_revision=False):
        parts = []
        if self.id:
            if drop_pid or not self.pid or self.id == self.pid:
                parts.append(self.id)
            else:
                parts.append(f'{self.pid}/{self.id}')
        if self.owner or self.name:
            parts.append(self.name or "*")
        if self.owner:
            parts.append(self.owner)
        result = '/'.join(parts[::-1])
        if self.revision and not drop_revision:
            result = f'{result}:{self.revision}'
        return result

    def __str__(self):
        return self.to_string()
