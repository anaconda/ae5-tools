import re
from collections import namedtuple

# from anaconda_platform/ui/base.py
RE_ID = r'a[0-3]-[a-f0-9]{32}'
SLUG_MAP = {'a0': 'projects', 'a1': 'sessions', 'a2': 'deployments', 'a3': 'channels'}
REVERSE_SLUG_MAP = {v: k for k, v in SLUG_MAP.items()}


class Identifier(namedtuple('Identifier', ['name', 'owner', 'id', 'revision'])):
    @classmethod
    def id_type(cls, idstr):
        if re.match(RE_ID, idstr):
            return SLUG_MAP[idstr.split('-', 1)[0]]
        else:
            raise ValueError(f'Invalid ID: {idstr}')

    @classmethod
    def id_prefix(cls, type):
        try:
            return REVERSE_SLUG_MAP[type]
        except KeyError:
            return ValueError(f'Invalid identifier type: {type}')

    @classmethod
    def from_string(self, idstr):
        rev_parts = idstr.rsplit(':', 1)
        revision = rev_parts[1] if len(rev_parts) == 2 else ''
        id_parts = rev_parts[0].split('/')
        name, owner, id = '', '', ''
        if len(id_parts) == 3 or re.match(RE_ID, id_parts[-1]):
            id = id_parts.pop()
        if id_parts:
            name = id_parts.pop()
        if id_parts:
            owner = id_parts.pop()
        if id_parts:
            raise ValueError(f'Invalid identifier: {idstr}')
        return Identifier(name, owner, id, revision)

    @classmethod
    def from_record(self, record, ignore_revision=False):
        rev = '' if ignore_revision else (record.get('revision') or '')
        return Identifier(record['name'], record['owner'], record['id'], rev)

    def project_filter(self, session=False):
        parts = []
        if self.name and self.name != '*':
            parts.append(f'name={self.name}')
        if self.owner and self.owner != '*':
            parts.append(f'owner={self.owner}')
        if self.id and self.id != '*':
            key = 'project_id' if session and self.id.startswith('a0-') else 'id'
            parts.append(f'{key}={self.id}')
        if parts:
            return ','.join(parts)

    def revision_filter(self):
        if self.revision and self.revision != '*':
            return f'name={self.revision}'

    def to_string(self, drop_revision=False):
        if self.id:
            if self.owner or self.name:
                result = f'{self.owner or "*"}/{self.name or "*"}/{self.id}'
            else:
                result = self.id
        elif self.owner:
            result = f'{self.owner}/{self.name or "*"}'
        else:
            result = self.name
        if self.revision and not drop_revision:
            result = f'{result}:{self.revision}'
        return result

    def __str__(self):
        return self.to_string()
