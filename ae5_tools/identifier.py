import re
from collections import namedtuple


class Identifier(namedtuple('Identifier', ['name', 'owner', 'id', 'revision'])):
    @classmethod
    def from_string(self, idstr):
        rev_parts = idstr.rsplit(':', 1)
        revision = rev_parts[1] if len(rev_parts) == 2 else ''
        id_parts = rev_parts[0].split('/')
        id, owner, name = '', '', ''
        if re.match(r'[a-f0-9]{2}-[a-f0-9]{32}', id_parts[-1]):
            id = id_parts.pop()
        if id_parts:
            name = id_parts.pop()
        if id_parts:
            owner = id_parts.pop()
        if id_parts:
            raise ValueError(f'Invalid identifier: {idstr}')
        return Identifier(name, owner, id, revision)

    def project_filter(self):
        parts = []
        if self.name and self.name != '*':
            parts.append(f'name={self.name}')
        if self.owner and self.owner != '*':
            parts.append(f'owner={self.owner}')
        if self.id and self.id != '*':
            parts.append(f'id={self.id}')
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
