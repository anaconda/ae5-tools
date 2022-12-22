import re
from collections import namedtuple


class Identifier(namedtuple("Identifier", ["owner", "name", "id", "pid", "revision"])):
    # from anaconda_platform/ui/base.py
    RE_ID = r"[a-f0-9]{2}-[a-f0-9]{32}"
    SLUG_MAP = {
        "a0": "projects",
        "a1": "sessions",
        "a2": "deployments",
        "a3": "channels",
    }
    REVERSE_SLUG_MAP = {v: k for k, v in SLUG_MAP.items()}
    # pods can be a1- or a2- so we have to handle them specially in the code below
    REVERSE_SLUG_MAP.update({"jobs": "a2", "runs": "a2", "pods": "a2"})

    @classmethod
    def id_type(cls, idstr, quiet=False):
        if idstr in cls.SLUG_MAP:
            return cls.SLUG_MAP[idstr]
        elif re.match(cls.RE_ID, idstr) and idstr[:2] in cls.SLUG_MAP:
            return cls.SLUG_MAP[idstr[:2]]
        elif not quiet:
            raise ValueError(f"Invalid identifier: {idstr}")

    @classmethod
    def id_prefix(cls, type, quiet=False):
        try:
            return cls.REVERSE_SLUG_MAP[type]
        except KeyError:
            if not quiet:
                return ValueError(f"Invalid identifier type: {type}")

    @classmethod
    def has_prefix(cls, type):
        return bool(cls.id_prefix(type, quiet=True))

    @classmethod
    def from_string(cls, idstr, no_revision=False, quiet=False):
        try:
            if no_revision:
                rev_parts = (idstr,)
            else:
                rev_parts = idstr.rsplit(":", 1)
            if len(rev_parts) == 1 or rev_parts[1] == "*":
                revision = ""
            else:
                revision = rev_parts[1]
            id_parts = rev_parts[0].split("/")
            name, owner, id, pid = "", "", "", ""
            if id_parts and (len(id_parts) == 4 or re.match(cls.RE_ID, id_parts[-1])):
                pid = id_parts.pop()
                if pid == "*":
                    pid = ""
                elif pid:
                    cls.id_type(pid)
            if id_parts and (len(id_parts) == 3 or re.match(cls.RE_ID, id_parts[-1])):
                id = id_parts.pop()
                if id == "*":
                    id = ""
                elif id:
                    cls.id_type(id)
            if id and pid:
                if cls.id_type(id) == "projects" and cls.id_type(pid) != "projects":
                    id, pid = pid, id
                if cls.id_type(pid) != "projects" or cls.id_type(id) == "projects" and id != pid:
                    raise ValueError(f"Invalid identifier: {idstr}")
            elif pid and cls.id_type(pid) == "projects":
                id = pid
            else:
                id, pid = pid, ""
            if id_parts:
                name = id_parts.pop()
                if name == "*":
                    name = ""
            if id_parts:
                owner = id_parts.pop()
                if owner == "*":
                    owner = ""
            if id_parts:
                raise ValueError(f"Invalid identifier: {idstr}")
        except ValueError:
            if quiet:
                return None
            raise
        return Identifier(owner, name, id, pid, revision)

    @classmethod
    def from_record(cls, record, ignore_revision=False):
        rev = "" if ignore_revision else (record.get("revision") or "")
        id = record["id"]
        pid = id if cls.id_type(id) == "projects" else record.get("project_id", "")
        return Identifier(record["owner"], record["name"], id, pid, rev)

    def project_filter(self, itype=None, include_wildcards=False, ignore_revision=False):
        parts = []
        if include_wildcards or self.name and self.name != "*":
            parts.append(f'name={self.name or "*"}')
        if include_wildcards or self.owner and self.owner != "*":
            parts.append(f'owner={self.owner or "*"}')
        if self.pid and self.pid != "*":
            dual_id = self.id and self.id != "*" and self.id != self.pid
            if dual_id or itype not in (None, "projects"):
                parts.append(f"project_id={self.pid}")
        elif include_wildcards and itype not in (None, "projects"):
            parts.append(f"project_id=*")
        if self.id and self.id != "*" and (self.id != self.pid or itype in (None, "projects")):
            if itype is not None:
                ival = Identifier.id_type(self.id)
                tval = "deployments" if itype in ("jobs", "runs") else itype
                if itype != "pods" and ival != tval:
                    raise ValueError(f"Expected a {itype[:-1]} ID, not a {ival[:-1]} ID: {self.id}")
            parts.append(f"id={self.id}")
        elif include_wildcards:
            parts.append(f"id=*")
        if not ignore_revision:
            if include_wildcards or self.revision and self.revision not in ("*", "latest"):
                parts.append(f'revision={self.revision or "*"}')
        return ",".join(parts) or "*"

    def revision_filter(self):
        if self.revision and self.revision != "*":
            return f"name={self.revision}"

    def to_dict(self, drop_revision=False):
        return {k: v for k, v in zip(self._fields, self) if (k != "revision" or not drop_revision) and v and v != "*"}

    def to_string(self, drop_pid=False, drop_revision=False):
        parts = []
        if self.id:
            if drop_pid or not self.pid or self.id == self.pid:
                parts.append(self.id)
            else:
                parts.append(f"{self.pid}/{self.id}")
        if self.owner or self.name or not self.id:
            parts.append(self.name or "*")
        if self.owner:
            parts.append(self.owner)
        result = "/".join(parts[::-1])
        if self.revision and not drop_revision:
            result = f"{result}:{self.revision}"
        return result

    def __str__(self):
        return self.to_string()

    def __bool__(self):
        return bool(self.to_dict())
