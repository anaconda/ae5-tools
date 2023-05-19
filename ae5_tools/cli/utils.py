import click

from ..api import IDENT_FILTERS
from ..identifier import Identifier


def param_callback(ctx, param, value):
    if value in (None, ()):
        return
    add_param(param.name.lower().replace("-", "_"), value)


GLOBAL_OPTIONS = [click.option("--yes", is_flag=True, expose_value=False, callback=param_callback, hidden=True)]


def global_options(func):
    for option in reversed(GLOBAL_OPTIONS):
        func = option(func)
    return func


def yes_option(func):
    return click.option(
        "--yes",
        is_flag=True,
        expose_value=False,
        callback=param_callback,
        hidden=False,
        help="Do not ask for confirmation.",
    )(func)


def add_param(param, value):
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    options = obj.setdefault("options", {})
    if param == "filter" and not isinstance(value, tuple):
        value = tuple(value.split(","))
    if param in options:
        ovalue = options[param]
        if param == "filter":
            value = ovalue + value
        elif not isinstance(ovalue, bool) and ovalue != value:
            param = param.replace("_", "-")
            raise click.UsageError(f"Conflicting values for --{param}: {ovalue}, {value}")
    options[param] = value


def stash_defaults():
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    obj["defaults"] = obj.get("options", {})
    obj["options"] = {}


def get_options():
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    options = obj.get("defaults", {})
    options.update(obj.get("options", {}))
    return options


def persist_option(param, value):
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    obj.setdefault("options", {})[param] = value
    if "defaults" in obj:
        obj["defaults"][param] = value


def ident_callback(handle_revision, required):
    def _callback(ctx, param, value):
        if value in (None, "", ()):
            return
        revision = None
        record_type = param.name.lower().replace("-", "_")
        ident_type = record_type.rstrip("s") + "s"
        if Identifier.has_prefix(ident_type):
            ident = Identifier.from_string(value)
            if ident.revision:
                if not handle_revision:
                    raise click.ClickException(f"Revision tag not expected here: {value}")
                revision = ident.revision
            # Just to verify that it the identifier is compatible
            # with this category of search (e.g., deployment, project)
            try:
                value = Identifier.project_filter(ident, ident_type, ignore_revision=True)
            except ValueError as exc:
                raise click.ClickException(str(exc))
        elif record_type in IDENT_FILTERS:
            value = IDENT_FILTERS[record_type].format(value=value)
        value = (record_type, tuple(value.split(",")), required, revision)
        add_param("ident_filter", value)

    return _callback


def ident_filter(name, handle_revision=False, required=False):
    callback = ident_callback(handle_revision, required)
    return click.argument(name, expose_value=False, callback=callback, required=required)


def click_text(text):
    def _emit(text):
        if text[0] == "@":
            text = text[1:]
            initial_indent = ""
        else:
            initial_indent = "  "
        click.echo(click.wrap_text(text, initial_indent=initial_indent, subsequent_indent="  "))

    paragraph = ""
    for line in text.splitlines():
        if not line or line.lstrip().startswith("-"):
            if paragraph:
                _emit(paragraph)
                paragraph = ""
            click.echo(line)
        elif paragraph:
            paragraph += " " + line
        else:
            paragraph = line
    if paragraph:
        _emit(paragraph)
