import click


def add_param(param, value):
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    if param == 'filter' and not isinstance(value, tuple):
        value = tuple(value.split(','))
    options = obj.setdefault('options', {})
    if param in obj:
        ovalue = options[param]
        if param == 'filter':
            value = ovalue + value
        elif not isinstance(ovalue, bool) and ovalue != value:
            param = param.replace('_', '-')
            raise click.UsageError(f'Conflicting values for --{param}: {ovalue}, {value}')
    options[param] = value


def stash_defaults():
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    obj['defaults'] = obj.get('options', {})
    obj['options'] = {}


def get_options():
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    options = obj.get('defaults', {})
    options.update(obj.get('options', {}))
    return options


def persist_option(param, value):
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    obj['defaults'][param] = obj['options'][param] = value


def param_callback(ctx, param, value):
    if value in (None, ()):
        return
    add_param(param.name.lower().replace('-', '_'), value)


def click_text(text):
    def _emit(text):
        if text[0] == '@':
            text = text[1:]
            initial_indent = ''
        else:
            initial_indent = '  '
        click.echo(click.wrap_text(text, initial_indent=initial_indent, subsequent_indent='  '))
    paragraph = ''
    for line in text.splitlines():
        if not line or line.lstrip().startswith('-'):
            if paragraph:
                _emit(paragraph)
                paragraph = ''
            click.echo(line)
        elif paragraph:
            paragraph += ' ' + line
        else:
            paragraph = line
    if paragraph:
        _emit(paragraph)
