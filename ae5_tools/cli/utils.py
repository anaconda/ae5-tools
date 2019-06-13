import click


def add_param(param, value):
    if value is None:
        return
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    if param == 'filter':
        ovalue = obj.get('filter') or ''
        value = f'{ovalue},{value}' if ovalue and value else (value or ovalue)
    obj[param] = value


def param_callback(ctx, param, value):
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
        if not line:
            if paragraph:
                _emit(paragraph)
                paragraph = ''
            click.echo(line)
        else:
            paragraph += line
    if paragraph:
        _emit(paragraph)
