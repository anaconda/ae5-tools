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
