__all__ = (
    'not_null', 'greater_than', 'gt', 'greater_or_equal', 'geq',
    'less_than', 'lt', 'less_or_equal', 'leq', 'not_equal', 'neq',
    'not_in'
    )

def not_null():
    def func():
        return 'IS NOT NULL', None
    return func

def operator(op, *args, **kwargs):
    def func():
        if len(args) > 0:
            return op, args[0]
        if 'col' in kwargs:
            return '%s %s' % (op, kwargs['col']), None
    return func

def greater_than(*args, **kwargs):
    return operator('>', *args, **kwargs)
gt = greater_than

def greater_or_equal(*args, **kwargs):
    return operator('>=', *args, **kwargs)
geq = greater_or_equal

def less_than(*args, **kwargs):
    return operator('<', *args, **kwargs)
lt = less_than

def less_or_equal(*args, **kwargs):
    return operator('<=', *args, **kwargs)
leq = less_or_equal

def not_equal(*args, **kwargs):
    return operator('!=', *args, **kwargs)
neq = not_equal

def not_in(*args, **kwargs):
    return operator('NOT IN', *args, **kwargs)