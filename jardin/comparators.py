__all__ = (
    'is_not_null', 'greater_than', 'gt', 'greater_or_equal', 'get',
    'less_than', 'lt', 'less_or_equal', 'let'
    )

def is_not_null():
    return 'IS NOT NULL'

def operator(op, *args, **kwargs):
    def func():
        if len(args) > 0:
            return op, args[0]
        if 'column' in kwargs:
            return '%s %s' % (op, kwargs['field']), None
    return func

def greater_than(*args, **kwargs):
    return operator('>', *args, **kwargs)
gt = greater_than

def greater_or_equal(field):
    return operator('>=', field)
get = greater_or_equal

def less_than(field):
    return operator('<', field)
lt = less_than

def less_or_equal(field):
    return operator('<=', field)
let = less_or_equal