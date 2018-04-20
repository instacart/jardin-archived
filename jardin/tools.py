import itertools
from operator import is_not
from functools import partial, wraps
import time


def stack_marker(stack, db_conn=None):
    filename, line_number, function_name = stack[1][1:4]
    stack = [db_conn.name] if db_conn else []
    stack += [filename, function_name, str(line_number)]
    return ':'.join(stack)

class classorinstancemethod(object):
    def __init__(self, method):
        self.method = method
    def __get__(self, instance, cls):
        return lambda *args, **kw: self.method(instance or cls, *args, **kw)

def add_to_where(where, item):
    if isinstance(where, dict):
        if isinstance(item, dict):
            where.update(**item)
        elif isinstance(item, list):
            where = [where] + item
        elif isinstance(item, str):
            where = [where, item]
    elif isinstance(where, list):
        if isinstance(item, list):
            where += item
        else:
            where += [item]
    elif isinstance(where, str):
        if isinstance(item, list):
            where = [where] + item
        else:
            where = [where, item]
    return where

def is_in_where(where, field):
    if isinstance(where, dict) or isinstance(where, str):
        return field in where
    elif isinstance(where, list):
        for w in where:
            if is_in_where(w, field):
                return True
    return False

def soft_del(func):
    def wrapper(self, **kwargs):
        if self.soft_delete and not kwargs.get('skip_soft_delete', False):
            add_soft_delete(kwargs, self.deleted_at_column())
        return func(self, **kwargs)
    return wrapper

def add_soft_delete(kwargs, deleted_at_column):
    if not is_in_where(kwargs.get('where', {}), deleted_at_column):
        kwargs['where'] = kwargs.get('where', {})
        kwargs['where'] = add_to_where(
            kwargs['where'],
            {deleted_at_column: None}
            )

def grouper(iterable, n, fillvalue=None):
  "Collect data into fixed-length chunks or blocks"
  # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
  args = [iter(iterable)] * n
  return itertools.izip_longest(fillvalue=fillvalue, *args)

def remove_none(res):
  return filter(partial(is_not, None), res)

def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry
