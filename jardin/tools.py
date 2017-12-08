from __future__ import print_function

import itertools
import time
import warnings
from functools import partial, wraps
from operator import is_not


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
    def _retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            num_of_tries, num_of_delays = tries, delay
            while num_of_tries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = "%s, Retrying in %d seconds..." % (
                        str(e), num_of_delays)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(num_of_delays)
                    num_of_tries -= 1
                    num_of_delays *= backoff
            return f(*args, **kwargs)

        return f_retry

    return _retry
