from functools import wraps
import logging
import traceback


logger = logging.getLogger(__name__)


def try_except(func):
    @wraps(func)
    def decorated_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            # add sentry logging
            traceback.print_exc()
    return decorated_function
