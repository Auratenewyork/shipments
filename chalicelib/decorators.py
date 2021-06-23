from functools import wraps

from sentry_sdk import capture_exception, configure_scope


def try_except(transaction='try_except', **tag):
    def decorated_function(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if transaction or tag:
                    with configure_scope() as scope:
                        scope.transaction = transaction
                        if tag:
                            scope.set_tag(*list(tag.items())[0])
                        capture_exception(e, scope=scope)
                else:
                    capture_exception(e)
        return wrapper
    return decorated_function
