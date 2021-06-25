from functools import wraps

from sentry_sdk import capture_exception, configure_scope


def try_except(*tag_list, **tags):
    def decorated_function(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                with configure_scope() as scope:
                    scope.transaction = func.__name__
                    for key, value in tags.items():
                        scope.set_tag(key, value)
                    capture_exception(e, scope=scope)
        return wrapper
    return decorated_function
