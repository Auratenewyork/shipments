from jinja2.filters import environmentfilter


@environmentfilter
def dateformat(env, obj):
    return obj.strftime("%b %d, %Y")
