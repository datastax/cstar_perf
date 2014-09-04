from flask import ( Flask, render_template, request, redirect, abort,
                    jsonify, make_response, session)
from app import app, db
from model import UnknownUserError, UnknownTestError

################################################################################
#### Template functions:
################################################################################
def get_user_id():
    return session.get('user_id', None)
app.jinja_env.globals['get_user_id'] = get_user_id

def user_is_authenticated():
    return session.get('logged_in',False)
app.jinja_env.globals['user_is_authenticated'] = user_is_authenticated


################################################################################
#### Helpers
################################################################################
def user_in_role(role, user=None):
    """Find if a user is in the given role"""
    if user is None:
        user = get_user_id()
    try:
        user_roles = db.get_user_roles(get_user_id())
        if role in user_roles:
            return True
    except UnknownUserError:
        pass
    return False

def requires_auth(role):
    """Ensures the current user has the appropriate authorization before
    running the wrapped function"""
    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kw):
            # Do the check:
            if user_is_authenticated():
                if user_in_role(role):
                    return function(*args, **kw)
            return make_response(render_template('access_denied.jinja2.html'), 401)
        return wrapper
    return decorator
