from flask import session, abort
from Controller.DatasetManager import DatasetManager
from functools import wraps

# SOURCE: https://docs.python.org/2/library/functools.html
def require_login(func):
    """Wrapper that makes functions only accessible if
    the user is loggedn in."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'loggedin' in session and session['loggedin']:
            return func(*args, **kwargs)
        else:
            abort(403)
    return wrapper
# ENDFUNCTION

def require_admin(func):
    """Wrapper that makes functions only accessible if 
    the user is logged in and if the user is a site-admin."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'userdata' in session and session['userdata'] is not None and session['userdata']['admin']:
            return func(*args, **kwargs)
        else:
            abort(403)

    return wrapper
# ENDFUNCTION

def require_perm(func, perm):
    """Wrapper to assert that a user has at least the specified permissions to the dataset."""

    @wraps(func)
    def wrapper(*args, **kwargs):

        # check that dataset_id is in kwargs
        if not 'dataset_id' in kwargs:
            raise RuntimeError("'dataset_id' not specified.")

        if not session['loggedin']:
            raise RuntimeError("Not logged in.")

        dataset_id = kwargs['dataset_id']
        userid = session['userdata']['userid']


        # only acces if user has correct permission or is site admin
        if DatasetManager.userHasAccessTo(dataset_id, userid, perm) or session['userdata']['admin']:
            return func(*args, **kwargs)
        else:
            abort(403)

    return wrapper
# ENDFUNCTION

def require_readperm(func):
    """Wrapper to assert that a user has at least read permissions to the dataset."""
    return require_perm(func, 'read')

def require_writeperm(func):
    """Wrapper to assert that a user has at least write permissions to the dataset."""
    return require_perm(func, 'write')

def require_adminperm(func):
    """Wrapper to assert that a user has at least admin permissions to the dataset."""
    return require_perm(func, 'admin')
