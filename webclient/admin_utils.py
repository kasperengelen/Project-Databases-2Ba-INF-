
from flask import Blueprint, url_for, session, redirect, abort, render_template
from functools import wraps

import user_utils

def require_admin(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'user_data' in session and session['user_data'] is not None and session['user_data']['admin']:
            return func(*args, **kwargs)
        else:
            abort(403)
            return redirect(url_for('index'))

    return wrapper
# ENDFUNCTION

########################################### CALLBACKS ###########################################

admin_pages = Blueprint('admin_pages', __name__)

@admin_pages.route('/admin/manage_users/')
@user_utils.require_login
@require_admin
def manage_users():
    """Returns a page that is only accessible to admins and displays
    a list of users with the option to remove each user."""

    # retrieve user list

    # create forms



    return render_template('manage_users.html', user_list = [])
# ENDFUNCTION

@admin_pages.route('/admin/delete_user')
def delete_user():
    pass

@admin_pages.route('/admin/manage_datasets/')
@user_utils.require_login
@require_admin
def manage_datasets():
    return render_template('manage_datasets.html', dataset_list = [])
# ENDFUNCTION

