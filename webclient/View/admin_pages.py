from flask import Blueprint, render_template
from utils import require_admin
from utils import require_login
from UserManager import UserManager
from admin_forms import DeleteUserForm

admin_pages = Blueprint('admin_pages', __name__)

@admin_pages.route('/admin/manage_users/')
@require_login
@require_admin
def manage_users():
    """Returns a page with which an admin can remove and manage users."""

    ## retrieve list of all users
    user_list = UserManager.getAllUsers()

    print(len(user_list))

    destroy_user_forms = []

    for user in user_list:
        form = DeleteUserForm()

        form.fillForm(user)

        destroy_user_forms.append(form)
    # ENDFOR

    return render_template('manage_users.html', destroy_user_forms = destroy_user_forms)
# ENDFUNCTION

@admin_pages.route('/admin/delete_user', methods=['POST'])
@require_login
@require_admin
def delete_user():
    """Callback for removing a user by an admin."""
    return "TODO"

@admin_pages.route('/admin/manage_datasets/')
@require_login
@require_admin
def manage_datasets():
    """Returns a page with which an admin can remove and manage datasets."""
    return render_template('manage_datasets.html', dataset_list = [])
