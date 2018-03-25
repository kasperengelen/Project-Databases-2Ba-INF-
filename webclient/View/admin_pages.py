from flask import Blueprint, render_template, redirect, url_for
from utils import require_admin
from utils import require_login
from UserManager import UserManager
from DatasetManager import DatasetManager
from admin_forms import DeleteUserForm, DeleteDatasetForm

admin_pages = Blueprint('admin_pages', __name__)

@admin_pages.route('/admin/manage_users/')
@require_login
@require_admin
def manage_users():
    """Returns a page with which an admin can remove and manage users."""

    ## retrieve list of all users
    user_list = UserManager.getAllUsers()

    destroy_user_forms = []

    for user in user_list:
        form = DeleteUserForm()

        form.fillForm(user)

        destroy_user_forms.append(form)
    # ENDFOR

    return render_template('manage_users.html', destroy_user_forms = destroy_user_forms)
# ENDFUNCTION

@admin_pages.route('/admin/delete_user/', methods=['POST'])
@require_login
@require_admin
def delete_user():
    """Callback for admins to delete a user."""

    if not form.validate():
        return redirect(url_for('admin_pages.manage_users'))

    userid = int(form.userid.data)

    if not UserManager.existsID(userid):
        flash(message="User does not exist.", category="error")
        return redirect(url_for('admin_pages.manage_users'))

    UserManager.destoyUser(userid)

    flash(message="User deleted.", category="error")

    return redirect(url_for('admin_pages.manage_users'))
# ENDFUNCTION

@admin_pages.route('/admin/manage_datasets/')
@require_login
@require_admin
def manage_datasets():
    """Returns a page with which an admin can remove and manage datasets."""

    dataset_list = DatasetManager.getAllDatasets()

    destroy_dataset_forms = []

    for dataset in dataset_list:
        form = DeleteDatasetForm()

        form.fillForm(dataset)

        destroy_dataset_forms.append(form)
    # ENDFOR

    return render_template('manage_datasets.html', destroy_dataset_forms = destroy_dataset_forms)
# ENDFUNCTION

@admin_pages.route('/admin/delete_dataset/', methods=['POST'])
@require_login
@require_admin
def delete_dataset():
    """Callback for admins to delete a dataset."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    DatasetManager.destroyDataset(dataset_id)

    return redirect(url_for('admin_pages.manage_datasets'))
# ENDFUNCTION

