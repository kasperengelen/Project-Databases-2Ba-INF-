from flask import Blueprint, render_template, redirect, url_for, request, flash
from utils import require_admin
from utils import require_login
from UserManager import UserManager
from DatasetManager import DatasetManager
from admin_forms import DeleteUserForm, DeleteDatasetForm, AdminUserEditForm, ActivateDecactivateUser

admin_pages = Blueprint('admin_pages', __name__)

@admin_pages.route('/admin/manage_users/')
@require_login
@require_admin
def manage_users():
    """Returns a page with which an admin can remove and manage users."""

    ## retrieve list of all users
    user_list = UserManager.getAllUsers()

    user_forms = []

    for user in user_list:
        deleteform = DeleteUserForm()
        activationform = ActivateDecactivateUser()

        deleteform.fillForm(user)
        activationform.fillForm(user)

        user_forms.append({
            'deleteform': deleteform,
            'activationform': activationform
        })
    # ENDFOR

    return render_template('admin_pages.manage_users.html', user_forms = user_forms)
# ENDFUNCTION

@admin_pages.route('/admin/edit_user/<int:userid>/', methods=['POST', 'GET'])
@require_login
@require_admin
def edit_user(userid):
    """Returns a page that provides a way to for an admin edit user information."""

    current_user = UserManager.getUserFromID(userid)

    # CHECK IF USER EXISTS
    if not UserManager.existsID(userid):
        abort(404)

    form = AdminUserEditForm(request.form)

    if request.method == 'POST' and form.validate():

        # check if new email is in use.
        if form.email.data != current_user.email and UserManager.existsEmail(form.email.data):
            flash(message="Specified e-mail address already in use.", category="error")
            return render_template('admin_pages.edit_user.html', form=form)

        # update information
        new_email = form.email.data
        new_fname = form.firstname.data
        new_lname = form.lastname.data

        current_user.editInfoNoPass(new_email, new_fname, new_lname)
        flash(message="Information updated.", category="success")
    else:
        form.fillFields(current_user)

    return render_template('admin_pages.edit_user.html', form=form)
# ENDFUNCTION

@admin_pages.route('/admin/delete_user/', methods=['POST'])
@require_login
@require_admin
def delete_user():
    """Callback for admins to delete a user."""

    form = DeleteUserForm(request.form)

    if not form.validate():
        return redirect(url_for('admin_pages.manage_users'))

    userid = int(form.userid.data)

    if not UserManager.existsID(userid):
        flash(message="User does not exist.", category="error")
        return redirect(url_for('admin_pages.manage_users'))

    UserManager.destroyUser(userid)

    flash(message="User deleted.", category="success")

    return redirect(url_for('admin_pages.manage_users'))
# ENDFUNCTION

@admin_pages.route('/admin/set_user_activation/', methods=['POST'])
@require_login
@require_admin
def set_user_activation():
    """Callback for admins to activate a user."""

    form = ActivateDecactivateUser(request.form)

    if not form.validate():
        return redirect(url_for('admin_pages.manage_users'))

    userid = int(form.userid.data)

    if not UserManager.existsID(userid):
        flash(message="User does not exist.", category="error")
        return redirect(url_for('admin_pages.manage_users'))

    UserManager.updateUserActivity(userid, bool(form.new_activation_status))

    if(bool(form.new_activation_status)):
        flash(message="User activated.", category="success")
    else:
        flash(message="User deactivated.", category="success")

    return redirect(url_for('admin_pages.manage_users'))

def deactivate_user():
    """Callback for admins to deactivate a user."""

    return redirect(url_for('admin_pages.manage_users'))


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

    return render_template('admin_pages.manage_datasets.html', destroy_dataset_forms = destroy_dataset_forms)
# ENDFUNCTION

@admin_pages.route('/admin/delete_dataset/', methods=['POST'])
@require_login
@require_admin
def delete_dataset():
    """Callback for admins to delete a dataset."""

    form = DeleteDatasetForm(request.form)

    dataset_id = int(form.setid.data)

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    DatasetManager.destroyDataset(dataset_id)

    return redirect(url_for('admin_pages.manage_datasets'))
# ENDFUNCTION

