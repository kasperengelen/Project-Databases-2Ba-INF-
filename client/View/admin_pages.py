from flask import Blueprint, render_template, redirect, url_for, request, flash
from flask import current_app as app

from Controller.AccessController import require_admin
from Controller.AccessController import require_login
from Controller.UserManager import UserManager
from Controller.DatasetManager import DatasetManager
from View.admin_forms import DeleteUserForm, DeleteDatasetForm, AdminUserEditForm, ActivateDeactivateUser, DatasetEditForm, AlterAdminForm
from View.form_utils import flash_errors

admin_pages = Blueprint('admin_pages', __name__)

@admin_pages.route('/admin/manage_users/')
@require_login
@require_admin
def manage_users():
    """Returns a page with which an admin can remove and manage users."""

    users = []

    for user in UserManager.getAllUsers():

        # create forms
        deleteform = DeleteUserForm()
        activationform = ActivateDeactivateUser()
        editform = AdminUserEditForm()

        # fill forms
        deleteform.fillForm(user)
        activationform.fillForm(user)
        editform.fillForm(user)

        users.append({
            'userinfo': user,
            'editform': editform,
            'deleteform': deleteform,
            'activationform': activationform
        })
    # ENDFOR

    return render_template('admin_pages.manage_users.html', users = users)
# ENDFUNCTION

@admin_pages.route('/admin/edit_user/<int:userid>/', methods=['POST'])
@require_login
@require_admin
def edit_user(userid):
    """Callback for admin to edit user information."""

    # CHECK IF USER EXISTS
    if not UserManager.existsID(userid):
        return redirect(url_for('admin_pages.manage_users'))

    form = AdminUserEditForm(request.form)

    if not form.validate():
        flash_errors(form)
    else:
        current_user = UserManager.getUserFromID(userid)
        # check if new email is in use.
        if form.email.data != current_user.email and UserManager.existsEmail(form.email.data):
            flash(message="Specified e-mail address already in use.", category="error")
            return render_template('admin_pages.edit_user.html', form=form)

        # update information
        new_email = form.email.data
        new_fname = form.firstname.data
        new_lname = form.lastname.data

        UserManager.editUserInfo(userid, new_fname, new_lname, new_email)

        flash(message="Information updated.", category="success")

    return redirect(url_for('admin_pages.manage_users'))
# ENDFUNCTION

@admin_pages.route('/admin/delete_user/', methods=['POST'])
@require_login
@require_admin
def delete_user():
    """Callback for admins to delete a user."""

    form = DeleteUserForm(request.form)

    if not form.validate():
        # DO NOT PRINT ERRORS, this error can only happen by editing HTML
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

    form = ActivateDeactivateUser(request.form)

    if not form.validate():
        # DO NOT PRINT ERRORS, this error can only happen by editing HTML
        return redirect(url_for('admin_pages.manage_users'))

    userid = int(form.userid.data)
    new_status = (form.new_activation_status.data == "True")

    if not UserManager.existsID(userid):
        flash(message="User does not exist.", category="error")
        return redirect(url_for('admin_pages.manage_users'))

    UserManager.updateUserActivity(userid, new_status)

    if new_status:
        flash(message="User activated.", category="success")
    else:
        flash(message="User deactivated.", category="success")

    return redirect(url_for('admin_pages.manage_users'))
# ENDFUNCTION

@admin_pages.route('/admin/manage_datasets/')
@require_login
@require_admin
def manage_datasets():
    """Returns a page with which an admin can remove and manage datasets."""

    dataset_list = DatasetManager.getAllDatasets()

    datasets = []

    for dataset in dataset_list:

        deleteform = DeleteDatasetForm()
        deleteform.fillForm(dataset)

        editform = DatasetEditForm()
        editform.fillForm(dataset)

        datasets.append({
            'datasetinfo': dataset.toDict(),
            'deleteform': deleteform,
            'editform': editform
        })
    # ENDFOR

    return render_template('admin_pages.manage_datasets.html', datasets = datasets)
# ENDFUNCTION

@admin_pages.route('/admin/edit_dataset/', methods=['POST'])
@require_login
@require_admin
def edit_dataset():
    """Callback for admins to edit the metadata of a dataset."""

    form = DatasetEditForm(request.form)

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('admin_pages.manage_datasets'))

    dataset_id = int(form.setid.data)

    if not DatasetManager.existsID(dataset_id):
        return redirect(url_for('admin_pages.manage_datasets'))

    dataset = DatasetManager.getDataset(dataset_id)

    flash(message="Information updated.", category="success")

    DatasetManager.changeMetadata(dataset_id, form.name.data, form.description.data)

    return redirect(url_for('admin_pages.manage_datasets'))
# ENDFUNCTION

@admin_pages.route('/admin/delete_dataset/', methods=['POST'])
@require_login
@require_admin
def delete_dataset():
    """Callback for admins to delete a dataset."""

    form = DeleteDatasetForm(request.form)

    if not form.validate():
        # DO NOT PRINT ERRORS, this error can only happen by editing HTML
        return redirect(url_for('admin_pages.manage_datasets'))

    dataset_id = int(form.setid.data)

    if not DatasetManager.existsID(dataset_id):
        return redirect(url_for('admin_pages.manage_datasets'))

    DatasetManager.destroyDataset(dataset_id)

    flash(message="Dataset deleted.", category="success")

    return redirect(url_for('admin_pages.manage_datasets'))
# ENDFUNCTION

@admin_pages.route('/admin/promote_demote', methods=['GET', 'POST'])
def promote_demote():
    """Page to promote and demote admins."""

    admin_form = AlterAdminForm(request.form)

    if request.method == "POST" and admin_form.validate():

        real_pincode = app.config['ADMIN_KEY']

        if admin_form.pincode.data != real_pincode:
            flash(message="Specified pincode is invalid.", category="error")
            return render_template('admin_pages.create_admin.html', admin_form = admin_form)

        if not UserManager.existsEmail(admin_form.email.data):
            flash(message="User with specified e-mail does not exist.", category="error")
            return render_template('admin_pages.create_admin.html', admin_form = admin_form)

        user = UserManager.getUserFromEmail(admin_form.email.data)

        if admin_form.action.data == "PROMOTE":
            # check if user is non-admin
            if user.admin:
                flash(message="Specified user is already an admin.", category="error")
                return render_template('admin_pages.create_admin.html', admin_form = admin_form)

            # make admin
            UserManager.editAdminStatus(user.userid, True)
        else:
            # check if user is admin
            if not user.admin:
                flash(message="Specified user is not an admin.", category="error")
                return render_template('admin_pages.create_admin.html', admin_form = admin_form)
            # demote admin
            print('test')
            UserManager.editAdminStatus(user.userid, False)

    return render_template('admin_pages.create_admin.html', admin_form = admin_form)
# ENDFUNCTION
