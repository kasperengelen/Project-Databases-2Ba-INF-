from flask import Blueprint, render_template, request, url_for, redirect, session, flash, abort
from utils import require_admin
from utils import require_login
from DatasetInfo import DatasetInfo
from DatasetManager import DatasetManager
from UserManager import UserManager
from dataset_forms import FindReplaceForm, DeleteAttrForm, DatasetForm, AddUserForm, RemoveUserForm
from TableViewer import TableViewer

dataset_pages = Blueprint('dataset_pages', __name__)

@dataset_pages.route('/dataset/<int:dataset_id>/')
@require_login
def view_dataset_home(dataset_id):
    """Returns a page that gives an overview of the
    dataset with the specified id."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    dataset_info = dataset.toDict()
    table_list = dataset.getTableNames()

    return render_template('dataset_view_home.html', dataset_info = dataset_info, table_list = table_list)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/<string:tablename>', defaults = {'page_nr': 1})
@dataset_pages.route('/dataset/<int:dataset_id>/<string:tablename>/<int:page_nr>')
@require_login
def view_dataset_table(dataset_id, tablename, page_nr):

    findrepl_form = FindReplaceForm()
    delete_form = DeleteAttrForm()

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    # RETRIEVE ATTRIBUTES
    # FILL FORMS

    # CHECK IN RANGE

    dataset_info = dataset.toDict()
    table_list = dataset.getTableNames()

    return render_template('dataset_transform.html', 
                                                dataset_info = dataset_info,
                                                page_indices = page_indices,
                                                table_data = table_data,
                                                findrepl_form = findrepl_form,
                                                delete_form = delete_form)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/<string:tablename>/deleteattr', methods=['POST'])
@require_login
def transform_deleteattr(dataset_id, tablename):
    """Callback for delete attribute transformation."""
    tv = TableViewer()



    return ""

@dataset_pages.route('/dataset/<int:dataset_id>/<string:tablename>/findreplace', methods=['POST'])
@require_login
def transform_findreplace(dataset_id, tablename):
    return ""

@dataset_pages.route('/dataset/create', methods=['GET', 'POST'])
@require_login
def create_dataset():
    """Returns a page with which a dataset can be created."""

    form = DatasetForm(request.form)

    if request.method == 'POST' and form.validate():

        setid = DatasetManager.createDataset(form.name.data, form.description.data)
        dataset = DatasetManager.getDataset(setid)

        dataset.addPerm(session['userdata']['email'], 'admin')
        flash(message="The dataset was created.", category="success")

        return redirect(url_for('dataset_pages.view_dataset_home', dataset_id = setid))
    else:
        return render_template('dataset_create.html', form=form)
# ENDFUNCTION

@dataset_pages.route('/dataset/list')
@require_login
def list_dataset():
    """Returns a page that lists all the datasets that the currently
    logged-in user has access to."""

    datasets = DatasetManager.getDatasetsForUser(session['userdata']['userid'])

    setlist = [dataset.toDict() for dataset in datasets]

    return render_template('dataset_list.html', setlist=setlist)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/manage/', methods=['GET', 'POST'])
@require_login
def manage_dataset(dataset_id):
    """Returns a page with which the metadata of the
    dataset with the specified id can be changed."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    form = DatasetForm(request.form)

    dataset = DatasetManager.getDataset(dataset_id)

    if request.method == 'POST' and form.validate():
        dataset.changeMetadata(form.name.data, form.description.data)
        flash(message="Information updated.", category="success")
        return redirect(url_for('dataset_pages.view_dataset_home', dataset_id = dataset_id))
    else:
        form.fillForm(dataset.toDict())
        return render_template('dataset_manage.html', form = form)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/permissions')
@require_login
def edit_perms_dataset(dataset_id):

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    ## RETRIEVE ADMIN FORMS
    admin_form_list = []
    admins = dataset.getAdminPerms()
    admins.remove(session['userdata']['userid']) # ADMIN CANNOT REMOVE ITSELF FROM DATASET.

    for admin_id in admins:
        admin_user = UserManager.getUserFromID(admin_id)

        form = RemoveUserForm()

        form.userid.data = admin_user.userid
        form.email.data = admin_user.email
        form.permission_type.data = 'admin'

        admin_form_list.append(form)
    # ENDFOR


    ## RETRIEVE WRITER FORMS
    write_form_list = []
    writers = dataset.getWritePerms()
    for writer_id in writers:
        write_user = UserManager.getUserFromID(writer_id)

        form = RemoveUserForm()

        form.userid.data = write_user.userid
        form.email.data = write_user.email
        form.permission_type.data = 'write'

        write_form_list.append(form)
    # ENDFOR


    ## RETRIEVE READER FORMS
    read_form_list = []
    readers = dataset.getReadPerms()

    for reader_id in readers:
        read_user = UserManager.getUserFromID(reader_id)

        form = RemoveUserForm()

        form.userid.data = read_user.userid
        form.email.data = read_user.email
        form.permission_type.data = 'read'

        read_form_list.append(form)
    # ENDFOR

    add_user_form = AddUserForm()

    return render_template('dataset_permissions.html', 
                                            dataset_id = dataset_id, 
                                            add_user_form = add_user_form, 
                                            admin_form_list = admin_form_list,
                                            write_form_list = write_form_list,
                                            read_form_list = read_form_list)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/add_user', methods=['POST'])
@require_login
def add_user_dataset(dataset_id):
    """Callback for form to add user access to dataset."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    form = AddUserForm(request.form)

    if not form.validate():
        flash(message="Invalid form, please check email and permission type.", category="error")
        return redirect(url_for('dataset_pages.edit_perms_dataset', dataset_id=dataset_id))

    try:
        dataset = DatasetManager.getDataset(dataset_id)
        dataset.addPerm(form.email.data, form.permission_type.data)
    except RuntimeError as e:
        flash(message=e.args[0], category="error")

    return redirect(url_for('dataset_pages.edit_perms_dataset', dataset_id=dataset_id))
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/remove_user', methods=['POST'])
@require_login
def remove_user_dataset(dataset_id):
    """Callback for form to remove user access from dataset."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    form = RemoveUserForm(request.form)

    if not form.validate():
        return redirect(url_for('dataset_pages.edit_perms_dataset', dataset_id=dataset_id))

    try:
        dataset = DatasetManager.getDataset(dataset_id)

        if int(form.userid.data) == session['userdata']['userid']:
            flash(message="User cannot remove itself from dataset.", category="error")
            return redirect(url_for('dataset_pages.edit_perms_dataset', dataset_id=dataset_id))
        dataset.removePerm(int(form.userid.data))
    except RuntimeError as e:
        flash(message=e.args[0], category="error")

    return redirect(url_for('dataset_pages.edit_perms_dataset', dataset_id=dataset_id))
# ENDFUNCTION
