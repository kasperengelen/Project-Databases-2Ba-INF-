from flask import Blueprint, render_template, request, url_for, redirect, session, flash, abort, send_from_directory, jsonify
from flask import current_app as app
from utils import require_admin
from utils import require_login
from utils import require_adminperm, require_writeperm, require_readperm
from DatasetInfo import DatasetInfo
from DatasetManager import DatasetManager
from UserManager import UserManager
from dataset_forms import FindReplaceForm, DeleteAttrForm, DatasetForm, AddUserForm, RemoveUserForm, DatasetListEntryForm, TableUploadForm, DownloadForm, DataTypeTransform, NormalizeZScore, OneHotEncoding, TypeConversionTestForm
from TableViewer import TableViewer
from werkzeug.utils import secure_filename
import os
from DataLoader import DataLoader, FileException as DLFileExcept
import shutil

dataset_pages = Blueprint('dataset_pages', __name__)

@dataset_pages.route('/dataset/<int:dataset_id>/')
@require_login
@require_readperm
def view_dataset_home(dataset_id):
    """Returns a page that gives an overview of the
    dataset with the specified id."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    dataset_info = dataset.toDict()
    table_list = dataset.getTableNames()

    upload_form = TableUploadForm()

    perm_type = dataset.getPermForUserID(session['userdata']['userid'])

    return render_template('dataset_pages.home.html', dataset_info = dataset_info, table_list = table_list, form = upload_form, perm_type=perm_type, downloadform = DownloadForm())
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/<string:tablename>', defaults = {'page_nr': 1})
@dataset_pages.route('/dataset/<int:dataset_id>/<string:tablename>/<int:page_nr>')
@require_login
@require_readperm
def view_dataset_table(dataset_id, tablename, page_nr):

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    # get tableviewer
    tv = dataset.getTableViewer(tablename)

    # get info
    dataset_info = dataset.toDict()

    # CHECK IN RANGE
    if not tv.is_in_range(page_nr, 50):
        flash(message="Page out of range.", category="error")
        return redirect(url_for('dataset_pages.view_dataset_table', dataset_id=dataset_id, tablename = tablename, page_nr = 1))

    # RETRIEVE ATTRIBUTES
    attrs = tv.get_attributes()

    # FORMS
    findrepl_form = FindReplaceForm()
    delete_form = DeleteAttrForm()
    typeconversion_form = DataTypeTransform()
    testform = TypeConversionTestForm()
    onehotencodingform = OneHotEncoding()
    zscoreform = NormalizeZScore()
    findrepl_form.fillForm(attrs)
    delete_form.fillForm(attrs)
    typeconversion_form.fillForm(attrs)
    onehotencodingform.fillForm(attrs)
    zscoreform.fillForm(attrs)
    testform.fillForm(attrs)

    # render table
    table_data = tv.render_table(page_nr, 50)

    # get indices
    page_indices = tv.get_page_indices(display_nr = 50, page_nr = page_nr)

    # RETRIEVE USER PERMISSION
    perm_type = dataset.getPermForUserID(session['userdata']['userid'])

    # RETRIEVE COLUMN STATISTICS
    colstats = {}

    for attr_name in tv.get_attributes():
        colstats[attr_name] = {
            "nullfreq": 0, #tv.get_null_frequency(attr_name),
            "mostfreq": 0, #tv.get_most_frequent_value(attr_name),
            "max": 0, #tv.get_max(attr_name),
            "min": 0, #tv.get_min(attr_name),
            "avg": 0 #tv.get_avg(attr_name)
        }
    # ENDFOR

    return render_template('dataset_pages.table.html', 
                                                table_name = tablename,
                                                dataset_info = dataset_info,
                                                page_indices = page_indices,
                                                table_data = table_data,
                                                findrepl_form = findrepl_form,
                                                delete_form = delete_form, 
                                                typeconversion_form = typeconversion_form,
                                                onehotencodingform = onehotencodingform,
                                                zscoreform = zscoreform,
                                                perm_type = perm_type,
                                                current_page=page_nr,
                                                colstats=colstats,
                                                testform=testform)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/<string:tablename>/deleteattr', methods=['POST'])
@require_login
@require_writeperm
def transform_deleteattr(dataset_id, tablename):
    """Callback for delete attribute transformation."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)

    form = DeleteAttrForm(request.form)
    form.fillForm(tv.get_attributes())

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.view_dataset_table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    tt = dataset.getTableTransformer(tablename)

    tt.delete_attribute(tablename, form.select_attr.data)
    flash(message="Attribute deleted.", category="success")

    return redirect(url_for('dataset_pages.view_dataset_table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/<string:tablename>/findreplace', methods=['POST'])
@require_login
@require_writeperm
def transform_findreplace(dataset_id, tablename):
    """Callback for universal search and replace transformation."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)

    form = FindReplaceForm(request.form)
    form.fillForm(tv.get_attributes())

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.view_dataset_table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    tt = dataset.getTableTransformer(tablename)

    try:
        tt.find_and_replace(tablename, form.select_attr.data, form.search.data, form.replacement.data)
    except:
        flash(message="No matches found.", category="error")

    return redirect(url_for('dataset_pages.view_dataset_table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/<string:tablename>/_get_options')
@require_login
@require_writeperm
def _get_options(tablename):
    attr = request.args.get('attr', '01', type=str)
    options = [(option, option) for option in self.get_conversion_options(tablename, attr=attr)]
    return jsonify(options)

@dataset_pages.route('/dataset/<int:dataset_id>/<string:tablename>/typeconversion', methods = ['POST'])
@require_login
@require_writeperm
def transform_typeconversion(dataset_id, tablename):
    """Callback for typeconversion transformation."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    form = DataTypeTransform(request.form)
    form.fillForm(tv.get_attributes())

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.view_dataset_table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    if not form.new_datatype.data in tt.get_conversion_options(form.select_attr.data):
        flash(message="Selected datatype not compatible with the selected attribute.", category="error")
    else:
        try:
            tt.change_attribute_type(tablename, form.select_attr.data, form.new_datatype.data)
            flash(message="Attribute type changed.", category="success")
        except:
            flash(message="An error occurred.", category="error")

    return redirect(url_for('dataset_pages.view_dataset_table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/<string:tablename>/onehotencoding', methods = ['POST'])
@require_login
@require_writeperm
def transform_onehotencoding(dataset_id, tablename):
    """Callback for one hot encoding transformation."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    form = OneHotEncoding(request.form)
    form.fillForm(tv.get_attributes())

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.view_dataset_table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    try:
        tt.one_hot_encode(tablename, form.select_attr.data)
        flash(message="One hot encoding complete.", category="success")
    except:
        flash(message="An error occurred.", category="error")

    return redirect(url_for('dataset_pages.view_dataset_table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/<string:tablename>/zscorenormalisation', methods = ['POST'])
@require_login
@require_writeperm
def transform_zscorenormalisation(dataset_id, tablename):
    """Callback for z-score normalisation."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)
    tt = dataset.getTableTransformer(tablename)

    form = NormalizeZScore(request.form)
    form.fillForm(tv.get_attributes())

    if not form.validate():
        flash(message="Invalid form.", category="error")
        return redirect(url_for('dataset_pages.view_dataset_table', dataset_id=dataset_id, tablename=tablename, page_nr=1))

    try:
        tt.normalize_using_zscore(tablename, form.select_attr.data)
        flash(message="normalisation complete.", category="success")
    except:
        flash(message="An error occurred.", category="error")

    return redirect(url_for('dataset_pages.view_dataset_table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

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
        return render_template('dataset_pages.create.html', form=form)
# ENDFUNCTION

@dataset_pages.route('/dataset/list/')
@require_login
def list_dataset():
    """Returns a page that lists all the datasets that the currently
    logged-in user has access to."""

    datasets = DatasetManager.getDatasetsForUser(session['userdata']['userid'])

    setlist = [dataset.toDict() for dataset in datasets]

    dataset_forms = []

    for dataset in datasets:
        form = DatasetListEntryForm()
        form.fillForm(dataset)

        dataset_forms.append(form)

    return render_template('dataset_pages.list.html', setlist=setlist, dataset_forms=dataset_forms)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/leave/', methods=['POST'])
@require_login
@require_readperm
def leave(dataset_id):
    """Callback for leaving a dataset."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    try:
        flash(message="Leaving dataset complete.", category="success")
        dataset = DatasetManager.getDataset(dataset_id)
        dataset.removePerm(int(session['userdata']['userid']))
    except:
        flash(message="Error when leaving dataset.", category="error")

    return redirect(url_for('dataset_pages.list_dataset'))
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/manage/', methods=['GET', 'POST'])
@require_login
@require_adminperm
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
        return render_template('dataset_pages.manage.html', form = form, dataset_id = dataset_id)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/permissions')
@require_login
@require_adminperm
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

    return render_template('dataset_pages.edit_perms.html', 
                                            dataset_id = dataset_id, 
                                            add_user_form = add_user_form, 
                                            admin_form_list = admin_form_list,
                                            write_form_list = write_form_list,
                                            read_form_list = read_form_list)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/add_user', methods=['POST'])
@require_login
@require_adminperm
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
        flash(message="Cannot add user to dataset.", category="error")

    return redirect(url_for('dataset_pages.edit_perms_dataset', dataset_id=dataset_id))
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/remove_user', methods=['POST'])
@require_login
@require_adminperm
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
        flash(message="Cannot remove user from dataset.", category="error")

    return redirect(url_for('dataset_pages.edit_perms_dataset', dataset_id=dataset_id))
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/delete', methods=['POST'])
@require_login
@require_adminperm
def delete(dataset_id):
    """Callback to delete dataset."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    DatasetManager.destroyDataset(dataset_id)
    flash(message="Dataset deleted.", category="success")

    return redirect(url_for('dataset_pages.list_dataset'))
# ENDFUNCTION



@dataset_pages.route('/dataset/<int:dataset_id>/upload', methods=['POST'])
@require_login
@require_writeperm
def upload(dataset_id):
    """Callback to upload data."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    form = TableUploadForm()

    # HANDLE SUBMITTED FILE
    if request.method == 'POST' and form.validate():
        file = form.data_file.data
        columnnames_included = bool(form.columnnames_included.data)

        if file:
            sec_filename = secure_filename(file.filename)

            real_upload_folder = None

            # create upload folder
            # format: <UPLOAD_FOLDER>/<USER_ID>_<X>/<FILENAME>.<EXT>
            for i in range(100):
                temp_dir = os.path.join(app.config['UPLOAD_FOLDER'], str(session['userdata']['userid']) + "_" + str(i))

                if not os.path.exists(temp_dir):
                    os.makedirs(temp_dir)
                    real_upload_folder = temp_dir
                    break

            if real_upload_folder is None:
                abort(500)

            # SAVE FILE
            real_filename = os.path.join(real_upload_folder, sec_filename)
            file.save(real_filename)

            # HANDLE FILE WITH DATALOADER
            dl = DataLoader(dataset_id)
            
            try:
                dl.read_file(real_filename, columnnames_included)
            except DLFileExcept as e:
                flash(message=str(e), category="error")
                # delete file + folder
                shutil.rmtree(real_upload_folder, ignore_errors=True)
                return redirect(url_for('dataset_pages.view_dataset_home', dataset_id=dataset_id))
            # ENDTRY

            # delete file + folder
            shutil.rmtree(real_upload_folder, ignore_errors=True)

            flash(message="Tables added.", category="success")

    elif len(form.errors) > 0:
        # print errors
        for error_msg in form.errors['data_file']:
            flash(message=error_msg, category="error")

    return redirect(url_for('dataset_pages.view_dataset_home', dataset_id=dataset_id))
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/<string:tablename>/download/')
@require_login
@require_readperm
def download(dataset_id, tablename):
    """Callback to download the specified table from the specified dataset."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    # get form
    form = DownloadForm(request.args)

    if not form.validate():
        print(form.errors)
        flash(message="Invalid parameters.", category="error")
        return redirect(url_for('dataset_pages.view_dataset_home', dataset_id=dataset_id))

    tv = dataset.getTableViewer(tablename)

    real_download_dir = None

    # create download folder
    # format: <DOWNLOAD_FOLDER>/<USER_ID>_<X>/
    for i in range(100):
        temp_dir = os.path.join(app.config['DOWNLOAD_FOLDER'], str(session['userdata']['userid']) + "_" + str(i))

        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
            real_download_dir = temp_dir
            break

    if real_download_dir is None:
        abort(500)

    # GET PARAMETERS
    delimiter = str(form.delimiter.data)
    nullrep = str(form.nullrep.data)
    quotechar = str(form.quotechar.data)

    # PREPARE FILE FOR DOWNLOAD
    tv.to_csv(foldername=real_download_dir, delimiter=delimiter, null=nullrep, quotechar=quotechar)

    filename = tablename + ".csv"

    # SEND TO USER
    send_file = send_from_directory(real_download_dir, filename, mimetype="text/csv", attachment_filename=filename)

    # DELETE FILE
    shutil.rmtree(real_download_dir, ignore_errors=True)

    return send_file
# ENDFUNCTION