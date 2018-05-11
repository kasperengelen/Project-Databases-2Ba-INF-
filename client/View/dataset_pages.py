from flask import Blueprint, render_template, request, url_for, redirect, session, flash, abort, send_from_directory, jsonify
from flask import current_app as app

from Controller.AccessController import require_login, require_admin
from Controller.AccessController import require_adminperm, require_writeperm, require_readperm
from Controller.DatasetManager import DatasetManager
from Controller.UserManager import UserManager
from Controller.DatasetPermissionsManager import DatasetPermissionsManager
from Controller.TableViewer import TableViewer
from Controller.DataLoader import DataLoader, FileException as DLFileExcept

from View.dataset_forms import DatasetForm, AddUserForm, RemoveUserForm, LeaveForm, TableUploadForm, EntryCountForm, DownloadForm, TableJoinForm, AttributeForm, HistoryForm, AddUserForm, RemoveUserForm
from View.transf_forms import FindReplaceForm, DataTypeTransform, NormalizeZScore, OneHotEncoding, RegexFindReplace, DiscretizeEqualWidth, ExtractDateTimeForm
from View.transf_forms import DiscretizeEqualFreq, DiscretizeCustomRange, DeleteOutlier, FillNullsMean, FillNullsMedian, FillNullsCustomValue
from View.transf_forms import PredicateFormOne, PredicateFormTwo, PredicateFormThree, DeleteAttrForm
from View.form_utils import flash_errors

from werkzeug.utils import secure_filename
import os

import shutil
import webbrowser
from utils import get_db

dataset_pages = Blueprint('dataset_pages', __name__)

@dataset_pages.route('/dataset/<int:dataset_id>/')
@require_login
@require_readperm
def home(dataset_id):
    """Returns a page that gives an overview of the
    dataset with the specified id."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    dataset_info = dataset.toDict()
    table_list = dataset.getTableNames()
    original_table_list = dataset.getOriginalTableNames()

    upload_form = TableUploadForm()
    join_form = TableJoinForm()
    join_form.fillForm(table_list)
    editform = DatasetForm()
    editform.fillForm(dataset_info)

    # edit permissions form

    perm_type = DatasetPermissionsManager.getPermForUserID(dataset_id, session['userdata']['userid'])

    if session['userdata']['admin']:
        perm_type = 'admin'

    return render_template('dataset_pages.home.html', dataset_info        = dataset_info, 
                                                      table_list          = table_list,
                                                      original_table_list = original_table_list,
                                                      uploadform          = upload_form,
                                                      join_form           = join_form,
                                                      editform            = editform,
                                                      perm_type           = perm_type)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>', defaults = {'page_nr': 1})
@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/<int:page_nr>')# TODO remove
@require_login
@require_readperm
def table(dataset_id, tablename, page_nr):

    row_count = session['rowcount']

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    # get tableviewer
    tv = dataset.getTableViewer(tablename)

    # get info
    dataset_info = dataset.toDict()

    # TODO remove
    # CHECK IN RANGE
    if not tv.is_in_range(page_nr, row_count):
        flash(message="Page out of range.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename = tablename, page_nr = 1))

    # RETRIEVE ATTRIBUTES
    attrs = tv.get_attributes()

    # FORMS
    findrepl_form         = FindReplaceForm()
    delete_form           = DeleteAttrForm()
    typeconversion_form   = DataTypeTransform()
    onehotencodingform    = OneHotEncoding()
    zscoreform            = NormalizeZScore()
    regexfindreplace_form = RegexFindReplace()
    discretizeWidth_form  = DiscretizeEqualWidth()
    discretizeFreq_form   = DiscretizeEqualFreq()
    discretizeCustom_form = DiscretizeCustomRange()
    deleteoutlier_form    = DeleteOutlier()
    fillnullmean_form     = FillNullsMean()
    fillnullmedian_form   = FillNullsMedian()
    fillnullcustom_form   = FillNullsCustomValue()
    attr_form             = AttributeForm()
    predicateone_form     = PredicateFormOne()
    predicatetwo_form     = PredicateFormTwo()
    predicatethree_form   = PredicateFormThree()
    extract_form          = ExtractDateTimeForm()

    entrycount_form       = EntryCountForm(entry_count = session['rowcount'])# TODO remove
    entrycount_form.fillForm(dataset_id, tablename)# TODO remove

    # fill forms with data
    findrepl_form.fillForm(attrs)
    delete_form.fillForm(attrs)
    onehotencodingform.fillForm(attrs)
    zscoreform.fillForm(attrs)
    regexfindreplace_form.fillForm(attrs)
    discretizeWidth_form.fillForm(attrs)
    discretizeFreq_form.fillForm(attrs)
    discretizeCustom_form.fillForm(attrs)
    deleteoutlier_form.fillForm(attrs)
    fillnullmean_form.fillForm(attrs)
    fillnullmedian_form.fillForm(attrs)
    fillnullcustom_form.fillForm(attrs)
    attr_form.fillForm(attrs)
    typeconversion_form.fillForm(attrs, [], [])
    extract_form.fillForm(attrs)
    predicateone_form.fillForm(attrs)
    predicatetwo_form.fillForm(attrs)
    predicatethree_form.fillForm(attrs)

    # render table
    table_data = tv.render_table(page_nr, row_count, show_types = True)

    # get indices
    page_indices = tv.get_page_indices(display_nr = row_count, page_nr = page_nr)# TODO remove

    # RETRIEVE USER PERMISSION
    perm_type = DatasetPermissionsManager.getPermForUserID(dataset_id, session['userdata']['userid'])

    if session['userdata']['admin']:
        perm_type = 'admin'

    attributes = tv.get_attributes()

    # RETRIEVE COLUMN STATISTICS
    colstats = []

    for attr_name in tv.get_attributes():
        colstats.append({
            "attr_name": attr_name
        })

    return render_template('dataset_pages.table_test.html', 
                                                table_name            = tablename,
                                                dataset_info          = dataset_info,
                                                page_indices          = page_indices,# TODO remove
                                                table_data            = table_data,
                                                findrepl_form         = findrepl_form,
                                                delete_form           = delete_form, 
                                                typeconversion_form   = typeconversion_form,
                                                onehotencodingform    = onehotencodingform,
                                                zscoreform            = zscoreform,
                                                regexfindreplace_form = regexfindreplace_form,
                                                discretizeWidth_form  = discretizeWidth_form,
                                                discretizeFreq_form   = discretizeFreq_form,
                                                discretizeCustom_form = discretizeCustom_form,
                                                deleteoutlier_form    = deleteoutlier_form,
                                                fillnullmean_form     = fillnullmean_form,
                                                fillnullmedian_form   = fillnullmedian_form,
                                                fillnullcustom_form   = fillnullcustom_form,
                                                perm_type             = perm_type,
                                                current_page          = page_nr,# TODO remove
                                                colstats              = colstats,
                                                attr_form             = attr_form,
                                                entrycount_form       = entrycount_form,# TODO remove
                                                extract_form          = extract_form,
                                                predicateone_form     = predicateone_form,
                                                predicatetwo_form     = predicatetwo_form,
                                                predicatethree_form   = predicatethree_form,
                                                downloadform          = DownloadForm(),
                                                original              = False,
                                                row_count             = row_count,
                                                table_columns         = ['First', 'Second', 'Third'])
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/original_table/<string:tablename>', defaults = {'page_nr': 1})
@dataset_pages.route('/dataset/<int:dataset_id>/original_table/<string:tablename>/<int:page_nr>')# TODO remove
@require_login
@require_readperm
def table_original(dataset_id, tablename, page_nr):

    row_count = session['rowcount']

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)
    dataset_info = dataset.toDict()

    if tablename not in dataset.getOriginalTableNames():
        abort(404)

    # get tableviewer
    tv = dataset.getTableViewer(tablename, original = True)

    # CHECK IN RANGE
    if not tv.is_in_range(page_nr, row_count):# TODO remove
        flash(message="Page out of range.", category="error")
        return redirect(url_for('dataset_pages.table_original', dataset_id=dataset_id, tablename = tablename, page_nr = 1))

    # render table
    table_data = tv.render_table(page_nr, row_count, show_types = True)

    entrycount_form = EntryCountForm(entry_count = session['rowcount'])
    entrycount_form.fillForm(dataset_id, tablename)

    # get indices
    page_indices = tv.get_page_indices(display_nr = row_count, page_nr = page_nr)# TODO remove


    return render_template('dataset_pages.table.html',
                                                table_name      = tablename,
                                                dataset_info    = dataset_info,
                                                table_data      = table_data,
                                                entrycount_form = entrycount_form,# TODO remove
                                                page_indices    = page_indices,# TODO remove
                                                current_page    = page_nr,# TODO remove
                                                original        = True,
                                                downloadform    = DownloadForm())
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/history', defaults = {'page_nr': 1, 'tablename': None}, methods=["GET", "POST"])
@dataset_pages.route('/dataset/<int:dataset_id>/history/<int:page_nr>', defaults = {'tablename': None}, methods=["GET", "POST"])# TODO remove
@dataset_pages.route('/dataset/<int:dataset_id>/history/<string:tablename>', defaults = {'page_nr': 1}, methods=["GET", "POST"])
@dataset_pages.route('/dataset/<int:dataset_id>/history/<string:tablename>/<int:page_nr>', methods=["GET", "POST"])# TODO remove
@require_login
@require_readperm
def history(dataset_id, tablename, page_nr):
    
    rowcount = session['rowcount']

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)
    dataset_info = dataset.toDict()

    # handle POST request to change table
    if request.method == "POST":
        form = HistoryForm(request.form)
        form.fillForm(dataset.getTableNames())

        if not form.validate():
            flash_errors(form)
            return redirect(url_for("dataset_pages.history", dataset_id = dataset_id, tablename = tablename, page_nr = 1))
        else:
            tablename = form.options.data
            if tablename == '__dataset':
                tablename = None
            return redirect(url_for("dataset_pages.history", dataset_id = dataset_id, tablename = tablename, page_nr = 1))
    else:
        form = HistoryForm(options = tablename)
        form.fillForm(dataset.getTableNames())

    # check if current tablename is valid
    if tablename is not None and tablename not in dataset.getTableNames():
        abort(404)

    ## retrieve data from dhm
    show_all = (tablename is None)

    dhm = dataset.getHistoryManager()

    ## check if the page nr is in range
    if not dhm.is_in_range(page_nr = page_nr, nr_rows = rowcount, show_all = show_all, table_name = tablename):# TODO remove
        flash(message="Page out of range.", category="error")
        return redirect(url_for("dataset_pages.history", dataset_id = dataset_id, tablename = tablename, page_nr = 1))

    ## retrieve render code
    table_data = dhm.render_history_table(page_nr = page_nr, nr_rows = rowcount, show_all = show_all, table_name = tablename)

    ## retrieve page indices
    page_indices = dhm.get_page_indices(display_nr = rowcount, page_nr = page_nr)# TODO remove

    ## entry count
    entrycount_form = EntryCountForm(entry_count = session['rowcount'])# TODO remove
    entrycount_form.fillForm(dataset_id, tablename)# TODO remove

    ## render the template with the needed variables
    return render_template('dataset_pages.history.html',
                                            table_data      = table_data,
                                            table_name      = tablename,
                                            dataset_info    = dataset_info,
                                            page_indices    = page_indices,# TODO remove
                                            history_form    = form,
                                            current_page    = page_nr,# TODO remove
                                            entrycount_form = entrycount_form)# TODO remove
# ENDFUNCTION

# TODO remove page
@dataset_pages.route('/dataset/set_session_rowcount/<string:redirect_type>', methods = ['POST'])
@require_login
def set_session_rowcount(redirect_type):
    """Callback to set the session rowcount."""

    if not redirect_type in ['CURRENT', 'ORIGINAL', 'HISTORY']:
        abort(404)

    form = EntryCountForm(request.form)

    # retrieve raw data
    dataset_id = int(form.cur_dataset.data)
    tablename = form.cur_tablename.data

    # replace empty tablename with None
    if tablename == '':
        tablename = None

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    # check if dataset exists
    if tablename is not None:
        if redirect_type == "ORIGINAL" and not tablename in dataset.getOriginalTableNames():
            abort(404)
        elif redirect_type == "CURRENT" and not tablename in dataset.getTableNames():
            abort(404)
    # ENDIF

    form.fillForm(tablename, dataset_id)

    if not form.validate():
        flash_errors(form)
        abort(500)

    if tablename is None and redirect_type != 'HISTORY':
        abort(404)

    session['rowcount'] = int(form.entry_count.data)

    if redirect_type == 'CURRENT':
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename, page_nr=1))
    elif redirect_type == 'ORIGINAL':
        return redirect(url_for('dataset_pages.table_original', dataset_id = dataset_id, tablename = tablename, page_nr = 1))
    elif redirect_type == 'HISTORY':
        return redirect(url_for('dataset_pages.history', dataset_id = dataset_id, tablename = tablename, page_nr = 1))
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/jointables', methods=['POST'])
@require_login
@require_writeperm
def transform_join_tables(dataset_id):
    """Callback for joining tables"""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    form = TableJoinForm(request.form)
    form.fillForm(dataset.getTableNames())

    # determine valid attribute choices for tables

    table1_name = str(form.tablename1.data)
    table2_name = str(form.tablename2.data)

    # check if tables exist.
    if not (table1_name in dataset.getTableNames() and table2_name in dataset.getTableNames()):
        flash(message="Invalid table names.", category="error")
        return redirect(url_for('dataset_pages.home', dataset_id=dataset_id))

    table1_info = dataset.getTableViewer(table1_name)
    table2_info = dataset.getTableViewer(table2_name)

    form.fillTable1(table1_info.get_attributes())
    form.fillTable2(table2_info.get_attributes())

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.home', dataset_id=dataset_id))

    tt = dataset.getTableTransformer(table2_name)

    try:
        tt.join_tables(form.tablename1.data, form.tablename2.data, [form.attribute1.data], [form.attribute2.data], form.newname.data)
        flash(message="Tables joined", category="success")
    except Exception as e:
        flash(message="An error occurred. Details: " + str(e), category="error")
    return redirect(url_for('dataset_pages.home', dataset_id=dataset_id))
# ENDFUNCTION

@dataset_pages.route('/dataset/list/')
@require_login
def list():
    """Returns a page that lists all the datasets that the currently
    logged-in user has access to."""

    createform = DatasetForm()

    datasets = []
    for dataset in DatasetManager.getDatasetsForUser(session['userdata']['userid']):

        permtype = DatasetPermissionsManager.getPermForUserID(dataset.setid, session['userdata']['userid'])

        datasets.append({
            'datasetinfo': dataset.toDict(),
            'permtype': permtype
        })

    return render_template('dataset_pages.list.html', datasets=datasets, createform=createform)
# ENDFUNCTION

@dataset_pages.route('/dataset/create', methods=['POST'])
@require_login
def create():
    """Returns a page with which a dataset can be created."""

    form = DatasetForm(request.form)

    if form.validate():

        setid = DatasetManager.createDataset(form.name.data, form.description.data)
        dataset = DatasetManager.getDataset(setid)

        DatasetPermissionsManager.addPerm(setid, session['userdata']['userid'], 'admin')
        flash(message="The dataset was created.", category="success")

        return redirect(url_for('dataset_pages.home', dataset_id = setid))
    else:
        flash_errors(form)
        return redirect(url_for('dataset_pages.list'))
# ENDFUNCTION


@dataset_pages.route('/dataset/<int:dataset_id>/leave/', methods=['POST'])
@require_login
@require_readperm
def leave(dataset_id):
    """Callback for leaving a dataset."""

    try:
        DatasetPermissionsManager.removePerm(dataset_id, int(session['userdata']['userid']))
        flash(message="Leaving dataset complete.", category="success")
    except:
        pass

    return redirect(url_for('dataset_pages.list'))
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/edit_info/', methods=['POST'])
@require_login
@require_adminperm
def edit_info(dataset_id):
    """Callback to edit the metadata of a dataset."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    form = DatasetForm(request.form)

    dataset = DatasetManager.getDataset(dataset_id)

    if form.validate():
        DatasetManager.changeMetadata(dataset_id, form.name.data, form.description.data)
        flash(message="Information updated.", category="success")
    else:
        flash_errors(form)

    return redirect(url_for('dataset_pages.home', dataset_id = dataset_id))
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/permissions')
@require_login
@require_adminperm
def permissions(dataset_id):

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    members = []

    member_ids = DatasetPermissionsManager.getAdminPerms(dataset_id)
    member_ids.extend(DatasetPermissionsManager.getWritePerms(dataset_id))
    member_ids.extend(DatasetPermissionsManager.getReadPerms(dataset_id))

    for userid in member_ids:
        user = UserManager.getUserFromID(userid)

        perm_type = DatasetPermissionsManager.getPermForUserID(dataset_id, userid)

        removeform = RemoveUserForm()
        removeform.fillForm(user)

        # user cannot remove itself
        if userid == session['userdata']['userid']:
            removeform = None

        members.append({
            'userinfo': user.toDict(),
            'perm_type': perm_type,
            'removeform': removeform
        })

    add_user_form = AddUserForm()

    return render_template('dataset_pages.permissions.html', 
                                            dataset_id = dataset_id, 
                                            add_user_form = add_user_form, 
                                            members = members)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/add_user', methods=['POST'])
@require_login
@require_adminperm
def add_user(dataset_id):
    """Callback for form to add user access to dataset."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    form = AddUserForm(request.form)

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.permissions', dataset_id=dataset_id))

    
    dataset = DatasetManager.getDataset(dataset_id)

    if not UserManager.existsEmail(form.email.data):
        flash(message="There is no user with the specified email address.", category="error")
        return redirect(url_for('dataset_pages.permissions', dataset_id=dataset_id))

    userid = UserManager.getUserFromEmail(form.email.data).userid

    if DatasetPermissionsManager.getPermForUserID(dataset_id, userid):
        flash(message="User already has access to the dataset.", category="error")
        return redirect(url_for('dataset_pages.permissions', dataset_id=dataset_id))

    DatasetPermissionsManager.addPerm(dataset_id, userid, form.permission_type.data)
    
    return redirect(url_for('dataset_pages.permissions', dataset_id=dataset_id))
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/remove_user', methods=['POST'])
@require_login
@require_adminperm
def remove_user(dataset_id):
    """Callback for form to remove user access from dataset."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    form = RemoveUserForm(request.form)

    if not form.validate():
        return redirect(url_for('dataset_pages.permissions', dataset_id=dataset_id))

    dataset = DatasetManager.getDataset(dataset_id)

    if int(form.userid.data) == session['userdata']['userid']:
        flash(message="User cannot remove itself from dataset.", category="error")
        return redirect(url_for('dataset_pages.permissions', dataset_id=dataset_id))
    try:
        DatasetPermissionsManager.removePerm(dataset_id, int(form.userid.data))
    except RuntimeError as e:
        flash(message="Cannot remove user from dataset.", category="error")

    return redirect(url_for('dataset_pages.permissions', dataset_id=dataset_id))
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

    return redirect(url_for('dataset_pages.list'))
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/delete', methods=['POST'])
@require_login
@require_adminperm
def delete_table(dataset_id, tablename):
    """Callback to delete a table."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    dataset.deleteTable(tablename)

    flash(message="Table deleted.", category="success")

    return redirect(url_for('dataset_pages.home', dataset_id = dataset_id))
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
            dl = DataLoader(dataset_id, get_db())
            
            try:
                dl.read_file(real_filename, columnnames_included)
            except DLFileExcept as e: # DLFileExcept = FileException
                flash(message=str(e), category="error")
                # TODO print error message
                # delete file + folder
                shutil.rmtree(real_upload_folder, ignore_errors=True)
                return redirect(url_for('dataset_pages.home', dataset_id=dataset_id))
            # ENDTRY

            # delete file + folder
            shutil.rmtree(real_upload_folder, ignore_errors=True)

            flash(message="Tables added.", category="success")

    elif len(form.errors) > 0:
        # print errors
        for error_msg in form.errors['data_file']:
            flash(message=error_msg, category="error")

    return redirect(url_for('dataset_pages.home', dataset_id=dataset_id))
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/download/', defaults = {'original': False})
@dataset_pages.route('/dataset/<int:dataset_id>/original_table/<string:tablename>/download', defaults = {'original': True})
@require_login
@require_readperm
def download_table(dataset_id, tablename, original):
    """Callback to download the specified table from the specified dataset."""

    if original:
        mode = "ORIGINAL"
    else:
        mode = "TABLE"

    return __download_helper(dataset_id=dataset_id, mode=mode, tablename=tablename)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/download/')
@require_login
@require_readperm
def download_dataset(dataset_id):
    """Callback to download all the tables from the specified dataset."""
    return __download_helper(dataset_id, mode="DATASET")
# ENDFUNCTION

def __download_helper(dataset_id, mode, tablename = None):
    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    # get form
    form = DownloadForm(request.args)

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.home', dataset_id=dataset_id))

    if not mode in ["TABLE", "ORIGINAL", "DATASET"]:
        raise RuntimeError("Invalid download mode")

    if mode == "TABLE":
        if not tablename in dataset.getTableNames():
            abort(404)
    elif mode == "ORIGINAL":
        if not tablename in dataset.getOriginalTableNames():
            abort(404)

    real_download_dir = None

    # create download folder
    # format: <DOWNLOAD_FOLDER>/<USER_ID>_<X>/
    for i in range(100):
        temp_dir = os.path.join(app.config['DOWNLOAD_FOLDER'], str(session['userdata']['userid']) + "_" + str(i))

        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
            real_download_dir = temp_dir
            break
    # ENDFOR

    if real_download_dir is None:
        abort(500)

    # GET PARAMETERS
    delimiter = str(form.delimiter.data)
    nullrep = str(form.nullrep.data)
    quotechar = str(form.quotechar.data)

    # PREPARE FILE FOR DOWNLOAD
    dd = dataset.getDownloader()
    if mode == "DATASET":
        dd.get_csv_zip(foldername=real_download_dir, delimiter=delimiter, null=nullrep, quotechar=quotechar)
    elif mode == "TABLE":
        dd.to_csv(tablename=tablename, foldername=real_download_dir, delimiter=delimiter, null=nullrep, quotechar=quotechar, original = False)
    elif mode == "ORIGINAL":
        dd.to_csv(tablename=tablename, foldername=real_download_dir, delimiter=delimiter, null=nullrep, quotechar=quotechar, original = True)

    # determine the filename of the zip
    filename = str(dataset_id) + ".zip"

    # SEND TO USER
    send_file = send_from_directory(real_download_dir, filename, mimetype="text/csv", attachment_filename=filename, as_attachment = True)

    # DELETE FILE
    shutil.rmtree(real_download_dir, ignore_errors=True)

    return send_file
# ENDFUNCTION

############################################################# DYNAMIC CALLBACKS #############################################################

@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/_get_options')
@require_login
@require_writeperm
def _get_options(dataset_id, tablename):
    """Callback for dynamic forms."""
    attr = request.args.get('attribute', '01', type=str)

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tt = dataset.getTableTransformer(tablename)
    tv = dataset.getTableViewer(tablename)

    if not attr in tv.get_attributes():
        abort(404)

    options = [(option, option) for option in tt.get_conversion_options(tablename, attribute=attr)]
    return jsonify(options)

# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/_get_datetype')
@require_login
@require_writeperm
def _get_datetype(dataset_id, tablename):
    """Callback for dynamic forms."""

    type = request.args.get('options', '01', type=str)

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tt = dataset.getTableTransformer(tablename)
    tv = dataset.getTableViewer(tablename)

    print(type)

    datetypes = [(datetype, datetype) for datetype in tt.get_datetime_formats(type)]
    return jsonify(datetypes)

# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/_get_hist_num')
@require_login
@require_readperm
def _get_hist_num(dataset_id, tablename):
    """Callback for dynamic forms."""
    attr_name = request.args.get('view_attr', '01', type=str)

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tt = dataset.getTableTransformer(tablename)
    tv = dataset.getTableViewer(tablename)

    if not attr_name in tv.get_attributes():
        abort(404)

    hist_num = tv.get_numerical_histogram(attr_name)
    return hist_num
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/_get_chart_freq')
@require_login
@require_readperm
def _get_chart_freq(dataset_id, tablename):
    """Callback for dynamic forms."""
    attr_name = request.args.get('view_attr', '01', type=str)

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tt = dataset.getTableTransformer(tablename)
    tv = dataset.getTableViewer(tablename)

    if not attr_name in tv.get_attributes():
        abort(404)

    chart_freq = tv.get_frequency_pie_chart(attr_name)
    return chart_freq
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/_get_colstats')
@require_login
@require_readperm
def _get_colstats(dataset_id, tablename):
    """Callback for dynamic forms."""
    attr_name = request.args.get('view_attr', '01', type=str)

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tt = dataset.getTableTransformer(tablename)
    tv = dataset.getTableViewer(tablename)

    if not attr_name in tv.get_attributes():
        abort(404)

    colstats = [tv.get_avg(attr_name),
                tv.get_min(attr_name),
                tv.get_max(attr_name),
                tv.get_null_frequency(attr_name),
                tv.get_most_frequent_value(attr_name)]

    colstats_string = "<table><tr><th>Average</th><th>Minimum</th><th>Maximum</th><th>Null Frequency</th><th>Most Frequent</th></tr>"

    for stat in colstats:
        colstats_string += "<td>" + str(stat) + "</td>"

    colstats_string += "</table>"

    return colstats_string
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/_get_attr1_options')
@require_login
@require_writeperm
def _get_attr1_options(dataset_id):
    tablename = request.args.get('tablename1', '01', type=str)

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)

    options = [(option, option) for option in tv.get_attributes()]

    return jsonify(options)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/_get_attr2_options')
@require_login
@require_writeperm
def _get_attr2_options(dataset_id):
    tablename = request.args.get('tablename2', '01', type=str)

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)

    options = [(option, option) for option in tv.get_attributes()]

    return jsonify(options)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/_get_table', defaults = {'original': False})
@dataset_pages.route('/dataset/<int:dataset_id>/original_table/<string:tablename>/_get_table', defaults = {'original': True})
def _get_table(dataset_id, tablename, original):
    """Callback to retrieve the dataset in JSON format."""
    # TODO page_nr, entry count

    keys = [key for key in request.args]
    keys.sort()
    for key in keys:
        print(key,":",request.args[key])

    start_nr  = int(request.args["start"])
    row_count = int(request.args["length"])

    col_nr     = int(request.args["order[0][column]"])
    sort_order = request.args["order[0][dir]"]

    # set current session row_count to the specified count
    session['rowcount'] = row_count

    retval = {
        'recordsTotal': 200,
        'recordsFiltered': 200,
        'data': [ [str(i), str(i+1), str(i+2)] for i in range(0, int(request.args["length"]))]
    }

    return jsonify(retval)
# ENDFUNCTION

# @dataset_pages.route('/dataset/<int:dataset_id>/history/dataset/_get_table')
# @dataset_pages.route('/dataset/<int:dataset_id>/history/table/<string:tablename>/_get_table')
# def _get_history_data(dataset_id, tablename):
#     pass