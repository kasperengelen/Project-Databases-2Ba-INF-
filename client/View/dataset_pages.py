from flask import Blueprint, render_template, request, url_for, redirect, session, flash, abort, send_from_directory, jsonify
from flask import current_app as app

from Controller.AccessController import require_login, require_admin
from Controller.AccessController import require_adminperm, require_writeperm, require_readperm
from Controller.DatasetManager import DatasetManager
from Controller.UserManager import UserManager
from Controller.DatasetPermissionsManager import DatasetPermissionsManager
from Controller.TableViewer import TableViewer

from Model.TableUploader import FileException as DLFileExcept
from Controller.TableJoiner import JoinException

from View.dataset_forms import DatasetForm, AddUserForm, RemoveUserForm, LeaveForm, TableUploadForm, TableJoinForm, AttributeForm, HistoryForm, AddUserForm, RemoveUserForm
from View.dataset_forms import DownloadDatasetCSVForm, DownloadDatasetSQLForm, DownloadTableCSVForm, DownloadTableSQLForm, CustomQueryForm, CopyTableForm
from View.transf_forms import FindReplaceForm, DataTypeTransform, NormalizeZScore, OneHotEncoding, RegexFindReplace, DiscretizeEqualWidth, ExtractDateTimeForm
from View.transf_forms import DiscretizeEqualFreq, DiscretizeCustomRange, DeleteOutlier, FillNullsMean, FillNullsMedian, FillNullsCustomValue, DedupForm
from View.transf_forms import PredicateFormOne, PredicateFormTwo, PredicateFormThree
from View.form_utils import flash_errors

from werkzeug.utils import secure_filename
import os
import shutil
import json

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
                                                      download_csv_form   = DownloadDatasetCSVForm(),
                                                      download_sql_form   = DownloadDatasetSQLForm(),
                                                      perm_type           = perm_type,
                                                      queryform           = CustomQueryForm())
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>')
@require_login
@require_readperm
def table(dataset_id, tablename):

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


    # RETRIEVE ATTRIBUTES
    attrs = tv.get_attributes()

    # FORMS
    findrepl_form         = FindReplaceForm()
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
    dedup_form            = DedupForm()

    # fill forms with data
    findrepl_form.fillForm(attrs)
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
    dedup_form.fillForm(attrs)



    # RETRIEVE USER PERMISSION
    perm_type = DatasetPermissionsManager.getPermForUserID(dataset_id, session['userdata']['userid'])

    if session['userdata']['admin']:
        perm_type = 'admin'

    # create attribute list
    attr_map = tv.get_columntype_dict()

    attribute_list = []

    for attr in tv.get_attributes():
        attribute_list.append({
            "name": attr,
            "type": attr_map[attr]
        })
    # ENDFOR

    return render_template('dataset_pages.table.html', 
                                                table_name            = tablename,
                                                dataset_info          = dataset_info,
                                                findrepl_form         = findrepl_form,
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
                                                attr_form             = attr_form,
                                                extract_form          = extract_form,
                                                predicateone_form     = predicateone_form,
                                                predicatetwo_form     = predicatetwo_form,
                                                predicatethree_form   = predicatethree_form,
                                                download_csv_form     = DownloadDatasetCSVForm(),
                                                download_sql_form     = DownloadTableSQLForm(),
                                                original              = False,
                                                row_count             = row_count,
                                                attribute_list        = attribute_list,
                                                queryform             = CustomQueryForm(),
                                                copyform              = CopyTableForm(),
                                                dedup_form            = dedup_form)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/original_table/<string:tablename>')
@require_login
@require_readperm
def table_original(dataset_id, tablename):

    row_count = session['rowcount']

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)
    dataset_info = dataset.toDict()

    if tablename not in dataset.getOriginalTableNames():
        abort(404)

    # get tableviewer
    tv = dataset.getTableViewer(tablename, original = True)

    # create attribute list
    attr_map = tv.get_columntype_dict()

    attribute_list = []

    for attr in tv.get_attributes():
        attribute_list.append({
            "name": attr,
            "type": attr_map[attr]
        })
    # ENDFOR

    return render_template('dataset_pages.table.html',
                                                table_name        = tablename,
                                                dataset_info      = dataset_info,
                                                original          = True,
                                                row_count         = row_count,
                                                download_csv_form = DownloadDatasetCSVForm(),
                                                download_sql_form = DownloadTableSQLForm(),
                                                attribute_list    = attribute_list)
# ENDFUNCTION

# TODO check if this works
@dataset_pages.route('/dataset/<int:dataset_id>/history/dataset', defaults = {'tablename': None}, methods=["GET", "POST"])
@dataset_pages.route('/dataset/<int:dataset_id>/history/table/<string:tablename>', methods=["GET", "POST"])
@require_login
@require_readperm
def history(dataset_id, tablename):
    
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
            return redirect(url_for("dataset_pages.history", dataset_id = dataset_id, tablename = tablename))
        else:
            tablename = form.options.data
            if tablename == '__dataset':
                tablename = None
            return redirect(url_for("dataset_pages.history", dataset_id = dataset_id, tablename = tablename))
    else:
        form = HistoryForm(options = tablename)
        form.fillForm(dataset.getTableNames())

    # check if current tablename is valid
    if tablename is not None and tablename not in dataset.getTableNames():
        abort(404)

    ## render the template with the needed variables
    return render_template('dataset_pages.history.html',
                                            table_name   = tablename,
                                            dataset_info = dataset_info,
                                            row_count    = rowcount,
                                            history_form = form)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/jointables', methods=['POST'])
@require_login
@require_writeperm
def join_tables(dataset_id):
    """Callback for joining tables"""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    form = TableJoinForm(request.form)
    form.fillForm(dataset.getTableNames())

    # determine valid attribute choices for tables

    table1_name = str(form.tablename1.data)
    table2_name = str(form.tablename2.data)
    newtable_name = str(form.newname.data)

    # check if tables exist.
    if not (table1_name in dataset.getTableNames() and table2_name in dataset.getTableNames()):
        flash(message="Invalid table names.", category="error")
        return redirect(url_for('dataset_pages.home', dataset_id=dataset_id))

    table1_info = dataset.getTableViewer(table1_name)
    table2_info = dataset.getTableViewer(table2_name)

    # fill anyways
    form.fillTable1(table1_info.get_attributes())
    form.fillTable2(table2_info.get_attributes())

    if not form.validate():
        flash_errors(form)
        return redirect(url_for('dataset_pages.home', dataset_id=dataset_id))

    # retrieve attr data
    table1_attr = form.attribute1.data
    table2_attr = form.attribute2.data

    join_type = form.join_type.data
    join_subtype = form.join_subtype.data

    tj = dataset.getTableJoiner(table1 = table1_name, table2=table2_name, newtable=newtable_name)

    try:
        if join_type == "normal":
            tj.normal_join([table1_attr],[table2_attr], type=join_subtype)
        else:
            tj.natural_join(type=join_subtype)
        flash(message="Tables joined", category="success")
    except JoinException as e:
        flash(message=str(e), category="error")
    
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

# TODO
@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/copy', methods=['POST'])
def copy_table(dataset_id, tablename):
    """Callback to copy a table."""

    form = CopyTableForm(request.form)

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if not tablename in dataset.getTableNames():
        abort(404)

    tt = dataset.getTableTransformer(tablename)

    newname = form.table_name.data

    if newname in dataset.getTableNames():
        flash(message="Specified tablename is already in use.", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
    # ENDIF

    # do copy
    tt.copy_table(old=tablename, new=newname)
    flash(message="Table copy has been made.", category="success")
    return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=newname))
# ENDFUNCTION


@dataset_pages.route('/dataset/<int:dataset_id>/upload', methods=['POST'])
@require_login
@require_writeperm
def upload(dataset_id):
    """Callback to upload data."""

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    form = TableUploadForm()

    # HANDLE SUBMITTED FILE
    if request.method == 'POST' and form.validate():
        file = form.data_file.data
        columnnames_included = form.columnnames_included.data
        automatic_types      = form.automatic_types.data

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
            tu = dataset.getUploader()
            
            try:
                tu.read_file(filename=real_filename, header=columnnames_included, automatic_type_conversion=automatic_types)
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

@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/download/CSV',          defaults = {'original': False, 'fileformat': 'CSV'})
@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/download/SQL',          defaults = {'original': False, 'fileformat': 'SQL'})
@dataset_pages.route('/dataset/<int:dataset_id>/original_table/<string:tablename>/download/CSV', defaults = {'original': True,  'fileformat': 'CSV'})
@dataset_pages.route('/dataset/<int:dataset_id>/original_table/<string:tablename>/download/SQL', defaults = {'original': True,  'fileformat': 'SQL'})
@require_login
@require_readperm
def download_table(dataset_id, tablename, original, fileformat):
    """Callback to download the specified table from the specified dataset."""

    if original:
        mode = "ORIGINAL"
    else:
        mode = "TABLE"

    return __download_helper(dataset_id=dataset_id, mode=mode, tablename=tablename, fileformat = fileformat)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/download/CSV', defaults = {'fileformat': 'CSV'})
@dataset_pages.route('/dataset/<int:dataset_id>/download/SQL', defaults = {'fileformat': 'SQL'})
@require_login
@require_readperm
def download_dataset(dataset_id, fileformat):
    """Callback to download all the tables from the specified dataset."""
    return __download_helper(dataset_id=dataset_id, mode="DATASET", fileformat = fileformat)
# ENDFUNCTION

def __download_helper(dataset_id, mode, fileformat, tablename = None):
    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if mode == "DATASET" and fileformat == "CSV":
        form = DownloadDatasetCSVForm(request.args)
    elif mode == "DATASET" and fileformat == "SQL":
        form = DownloadDatasetSQLForm(request.args)
    elif mode in ["TABLE", "ORIGINAL"] and fileformat == "CSV":
        form = DownloadTableCSVForm(request.args)
    elif mode in ["TABLE", "ORIGINAL"] and fileformat == "SQL":
        form = DownloadTableSQLForm(request.args)

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

    if fileformat == 'CSV':
        # GET PARAMETERS
        delimiter = str(form.delimiter.data)
        nullrep = str(form.nullrep.data)
        quotechar = str(form.quotechar.data)

        # PREPARE FILE FOR DOWNLOAD
        dd = dataset.getDownloader()
        if mode == "DATASET":
            filename = dd.get_csv_zip(foldername=real_download_dir, delimiter=delimiter, null=nullrep, quotechar=quotechar, original=form.original_check.data)
        elif mode == "TABLE":
            filename = dd.get_csv(tablename=tablename, foldername=real_download_dir, delimiter=delimiter, null=nullrep, quotechar=quotechar, original = False)
        elif mode == "ORIGINAL":
            filename = dd.get_csv(tablename=tablename, foldername=real_download_dir, delimiter=delimiter, null=nullrep, quotechar=quotechar, original = True)
        
    elif fileformat == 'SQL':
        dd = dataset.getDownloader()
        if mode == "DATASET":
            filename = dd.get_dataset_dump(foldername=real_download_dir, original=form.original_check.data)
        elif mode == "TABLE":
            filename = dd.get_table_dump(tablename=tablename, foldername=real_download_dir, original=False)
        elif mode == "ORIGINAL":
            filename = dd.get_table_dump(tablename=tablename, foldername=real_download_dir, original=True)
    else:
        raise RuntimeError("Invalid file format: '" + fileformat + "'.")

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

@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/<string:attr_name>/_get_chart_freq')
@require_login
@require_readperm
def _get_chart_freq(dataset_id, tablename, attr_name):
    """Callback for dynamic forms."""

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

    retval = {
        "labels": chart_freq[0],
        "sizes": chart_freq[1]
    }

    return jsonify(retval)
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
@require_login
@require_readperm
def _get_table(dataset_id, tablename, original):
    """Callback to retrieve the dataset in JSON format."""

    start_nr   = request.args.get('start',            type=int)
    row_count  = request.args.get('length',           type=int)
    col_nr     = request.args.get('order[0][column]', type=int)
    sort_order = request.args.get('order[0][dir]',    type=str)

    # set current session row_count to the specified count
    session['rowcount'] = row_count



    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if original:
        if not tablename in dataset.getOriginalTableNames():
            abort(404)
    else:
        if not tablename in dataset.getTableNames():
            abort(404)

    tv = dataset.getTableViewer(tablename, original = original)

    col_name = tv.get_attributes()[col_nr]

    data = tv.render_json(offset = start_nr, limit = row_count, order = True, ascending = (sort_order == 'asc'), on_column=col_name)

    retval = {
        'recordsTotal':    int(tv.get_rowcount()),
        'recordsFiltered': int(tv.get_rowcount()),
        'data':            json.loads(data)
    }

    return jsonify(retval)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/history/dataset/_get_table', defaults = {'tablename': None})
@dataset_pages.route('/dataset/<int:dataset_id>/history/table/<string:tablename>/_get_table')
@require_login
@require_readperm
def _get_history_table(dataset_id, tablename):
    """Callback to retrieve the history table in JSON format."""

    start_nr   = request.args.get('start',            type=int)
    row_count  = request.args.get('length',           type=int)
    col_nr     = request.args.get('order[0][column]', type=int)
    sort_order = request.args.get('order[0][dir]',    type=str)

    if col_nr != 0:
        abort(500)

    session['rowcount'] = row_count

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)
    dhm = dataset.getHistoryManager()

    if tablename is None:
        # entire dataset

        total_rowcount = dhm.get_rowcount()
        data = dhm.render_history_json(offset = start_nr, limit = row_count, show_all = True, reverse_order = (sort_order == "asc"))
    else:
        # only tablename
        if not tablename in dataset.getTableNames():
            abort(404)

        total_rowcount = dhm.get_rowcount(tablename = tablename)
        data = dhm.render_history_json(offset = start_nr, limit = row_count, show_all = False, table_name = tablename, reverse_order = (sort_order == "asc"))
    # ENDIF

    retval = {
        'recordsTotal': total_rowcount,
        'recordsFiltered': total_rowcount,
        'data': json.loads(data)
    }

    return jsonify(retval)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/_custom_query', methods=['POST'])
@require_login
@require_readperm
def _custom_query(dataset_id):
    """Callback to execute custom queries."""

    form = CustomQueryForm(request.form)

    print(form.query.data)

    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)
    write = DatasetPermissionsManager.userHasSpecifiedAccessTo(setid=dataset_id, userid=session['userdata']['userid'], minimum_perm_type="write") or session['userdata']['admin']

    qe = dataset.getQueryExecutor(write_perm = write)

    retval = {
        "data": None,
        "columns": None,
        "empty": True,
        "error": False,
        "error_msg": ""
    }

    if not form.validate():
        retval["error"] = True
        # TODO retrieve errors
        retval["error_msg"] = "Invalid form."

        return jsonify(retval)
    # ENDIF

    try:
        result = qe.execute_transaction(form.query.data)
        # alter retval to include data

        if result:
            retval["columns"] = result[0]
            retval["data"]    = json.loads(result[1])
            retval["empty"]   = False
    except Exception as e:
        # TODO error message
        retval["error"] = True
        retval["error_msg"] = "An error occurred. Details: " + str(e)

    return jsonify(retval)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/dedup/find_matches', methods = ['POST'])
@require_login
@require_writeperm
def dedup_find_matches(dataset_id, tablename):
    """Callback to get a list of html tables. Each table represents a set of matches"""
    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    tv = dataset.getTableViewer(tablename)

    form = DedupForm(request.form)

    form.fillForm(tv.get_attributes())

    if not form.validate():
        flash(message="Invalid form", category="error")
        return redirect(url_for('dataset_pages.table', dataset_id=dataset_id, tablename=tablename))
    # ENDIF

    dd = dataset.getDeduplicator()

    ignore_list = form.ignore_list.data
    exactmatch_list = form.exactmatch_list.data

    table_list = dd.find_matches(dataset_id, tablename, exactmatch_list, ignore_list)

    return render_template('dataset_pages.deduplication.html', table_list=table_list,
                                                                attributes=tv.get_attributes())
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/clusterid/<int:clusterid>/keep_entries/<list:keep_entries>/dedup/deduplicate_cluster', methods=['POST'])
@require_login
@require_writeperm
def dedup_deduplicate_cluster(dataset_id, tablename, clusterid, keep_entries):
    """Callback to specifiy what entries need to be kept from a cluster"""
    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    dd = dataset.getDeduplicator()
    dd.deduplicate_cluster(dataset_id, tablename, clusterid, keep_entries)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/clusterid/<int:clusterid>/dedup/yes_to_all', methods=['POST'])
@require_login
@require_writeperm
def dedup_yes_to_all(dataset_id, tablename, clusterid):
    """Callback to automatically deduplicate starting from clusterid"""
    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    dd = dataset.getDeduplicator()
    dd.yes_to_all(dataset_id, tablename, clusterid)
# ENDFUNCTION

@dataset_pages.route('/dataset/<int:dataset_id>/table/<string:tablename>/dedup/cancel', methods=['POST'])
@require_login
@require_writeperm
def dedup_cancel(dataset_id, tablename):
    """Callback to cancel the current deduplication"""
    if not DatasetManager.existsID(dataset_id):
        abort(404)

    dataset = DatasetManager.getDataset(dataset_id)

    if tablename not in dataset.getTableNames():
        abort(404)

    dd = dataset.getDeduplicator()
    dd.clean_data(dataset_id, tablename)
# ENDFUNCTION