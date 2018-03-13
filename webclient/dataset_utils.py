# file that contains all code related datasets.

from flask import render_template, flash, request, url_for, session, redirect, abort
from wtforms import StringField, PasswordField, validators, TextAreaField, SelectField, HiddenField
from flask_wtf import FlaskForm
from wtforms.validators import Length, InputRequired, Email, EqualTo, DataRequired
from db_wrapper import DBConnection
from passlib.hash import sha256_crypt
from utils import EnumCheck
from utils import Logger
from DataViewer import DataViewer
from DataTransformer import DataTransformer

class DatasetForm(FlaskForm):
    name = StringField("Dataset name", [InputRequired(message="Name is required."), Length(min=2, max=64, message="Name must be between 6 and 64 characters long.")])

    description = TextAreaField("Description", [Length(min=0, max=256, message="Description can contain max 256 characters.")])

def view_dataset_home(request_data, set_id):
    """Given a specified ID, return a page that contains
    information about the dataset. This page does not specify
    information contained in the tables of the dataset."""

    ## retrieve basic information
    with DBConnection() as db_conn:
        db_conn.cursor().execute("SELECT * FROM SYSTEM.datasets WHERE setid = %s", [set_id])
        result = db_conn.cursor().fetchone()

        dataset_info = {
            "id":          result[0],
            "displayName": result[1],
            "description": result[2] 
        }
    # ENDWITH

    dv = DataViewer()

    ## retrieve list of tables
    table_list = dv.get_tablenames(set_id)

    return render_template('dataset_view_home.html', 
                                dataset_info = dataset_info, 
                                table_list = table_list)
    # ENDWITH
# ENDFUNCTION

def view_dataset_table(request_data, set_id, tablename, page_nr):
    """ Given the id of a dataset, the identifier of a table and a page_nr
    of that dataset this returns the data contained in that dataset."""



    dv = DataViewer()

    if not dv.is_in_range(set_id, tablename, page_nr, 50):
        flash(message="Page out of range.", category="error")
        return redirect(url_for('view_dataset_home', dataset_id=set_id))

    ## retrieve basic information
    with DBConnection() as db_conn:
        db_conn.cursor().execute("SELECT * FROM SYSTEM.datasets WHERE setid = %s", [set_id])
        result = db_conn.cursor().fetchone()

        dataset_info = {
            "id":          result[0],
            "displayName": result[1],
            "description": result[2] 
        }
    # ENDWITH

    page_indices = dv.get_page_indices(set_id, tablename, 50, page_nr)

    table_data = dv.render_table(set_id, tablename, page_nr, 50)

    return render_template('dataset_view_table.html', 
                                dataset_info = dataset_info, 
                                page_indices = page_indices, 
                                table_data   = table_data,
                                table_name = tablename,
                                current_page = page_nr)
# ENDFUNCTION

########################################################################################## TIJDELIJK ##########################################################################################

class FindReplaceForm(FlaskForm):
    search = StringField('Search', [InputRequired(message="Input is required.")])
    replacement = StringField('Replacement', [InputRequired(message="Input is required.")])

class DeleteAttrForm(FlaskForm):
    select_attr = SelectField('Attribute', choices=[])

def transform_dataset_table(request_data, set_id, tablename, page_nr):
    """Given the id of a dataset, the name of a table and a page nr,
    this returns a page on which data can be transformed."""
    
    ## Find Replace ##
    # create form
    findrepl_form = FindReplaceForm()

    ## Delete Attribute ##
    deleteattr_form = DeleteAttrForm()

    # set attribute values
    dv = DataViewer()

    # set possible attribues
    attrs = dv.get_attributes(set_id, tablename)
    deleteattr_form.select_attr.choices = [(attrname, attrname) for attrname in attrs]

    ## retrieve information about table ##
    if not dv.is_in_range(set_id, tablename, page_nr, 50):
        flash(message="Page out of range.", category="error")
        return redirect(url_for('view_dataset_home', dataset_id=set_id))

    ## retrieve basic information
    with DBConnection() as db_conn:
        db_conn.cursor().execute("SELECT * FROM SYSTEM.datasets WHERE setid = %s", [set_id])
        result = db_conn.cursor().fetchone()

        dataset_info = {
            "id":          result[0],
            "displayName": result[1],
            "description": result[2] 
        }
    # ENDWITH

    page_indices = dv.get_page_indices(set_id, tablename, 50, page_nr)

    table_data = dv.render_table(set_id, tablename, page_nr, 50)

    return render_template('dataset_transform.html', 
                                                dataset_info = dataset_info,
                                                page_indices = page_indices,
                                                table_data = table_data,
                                                table_name = tablename,
                                                delete_form = deleteattr_form,
                                                findrepl_form = findrepl_form)
# ENDFUNCTION

def transform_delete_attr(request_data, set_id, tablename):
    
    dv = DataViewer()

    form = DeleteAttrForm(request_data.form)
    
    # set possible attribues
    attrs = dv.get_attributes(set_id, tablename)
    form.select_attr.choices = [(attrname, attrname) for attrname in attrs]

    if not form.validate():
        Logger.log("Invalid form")
        return redirect(url_for('transform_dataset_table', dataset_id=set_id, tablename=tablename, page_nr=1))

    

    if not form.select_attr.data in dv.get_attributes(set_id, tablename):
        Logger.log("Invalid attribute name.")
        return redirect(url_for('transform_dataset_table', dataset_id=set_id, tablename=tablename, page_nr=1))

    dt = DataTransformer(session['user_data']['user_id'])

    dt.delete_attribute(set_id, tablename, form.select_attr.data)
    flash(message="Attribute deleted.", category="success")

    return redirect(url_for('transform_dataset_table', dataset_id=set_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

def transform_findreplace(request_data, set_id, tablename):


    return redirect(url_for('transform_dataset_table', dataset_id=set_id, tablename=tablename, page_nr=1))
# ENDFUNCTION

########################################################################################## TIJDELIJK ##########################################################################################

def create_dataset(request_data):
    """Returns a page where a new dataset can be created.
    The current logged in user will be made administrator
    of the new dataset."""

    # CREATE FORM
    form = DatasetForm(request_data.form)

    if request_data.method == 'POST' and form.validate(): # there was valid submitted data
        with DBConnection() as db_conn:
            # INSERT DATA INTO DB
            db_conn.cursor().execute("INSERT INTO SYSTEM.datasets(setname, description) VALUES (%s, %s) RETURNING setid;", [form.name.data, form.description.data])
            db_conn.commit()

            # retrieve ID
            set_id = db_conn.cursor().fetchone()[0]

            # SET PERMISSIONS
            db_conn.cursor().execute("INSERT INTO SYSTEM.set_permissions(userid, setid, permission_type) VALUES (%s, %s, 'admin');", [session['user_data']['user_id'], set_id])
            db_conn.commit()
        # ENDWITH

        flash(message="The dataset was created.", category="success")
        return redirect(url_for('view_dataset', dataset_id=set_id))
    else: # no submitted data or invalid data
        return render_template('dataset_create.html', form=form)
    # ENDIF
# ENDFUNCTION

def list_dataset(request_data):
    """Returns a page that lists all datasets associated
    with the current user."""

    dataset_list = []

    with DBConnection() as db_conn:
        db_conn.cursor().execute("SELECT * FROM SYSTEM.datasets WHERE setid IN (SELECT setid FROM SYSTEM.set_permissions WHERE userid = %s);", [session['user_data']['user_id']])
        results = db_conn.cursor().fetchall()

        # iterate over datasets
        for dataset in results:
            setid        = dataset[0]
            print(setid)
            display_name = dataset[1]
            description  = dataset[2]

            dataset_list.append({
                "id":          setid,
                "displayName": display_name,
                "description": description
            })
        # ENDIF
    # ENDWITH

    return render_template('dataset_list.html', setlist = dataset_list)
# ENDFUNCTION

def manage_dataset(request_data, set_id):
    """Returns a page where the name and the permissions
    for a dataset can be edited."""

    # CREATE FORM
    form = DatasetForm(request_data.form)

    if request_data.method == 'POST' and form.validate: # there was form data
        if form.validate(): # form data was valid
            with DBConnection() as db_conn:
                db_conn.cursor().execute("UPDATE SYSTEM.datasets SET setname = %s, description = %s WHERE setid = %s;", 
                                    [form.name.data, form.description.data, set_id])
                db_conn.commit()

                flash("Information is updated.")
            # ENDWITH
        # ENDIF
    else: # no form data
        # retrieve data from DB
        with DBConnection() as db_conn:
            # retrieve data
            db_conn.cursor().execute("SELECT * FROM SYSTEM.datasets WHERE setid = %s;", [set_id])
            result = db_conn.cursor().fetchone()

            # fill form
            form.name.data = result[1]
            form.description.data = result[2]

        # ENDWITH
    # ENDIF
    return render_template('dataset_manage.html', form = form)
# ENDFUNCTION

class AddUserForm(FlaskForm):
    """Form to give a user permission to alter the dataset."""
    email = StringField('Email', [
        InputRequired(message="Email is required."),
        Email(message="The supplied email address is not valid."),
        Length(min=6, max=70, message="Email address should contain between 1 and 50 characters.")
    ])

    permission_type = SelectField('Permission Type', choices=[('admin', 'Admin'),('write', 'Write'),('read', 'Read')])
# ENDCLASS

class RemoveUserForm(FlaskForm):
    """Form to revoke a user's permission to alter the dataset."""
    userid = HiddenField('UserID')
    email = HiddenField('Email', [Email()])
    permission_type = HiddenField('Permission Type', [EnumCheck(message="Invalid permission type.", choises=['read', 'write', 'admin'])])
# ENDCLASS

def edit_perms_dataset(request_data, set_id):
    """Returns a page where the name and the permissions for
    a dataset can be edited."""

    # create empty form.
    form_add_user = AddUserForm()

    ### CREATE DELETE LIST ###
    ## retrieve admins ##
    with DBConnection() as db_conn:
        # join permissions and users and filter only the permissions that are related to the specified data set.
        db_conn.cursor().execute("SELECT UserAccs.userid, UserAccs.fname, UserAccs.lname, UserAccs.email, Perms.permission_type "
                                        "FROM SYSTEM.user_accounts AS UserAccs "
                                        "INNER JOIN SYSTEM.set_permissions AS Perms "
                                        "ON UserAccs.userid = Perms.userid WHERE setid=%s AND permission_type='admin' AND UserAccs.userid <> %s; ", [set_id, session['user_data']['user_id']])

        results = db_conn.cursor().fetchall()

        admin_list = []
        
        for result in results:

            # create form
            delete_admin_form = RemoveUserForm()

            # fill with data
            delete_admin_form.userid.data = result[0]
            delete_admin_form.email.data = result[3]
            delete_admin_form.permission_type.data = result[4]

            admin_list.append(delete_admin_form)
        # ENDFOR
    # ENDWITH

    ## retrieve write perms ##
    with DBConnection() as db_conn:
        # join permissions and users and filter only the permissions that are related to the specified data set.
        db_conn.cursor().execute("SELECT UserAccs.userid, UserAccs.fname, UserAccs.lname, UserAccs.email, Perms.permission_type "
                                        "FROM SYSTEM.user_accounts AS UserAccs "
                                        "INNER JOIN SYSTEM.set_permissions AS Perms "
                                        "ON UserAccs.userid = Perms.userid WHERE setid=%s AND permission_type='write' AND UserAccs.userid <> %s; ", [set_id, session['user_data']['user_id']])

        results = db_conn.cursor().fetchall()

        write_list = []

        for result in results:
            # create form
            delete_write_form = RemoveUserForm()

            # fill with data
            delete_write_form.userid.data = result[0]
            delete_write_form.email.data = result[3]
            delete_write_form.permission_type.data = result[4]

            write_list.append(delete_write_form)
        # ENDFOR
    # ENDWITH

    ## retrieve read perms ##
    with DBConnection() as db_conn:
        # join permissions and users and filter only the permissions that are related to the specified data set.
        db_conn.cursor().execute("SELECT UserAccs.userid, UserAccs.fname, UserAccs.lname, UserAccs.email, Perms.permission_type "
                                        "FROM SYSTEM.user_accounts AS UserAccs "
                                        "INNER JOIN SYSTEM.set_permissions AS Perms "
                                        "ON UserAccs.userid = Perms.userid WHERE setid=%s AND permission_type='read' AND UserAccs.userid <> %s; ", [set_id, session['user_data']['user_id']])

        results = db_conn.cursor().fetchall()

        read_list = []

        for result in results:
            # create form
            delete_read_form = RemoveUserForm()

            # fill with data
            delete_read_form.userid.data = result[0]
            delete_read_form.email.data = result[3]
            delete_read_form.permission_type.data = result[4]

            read_list.append(delete_read_form)
        # ENDFOR
    # ENDWITH

    return render_template('dataset_permissions.html', setid = set_id, form = form_add_user, admin_list = admin_list, read_list = read_list, write_list = write_list)
# ENDFUNCTION

def add_user_dataset(request_data, set_id):
    """Callback that adds the user contained in
    the POST data from the specified dataset."""

    # read form
    form = AddUserForm(request_data.form)

    # validate form
    if not form.validate():
        flash(message="Invalid form, please check email and permission type.", category="error")
        return redirect(url_for('edit_perms_dataset', dataset_id=set_id))

    with DBConnection() as db_conn:
        ## check if user exists ##
        db_conn.cursor().execute("SELECT * FROM SYSTEM.user_accounts WHERE email=%s", [form.email.data])
        result = db_conn.cursor().fetchone()
        if result is None:
            flash(message="User with specified email address does not exist.", category="error")
            return redirect(url_for('edit_perms_dataset', dataset_id=set_id))
        # ENDIF

        ## check if permission already exists for user ##
        db_conn.cursor().execute("SELECT * FROM SYSTEM.set_permissions WHERE userid=(SELECT userid FROM SYSTEM.user_accounts WHERE email=%s) AND setid=%s", [form.email.data, set_id])
        result = db_conn.cursor().fetchone()

        if result is not None:
            flash(message="Specified user already has permissions for specified data set.", category="error")
            return redirect(url_for('edit_perms_dataset', dataset_id=set_id))
        # ENDIF

        ## add permission ##
        db_conn.cursor().execute("INSERT INTO SYSTEM.set_permissions(userid, setid, permission_type) VALUES ((SELECT userid FROM SYSTEM.user_accounts WHERE email=%s), %s, %s);", [form.email.data, set_id, form.permission_type.data])
        result = db_conn.commit()
    # ENDWITH

    return redirect(url_for('edit_perms_dataset', dataset_id=set_id))
# ENDFUNCTION

def remove_user_dataset(request_data, set_id):
    """Callback that removes the user contained in
    the POST data from the specified dataset."""

    # create form
    form = RemoveUserForm(request_data.form)

    # validate
    if not form.validate():
        Logger.log("'remove_user_dataset(request_data, set_id)': Form not valid.")
        return redirect(url_for('edit_perms_dataset', dataset_id=set_id))
    # ENDIF

    with DBConnection() as db_conn:
        ## check that user does not edit own permissions ##

        if form.email.data == session['user_data']['email']:
            print(form.email.data)
            print(session['user_data']['email'])
            flash(message="User cannot remove itself from dataset.", category="error")
            Logger.log("'remove_user_dataset(request_data, set_id)': Cannot remove self.")
            return redirect(url_for('edit_perms_dataset', dataset_id=set_id))
        # ENDIF

        ## check if user exists ##
        db_conn.cursor().execute("SELECT * FROM SYSTEM.user_accounts WHERE email=%s", [form.email.data])
        result = db_conn.cursor().fetchone()
        if result is None:
            Logger.log("'remove_user_dataset(request_data, set_id)': User with email does not exist.")
            return redirect(url_for('edit_perms_dataset', dataset_id=set_id))
        # ENDIF

        ## check if permission already exists for user ##
        db_conn.cursor().execute("SELECT * FROM SYSTEM.set_permissions WHERE userid=(SELECT userid FROM SYSTEM.user_accounts WHERE email=%s) AND setid=%s AND permission_type=%s", [form.email.data, set_id, form.permission_type.data])
        result = db_conn.cursor().fetchone()

        if result is None:
            Logger.log("'remove_user_dataset(request_data, set_id)': No permissions to remove.")
            return redirect(url_for('edit_perms_dataset', dataset_id=set_id))
        # ENDIF

        ## remove permission ##
        db_conn.cursor().execute("DELETE FROM SYSTEM.set_permissions WHERE "
                                    "userid=(SELECT userid FROM SYSTEM.user_accounts WHERE email=%s) "
                                    "AND setid=%s "
                                    "AND permission_type=%s;", 
                                                [form.email.data, set_id, form.permission_type.data])
        db_conn.commit()
    # ENDWITH

    return redirect(url_for('edit_perms_dataset', dataset_id=set_id))
# ENDFUNCTION

