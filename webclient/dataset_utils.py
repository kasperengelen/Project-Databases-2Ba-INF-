# file that contains all code related datasets.

from flask import render_template, flash, request, url_for, session, redirect, abort
from wtforms import StringField, PasswordField, validators, TextAreaField
from flask_wtf import FlaskForm
from wtforms.validators import Length, InputRequired, Email, EqualTo, DataRequired
from db_wrapper import DBConnection
from passlib.hash import sha256_crypt

class DatasetForm(FlaskForm):
    name = StringField("Dataset name", [InputRequired(message="Name is required.")])

    description = TextAreaField("Description", [Length(min=0, max=256, message="Description can contain max 256 characters.")])

def view_dataset(request_data, set_id):
    """Given a specified ID, return a page that contains
    information about the dataset."""

    # retrieve information about the dataset
    with DBConnection() as db_conn:
        db_conn.cursor().execute("SELECT * FROM SYSTEM.datasets WHERE setid = %s", [set_id])
        result = db_conn.cursor().fetchone()

        if result is not None:
            dataset = {
                "id":          result[0],
                "displayName": result[1],
                "description": result[2] 
            }

            return render_template('dataset_view.html', dataset = dataset)
        else:
            abort(404)
        # ENDIF
    # ENDWITH
# ENDFUNCTION

def create_dataset(request_data):
    """Returns a page where a new dataset can be created.
    The current logged in user will be made administrator
    of the new dataset."""

    # CREATE FORM
    form = DatasetForm(request_data.form)

    if request_data.method == 'POST': # there was submitted data
        if form.validate(): # submitted data is valid
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

        else: # there are invalid fields
            return render_template('dataset_create.html', form=form)
        # ENDIF
    else: # no submitted data
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

    if request_data.method == 'POST': # there was formd data
        if form.validate(): # form data was valid
            pass
        else: # form data was invalid
            return render_template('dataset_manage.html', form = form)
    else: # no form data
        # fill form with current data

        return render_template('dataset_manage.html', form = form)
    # ENDIF
# ENDFUNCTION

def edit_perms_dataset(request_data, set_id):
    """Returns a page where the name and the permissions for
    a dataset can be edited."""
    
    # 'user_id', 'firstname', 'lastname', 'email', 'permission_type'

    ## retrieve admins ##
    with DBConnection() as db_conn:
        # join permissions and users and filter only the permissions that are related to the specified data set.
        db_conn.cursor().execute("SELECT UserAccs.userid, UserAccs.fname, UserAccs.lname, UserAccs.email, Perms.permission_type "
                                        "FROM SYSTEM.user_accounts AS UserAccs "
                                        "INNER JOIN SYSTEM.set_permissions AS Perms "
                                        "ON UserAccs.userid = Perms.userid WHERE setid=%s AND permission_type='admin'; ", [set_id])

        results = db_conn.cursor().fetchall()

        admin_list = []

        for result in results:
            admin_list.append({
                "user_id":         result[0],
                "firstname":       result[1],
                "lastname":        result[2],
                "email":           result[3],
                "permission_type": result[4]
            })
        # ENDFOR
    # ENDWITH

    ## retrieve write perms ##
    with DBConnection() as db_conn:
        # join permissions and users and filter only the permissions that are related to the specified data set.
        db_conn.cursor().execute("SELECT UserAccs.userid, UserAccs.fname, UserAccs.lname, UserAccs.email, Perms.setid, Perms.permission_type "
                                        "FROM SYSTEM.user_accounts AS UserAccs "
                                        "INNER JOIN SYSTEM.set_permissions AS Perms "
                                        "ON UserAccs.userid = Perms.userid WHERE setid=%s AND permission_type='write'; ", [set_id])

        results = db_conn.cursor().fetchall()

        write_list = []

        for result in results:
            write_list.append({
                "user_id":         result[0],
                "firstname":       result[1],
                "lastname":        result[2],
                "email":           result[3],
                "permission_type": result[4]
            })
        # ENDFOR
    # ENDWITH

    ## retrieve read perms ##
    with DBConnection() as db_conn:
        # join permissions and users and filter only the permissions that are related to the specified data set.
        db_conn.cursor().execute("SELECT UserAccs.userid, UserAccs.fname, UserAccs.lname, UserAccs.email, Perms.setid, Perms.permission_type "
                                        "FROM SYSTEM.user_accounts AS UserAccs "
                                        "INNER JOIN SYSTEM.set_permissions AS Perms "
                                        "ON UserAccs.userid = Perms.userid WHERE setid=%s AND permission_type='read'; ", [set_id])

        results = db_conn.cursor().fetchall()

        read_list = []

        for result in results:
            read_list.append({
                "user_id":         result[0],
                "firstname":       result[1],
                "lastname":        result[2],
                "email":           result[3],
                "permission_type": result[4]
            })
        # ENDFOR
    # ENDWITH

    return render_template('dataset_permissions.html', admin_list = admin_list, read_list = read_list, write_list = write_list)
# ENDFUNCTION

def add_user_dataset(request_data, set_id):
    """Callback that adds the user contained in
    the POST data from the specified dataset."""

    if not request_data.form.permission_type in ['read', 'write', 'admin']:
        flash("Invalid permission type: '" + request_data.form.permission_type + "'")
        return redirect(url_for('edit_perms_dataset'))

    with DBConnection() as db_conn:
        # retrieve user id from email
        user_data = UserInformation.from_email(request_data.form.email)

        ## check if user exists ##
        db_conn.cursor().execute("SELECT * FROM SYSTEM.user_accounts WHERE email=%s", [request_data.form.email])
        result = db_conn.cursor().fetchone()
        if result is None:
            flash(message="User with specified email address does not exist.", category="error")
            return redirect(url_for('edit_perms_dataset'))
        # ENDIF

        ## check if permission already exists for user ##
        user_data = UserInformation.from_email(request_data.form.email)
        db_conn.cursor().execute("SELECT * FROM SYSTEM.set_permissions WHERE userid=%s AND setid=%s", [user_data.user_id, set_id])
        result = db_conn.cursor().fetchone()

        if result is not None:
            flash(message="Specified user already had permissions for specified data set.", category="error")
            return redirect(url_for('edit_perms_dataset'))
        # ENDIF

        ## add permission ##
        db_conn.cursor().execute("INSERT INTO SYSTEM.set_permissions(userid, setid, permission_type) VALUES (%s, %s, %s);", [user_data.user_id, set_id, request_data.form.permission_type])
        result = db_conn.commit()

    return redirect(url_for('edit_perms_dataset'))

def remove_user_dataset(request_data, set_id):
    """Callback that removes the user contained in
    the POST data from the specified dataset."""

    if not request_data.form.permission_type in ['read', 'write', 'admin']:
        flash("Invalid permission type: '" + request_data.form.permission_type + "'")
        return redirect(url_for('edit_perms_dataset'))
    
    with DBConnection() as db_conn:
        # retrieve user id from email
        user_data = UserInformation.from_email(request_data.form.email)
        
        ## check that user does not edit own permissions ##
        if user_data.user_id == session['user_data']['user_id']:
            flash(message="User cannot remove itself from data set.")

        ## check if user exists ##
        db_conn.cursor().execute("SELECT * FROM SYSTEM.user_accounts WHERE email=%s", [request_data.form.email])
        result = db_conn.cursor().fetchone()
        if result is None:
            flash(message="User with specified email address does not exist.", category="error")
            return redirect(url_for('edit_perms_dataset'))
        # ENDIF

        ## check if permission already exists for user ##
        user_data = UserInformation.from_email(request_data.form.email)
        db_conn.cursor().execute("SELECT * FROM SYSTEM.set_permissions WHERE userid=%s AND setid=%s AND permission_type=%s", [user_data.user_id, set_id, request_data.form.permission_type])
        result = db_conn.cursor().fetchone()

        if result is None:
            flash(message="Specified user does not have the specified permissions for the specified data set.", category="error")
            return redirect(url_for('edit_perms_dataset'))
        # ENDIF

        ## remove permission ##
        db_conn.cursor().execute("DELETE FROM SYSTEM.set_permissions WHERE userid=%s AND setid=%s", [user_data.user_id, request_data.form.permission_type])

    return redirect(url_for('edit_perms_dataset'))

