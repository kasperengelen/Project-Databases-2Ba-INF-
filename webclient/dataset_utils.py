# file that contains all code related datasets.

from flask import render_template, flash, request, url_for, session, redirect, abort
from wtforms import StringField, PasswordField, validators, TextAreaField
from flask_wtf import FlaskForm
from wtforms.validators import Length, InputRequired, Email, EqualTo, DataRequired
from db_wrapper import DBConnection
from passlib.hash import sha256_crypt

class CreateDatasetFrom(FlaskForm):
    name = StringField("Dataset name", [InputRequired(message="Name is required.")])

    description = TextAreaField("Description", [Length(min=0, max=256, message="Description can contain max 256 characters.")])

def view_dataset(request_data, set_id):
    """Given a specified ID, return a page that contains
    information about the dataset."""

    # retrieve information about the dataset
    with DBConnection as db_conn:
        db_conn.cursor().execute("SELECT * FROM datasets WHERE setid = %s", [set_id])
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
    form = CreateDatasetFrom(request_data.form)

    if request_data.method == 'POST': # there was submitted data
        if form.validate(): # submitted data is valid
            with DBConnection() as db_conn:
                db_conn.cursor().execute("INSERT INTO datasets(setname, description) VALUES (%s, %s) RETURNING setid;", [form.name.data, form.description.data])
                db_conn.commit()
                print(db_conn.fetchone())
            # ENDWITH

            # retrieve ID
            set_id = None


            flash(message="The dataset was created.", category="success")
            return redirect(url_for('/dataset/' + set_id + '/view/'))

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
        db_conn.cursor().execute("SELECT * FROM datasets WHERE setid IN (SELECT setid FROM set_permissions WHERE userid = %s);", [session['user_data']['user_id']])
        results = db_conn.cursor().fetchall()

        # iterate over datasets
        for dataset in results:
            setid        = dataset[0]
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

def manage_dataset(request_data):
    """Returns a page where the name and the permissions
    for a dataset can be edited."""

    return render_template('dataset_manage.html')
