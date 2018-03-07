# file that contains all code related datasets.

from flask import render_template, flash, request, url_for, session, redirect
from wtforms import StringField, PasswordField, validators
from flask_wtf import FlaskForm
from wtforms.validators import Length, InputRequired, Email, EqualTo, DataRequired
from db_wrapper import DBConnection
from passlib.hash import sha256_crypt

def view_dataset(request_data):
    """Given a specified ID, return a page that contains
    information about the dataset."""

    # retrieve information about the dataset

    return render_template('dataset_view.html', dataset = dataset)

def create_dataset(request_data):
    """Returns a page where a new dataset can be created.
    The current logged in user will be made administrator
    of the new dataset."""

    return render_template('dataset_create.html')

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
