# file that contains all code for managing users: registration, editing, etc.
# SOURCE: wtforms documentation, stackoverflow and https://www.youtube.com/watch?v=zRwy8gtgJ1A&list=PLillGF-RfqbbbPz6GSEM9hLQObuQjNoj_&index=1

from flask import render_template, request, url_for, session, redirect
from wtforms import StringField, PasswordField, validators
from flask_wtf import FlaskForm
from wtforms.validators import Length, InputRequired, Email, EqualTo, DataRequired
from db_wrapper import DBConnection
from passlib.hash import sha256_crypt


class UserRegisterForm(FlaskForm):
    """Class that represents a user registration form. The form has the following fields:
    Firstname and lastname fiels. These need to be between 1 and 50 characters long. Both are required.
    Email field, this needs to be between 6 and 70 characters long, is required and is checked with
    a regex to be a valid email.
    Password and Confirm password fields, these need to contain identical values, need to be between
    6 and 50 characters long, and are required.
    """
    firstname = StringField('Firstname', [InputRequired(message="Firstname is required."),
                                          Length(min=1, max=50, message="Firstname should contain between 1 and 50 characters.")])
    lastname = StringField('Lastname', [InputRequired(message="Lastname is required."),
                                        Length(min=1, max=50, message="Lastname should contain between 1 and 50 characters.")])
    email = StringField('Email', [InputRequired(message="Email address is required."),
                                  Email(message="The supplied email address is not of a valid format."),
                                  Length(min=6, max=70, message="Email address should contain between 1 and 50 characters.")])
    password = PasswordField('Password', [InputRequired("Password is required."),
                                          EqualTo('passwordconfirm', message="Passwords do not match."),
                                          Length(min=6, max=50, message="Password needs to be between 6 and 50 characters long.")])
    passwordconfirm = PasswordField('Confirm Password')


class UserLoginForm(FlaskForm):
    """Class that represents a user login form. The form has a email field,
    this field is checked with a regex to be of the format of an email address.
    The supplied email needs to be between 6 and 70 characters long. The form also
    has a password field."""
    email = StringField('Email', [InputRequired(message="Email is required."),
                                  Email(message="The supplied email address is not of a valid format."),
                                  Length(min=6, max=70, message="Email ")])
    password = PasswordField('Password', [DataRequired()])


def login_user(request_data):
    """Given the specified request data received from a POST or GET request, this will try to login
    a user with the data contained in the request. If no data is present in the request, this will return
    the login page."""
    login_form = UserLoginForm(request_data.form)

    # validate the data supplied by the form.
    if request_data.method == 'POST' and login_form.validate():
        # the data is valid

        # check if the user exists in the database and if the
        # password checks out
        with DBConnection() as db_connection:
            db_connection.cursor().execute("SELECT * FROM user_accounts WHERE email = %s;", [login_form.email.data])
            result = db_connection.cursor().fetchone() # we fetch the first and max only tuple

            if result is not None:
                # user exists
                user_id = result[0] # the user identifier
                real_password_hash = result[4] # the real password

                if sha256_crypt.verify(login_form.password.data, real_password_hash):
                    # password checks out
                    session["user_id"] = user_id
                    session["logged_in"] = True
                    
                    flash("You are now logged in.")

                    return redirect(url_for("/"))

                else:
                    # password does not match
                    flash(message="Specified password is invalid.", category="error")
                    return render_template('login.html', form = login_form)

            else:
                # user does not exist
                flash(message="Specified e-mail address does not belong to an existing user.", category="error")
                return render_template('login.html', form = login_form)

    return render_template('login.html', form = login_form)

def register_user(request_data):
    """Given the specified request data received from a POST or GET request, this will try to register
    a user with the data contained in the request. If no data is present in the request, this will return
    the register page."""
    register_form = UserRegisterForm(request_data.form)

    # validate the data supplied by the form.
    if request_data.method == 'POST' and register_form.validate():
        # the data is valid

        # check if there is already a user with the specified email
        with DBConnection() as db_connection:
            db_connection.cursor().execute("SELECT * FROM user_accounts WHERE email = %s;", [register_form.email.data])
            result = db_connection.cursor().fetchone() # we fetch the first

            # there are users with the specified 
            if result is not None:
                flash(message="Error, specified e-mail address already in use.", category="error")
                return render_template('register.html',  form = register_form)

        # retrieve new values from form
        fname = register_form.firstname.data
        lname = register_form.lastname.data
        email = register_form.email.data
        password = sha256_crypt.hash(register_form.password.data)

        with DBConnection() as db_connection:
            db_connection.cursor().execute("INSERT INTO user_accounts(fname, lname, email, passwd) VALUES (%s, %s, %s, %s);", [fname, lname, email, password])
            db_connection.commit()

        flash(message="You are now registered as a user.", category="message")

        return redirect(url_for("login"))

    return render_template('register.html',  form = register_form)
