# file that contains all code for users:
#   Login
#   Registration
#   Edit information
#   View information
# SOURCE: wtforms documentation, stackoverflow and https://www.youtube.com/watch?v=zRwy8gtgJ1A&list=PLillGF-RfqbbbPz6GSEM9hLQObuQjNoj_&index=1

from flask import render_template, flash, request, url_for, session, redirect
from wtforms import StringField, PasswordField, validators
from flask_wtf import FlaskForm
from wtforms.validators import Length, InputRequired, Email, EqualTo, DataRequired
from db_wrapper import DBConnection
from passlib.hash import sha256_crypt
import utils


class UserInformation:
    """Class that contains information about a user."""
    def __init__(self, user_id, email, firstname, lastname, register_date):
        """Constructor based on individual data fields. Note: Register_data contains the timestamp retrieved from SQL"""
        self.user_id = user_id
        self.email = email
        self.firstname = firstname
        self.lastname = lastname

        self.register_date = {
            "Y": None,
            "M": None,
            "D": None,

            "hr": None,
            "min": None,
            "sec": None
        }
    # ENDMETHOD

    def toJson(self):
        return {
            "user_id": self.user_id,
            "email": self.email,
            "firstname": self.firstname,
            "lastname": self.lastname,
            "register_date": {
                "Y": self.register_date['Y'],
                "M": self.register_date['M'],
                "D": self.register_date['D'],

                "hr": self.register_date['hr'],
                "min": self.register_date['min'],
                "sec": self.register_date['sec']
            }
        }

    def __init__(self, email, password_candidate):
        """Constructor based on an email address and a password. The constructor will retrieve the other data from the DB"""
        with DBConnection() as db_connection:
            db_connection.cursor().execute("SELECT * FROM user_accounts WHERE email = %s;", [email])
            result = db_connection.cursor().fetchone() # we fetch the first and max only tuple

            if result is None: # if the user witht the specified email and password doesn't exist
                raise RuntimeError("No user with the specified email exists.")

            correct_password_hash = result[4]

            if not sha256_crypt.verify(password_candidate, correct_password_hash):
                raise RuntimeError("Invalid password.")

            self.user_id = int(result[0])
            self.firstname = str(result[1])
            self.lastname = str(result[2])
            self.email = str(result[3])
            self.register_date = utils.sql_time_to_dict(str(result[5]))
    # ENDMETHOD
# ENDCLASS


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
# ENDCLASS

class UserLoginForm(FlaskForm):
    """Class that represents a user login form. The form has a email field,
    this field is checked with a regex to be of the format of an email address.
    The supplied email needs to be between 6 and 70 characters long. The form also
    has a password field."""
    email = StringField('Email', [InputRequired(message="Email is required."),
                                  Email(message="The supplied email address is not of a valid format."),
                                  Length(min=6, max=70, message="Email ")])
    password = PasswordField('Password', [DataRequired()])
# ENDCLASS

def login_user(request_data):
    """Given the specified request data received from a POST or GET request, this will try to login
    a user with the data contained in the request. If no data is present in the request, this will return
    the login page."""
    login_form = UserLoginForm(request_data.form)

    # validate the data supplied by the form.
    if request_data.method == 'POST': # There was POST data
        if login_form.validate(): # The data supplied by the form was valid

            # check if the user exists in the database and if the
            # password checks out
            with DBConnection() as db_connection:
                try:
                    user_data = UserInformation(login_form.email.data, login_form.password.data)
                    session['logged_in'] = True
                    session['user_data'] = user_data

                    return redirect(url_for("index"))
                    
                except RuntimeError as err:
                    flash(message="Specified email and password combination is invalid.", category="error")
                    return render_template('login.html', form = login_form)
                # ENDTRY
            # ENDWITH
        else: # the form data was invalid
            return render_template('login.html', form = login_form)
        # ENDIF
    else: # The page was opened without form data
        return render_template('login.html', form = login_form)
    # ENDIF
# END FUNCTION

def register_user(request_data):
    """Given the specified request data received from a POST or GET request, this will try to register
    a user with the data contained in the request. If no data is present in the request, this will return
    the register page."""
    register_form = UserRegisterForm(request_data.form)

    # validate the data supplied by the form.
    if request_data.method == 'POST': # There was POST data
        if register_form.validate(): # The data supplied by the form was valid
            # Validated Post data -> try to register user

            # CHECK if there is already a user with the specified email
            with DBConnection() as db_connection:
                db_connection.cursor().execute("SELECT * FROM user_accounts WHERE email = %s;", [register_form.email.data])
                result = db_connection.cursor().fetchone() # we fetch the first

                # there are users with the specified email --> error
                if result is not None:
                    flash(message="Specified e-mail address already in use.", category="error")
                    return render_template('register.html',  form = register_form)
                #ENDIF
            #ENDWITH

            # RETRIEVE data from form
            fname = register_form.firstname.data
            lname = register_form.lastname.data
            email = register_form.email.data
            password = sha256_crypt.hash(register_form.password.data)

            # REGISTER user data into database
            with DBConnection() as db_connection:
                db_connection.cursor().execute("INSERT INTO user_accounts(fname, lname, email, passwd) VALUES (%s, %s, %s, %s);", [fname, lname, email, password])
                db_connection.commit()
            #ENDWITH

            # notify user that the registration is complete
            flash(message="You are now registered as a user.", category="success")

            return redirect(url_for("login"))
        else: # The data supplied by the form was not valid.
            return render_template('register.html',  form = register_form)
        #ENDIF
    else: # The page was opened without form data
        return render_template('register.html',  form = register_form)
# END FUNCTION



