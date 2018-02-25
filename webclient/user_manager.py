# file that contains all code for managing users: registration, editing, etc.
# SOURCE: wtforms documentation, stackoverflow and https://www.youtube.com/watch?v=zRwy8gtgJ1A&list=PLillGF-RfqbbbPz6GSEM9hLQObuQjNoj_&index=1

from flask import render_template, request
from wtforms import StringField, PasswordField, validators
from flask_wtf import FlaskForm
from wtforms.validators import Length, InputRequired, Email, EqualTo, DataRequired

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
    login_form = UserLoginForm(request_data.form)

    # validate the data supplied by the form.
    if request_data.method == 'POST' and login_form.validate():
        # the data is valid

        # check if the user exists in the database and if the
        # password checks out

        # set session to logged in with the specified user id
        pass

    return render_template('login.html', form = login_form)

def register_user(request_data):
    register_form = UserRegisterForm(request_data.form)

    # validate the data supplied by the form.
    if request_data.method == 'POST' and register_form.validate():
        # the data is valid

        # check if the user is already registered
        pass


    return render_template('register.html',  form = register_form)
