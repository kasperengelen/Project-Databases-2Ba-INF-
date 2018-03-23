from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField
from wtforms.validators import Length, InputRequired, Email, EqualTo

class UserLoginForm(FlaskForm):
    """Class that represents a user login form. The form has a email field,
    this field is checked with a regex to be of the format of an email address.
    The supplied email needs to be between 6 and 70 characters long. The form also
    has a password field."""
    email = StringField('Email', [InputRequired(message="Email is required."),
                                  Email(message="The supplied email address is not of a valid format."),
                                  Length(min=6, max=70, message="Email ")])
    password = PasswordField('Password', [InputRequired("Password is required.")])
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

class UserEditForm(FlaskForm):
    """Class that represents a form used to update user information."""
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

    def fillFields(self, user_data):
      """Given a dict that contains information about
      the user, this will fill in the fields of this form."""

      self.firstname.data = user_data['firstname']
      self.lastname.data = user_data['lastname']
      self.email.data = user_data['email']
    # ENDMETHOD
# ENDCLASS