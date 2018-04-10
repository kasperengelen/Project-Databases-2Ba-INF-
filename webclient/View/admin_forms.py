from flask_wtf import FlaskForm
from wtforms import HiddenField, StringField
from wtforms.validators import Email, InputRequired, Length

class DeleteUserForm(FlaskForm):
    """Class that represents a form that allows an admin
    to delete a user. It also contains information to identify the user."""
    
    userid = HiddenField('User ID')
    email = HiddenField('Email', [Email()])

    def fillForm(self, user):
        self.userid.data = str(user.userid)
        self.email.data = str(user.email)
    # ENDMETHOD
# ENDCLASS

class DeleteDatasetForm(FlaskForm):
    """Class that represents a form that allows an admin to delete
    a dataset."""
    
    setid = HiddenField('Set id')
    setname = HiddenField('Set name')

    def fillForm(self, dataset):
        self.setid.data = dataset.setid
        self.setname.data = dataset.name
    # ENDMETHOD
# ENDCLASS

class AdminUserEditForm(FlaskForm):
    firstname = StringField('Firstname', [InputRequired(message="Firstname is required."),
                                          Length(min=1, max=50, message="Firstname should contain between 1 and 50 characters.")])
    lastname = StringField('Lastname', [InputRequired(message="Lastname is required."),
                                        Length(min=1, max=50, message="Lastname should contain between 1 and 50 characters.")])
    email = StringField('Email', [InputRequired(message="Email address is required."),
                                  Email(message="The supplied email address is not of a valid format."),
                                  Length(min=6, max=70, message="Email address should contain between 1 and 50 characters.")])

    def fillFields(self, user_data):
        self.firstname.data = user_data.fname
        self.lastname.data = user_data.lname
        self.email.data = user_data.email
    # ENDMETHOD
# ENDCLASS

class ActivateDecactivateUser(FlaskForm):
    userid = HiddenField('User id')
    new_activation_status = HiddenField('New activation status')

    def fillForm(self, user_data):
        self.userid = user_data.userid
        self.new_activation_status = user_data.active
    # ENDMETHOD
# ENDCLASS

