from flask_wtf import FlaskForm
from wtforms import HiddenField, StringField, IntegerField, TextAreaField
from wtforms.widgets import HiddenInput
from wtforms.validators import Email, InputRequired, Length

class DeleteUserForm(FlaskForm):
    """Form for admins to delete a user."""
    
    userid = IntegerField('User ID', widget = HiddenInput())

    def fillForm(self, user):
        self.userid.data = str(user.userid)
    # ENDMETHOD
# ENDCLASS

class DeleteDatasetForm(FlaskForm):
    """Form for admins to delete a dataset."""
    
    setid = IntegerField('Set id', widget = HiddenInput())

    def fillForm(self, dataset):
        self.setid.data = dataset.setid
    # ENDMETHOD
# ENDCLASS

class AdminUserEditForm(FlaskForm):
    """Form for admins to edit the information of a user."""

    firstname = StringField('Firstname', [InputRequired(message="Firstname is required."),
                                          Length(min=1, max=50, message="Firstname should contain between 1 and 50 characters.")])
    lastname = StringField('Lastname', [InputRequired(message="Lastname is required."),
                                        Length(min=1, max=50, message="Lastname should contain between 1 and 50 characters.")])
    email = StringField('Email', [InputRequired(message="Email address is required."),
                                  Email(message="The supplied email address is not of a valid format."),
                                  Length(min=6, max=70, message="Email address should contain between 1 and 50 characters.")])

    def fillForm(self, user_data):
        self.firstname.data = user_data.fname
        self.lastname.data = user_data.lname
        self.email.data = user_data.email
    # ENDMETHOD
# ENDCLASS

class DatasetEditForm(FlaskForm):
    """Form for admins to edit the information of a user."""
    setid = IntegerField('Set id', widget = HiddenInput())
    name = StringField("Dataset name", [InputRequired(message="Name is required."), Length(min=2, max=64, message="Name must be between 6 and 64 characters long.")])
    description = TextAreaField("Description", [Length(min=0, max=256, message="Description can contain max 256 characters.")])

    def fillForm(self, dataset):
        self.setid.data = dataset.setid
        self.name.data = dataset.name
        self.description.data = dataset.desc
    # ENDMETHOD
# ENDCLASS

class ActivateDeactivateUser(FlaskForm):
    userid = IntegerField('User id', widget = HiddenInput())
    new_activation_status = HiddenField('New activation status')

    def fillForm(self, user_data):
        self.userid.data = user_data.userid
        self.new_activation_status.data = not user_data.active
    # ENDMETHOD
# ENDCLASS

