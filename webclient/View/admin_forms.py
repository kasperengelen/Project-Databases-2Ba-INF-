from flask_wtf import FlaskForm
from wtforms import HiddenField
from wtforms.validators import Email

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
    
    setid = None
    setname = None

    def fillForm(self, dataset):
        self.setid.data = dataset.setid
        self.setname.data = dataset.name
    # ENDMETHOD
# ENDCLASS

