from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, TextAreaField, SelectField, HiddenField
from wtforms.validators import Length, InputRequired, Email, EqualTo
from utils import EnumCheck

class DatasetForm(FlaskForm):
    """Form that queries the user for the metadata of a dataset."""
    name = StringField("Dataset name", [InputRequired(message="Name is required."), Length(min=2, max=64, message="Name must be between 6 and 64 characters long.")])
    description = TextAreaField("Description", [Length(min=0, max=256, message="Description can contain max 256 characters.")])

    def fillForm(self, dataset_info):
        self.name.data = dataset_info['name']
        self.description.data = dataset_info['desc']
    # ENDMETHOD
# ENDCLASS

class FindReplaceForm(FlaskForm):
    """Form for the search and replace transformation."""
    select_attr = SelectField('Attribute', choices=[])
    search = StringField('Search', [InputRequired(message="Input is required.")])
    replacement = StringField('Replacement', [InputRequired(message="Input is required.")])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class DeleteAttrForm(FlaskForm):
    """Form for the delete attribute/column transformation."""
    select_attr = SelectField('Attribute', choices=[])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class AddUserForm(FlaskForm):
    """Form to give a user permission to alter the dataset."""
    email = StringField('Email', [
        InputRequired(message="Email is required."),
        Email(message="The supplied email address is not valid."),
        Length(min=6, max=70, message="Email address should contain between 1 and 50 characters.")
    ])
    permission_type = SelectField('Permission Type', choices=[('admin', 'Admin'),('write', 'Write'),('read', 'Read')])
# ENDCLASS

class RemoveUserForm(FlaskForm):
    """Form to revoke a user's permission to alter the dataset."""
    userid = HiddenField('UserID')
    email = HiddenField('Email', [Email()])
    permission_type = HiddenField('Permission Type', [EnumCheck(message="Invalid permission type.", choises=['read', 'write', 'admin'])])
# ENDCLASS