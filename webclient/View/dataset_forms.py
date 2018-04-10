from flask_wtf import FlaskForm
from flask_wtf.file import FileField as FWFileField, FileRequired as FWFileRequired
from wtforms import StringField, PasswordField, TextAreaField, SelectField, HiddenField, FileField, BooleanField
from wtforms.validators import Length, InputRequired, Email, EqualTo, Regexp
from utils import EnumCheck, FilenameCheck

class DatasetForm(FlaskForm):
    """Form that queries the user for the metadata of a dataset."""
    name = StringField("Dataset name", [InputRequired(message="Name is required."), Length(min=2, max=64, message="Name must be between 6 and 64 characters long.")])
    description = TextAreaField("Description", [Length(min=0, max=256, message="Description can contain max 256 characters.")])

    def fillForm(self, dataset_info):
        self.name.data = dataset_info['name']
        self.description.data = dataset_info['desc']
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

class DatasetListEntryForm(FlaskForm):
    """Form that contains data about a dataset
    that the user is part of."""

    setid = HiddenField('Set id')
    name = HiddenField('Set Name')
    desc = HiddenField('Description')

    def fillForm(self, dataset):
        self.setid.data = dataset.setid
        self.name.data = dataset.name
        self.desc.data = dataset.desc
    # ENDMETHOD
# ENDCLASS

class TableUploadForm(FlaskForm):
    """Form to upload tables."""
    data_file = FWFileField('File', [FWFileRequired("No file selected."), FilenameCheck("Invalid filename. Only alphanumeric characters and underscore allowed. Only csv, zip, sql and dump files allowed.", "[A-Za-z0-9][A-Za-z0-9_]+\\.(sql|csv|zip|dump)")])
    columnnames_included = BooleanField('Column names included in files? (CSV only)')
# ENDCLASS

class DownloadForm(FlaskForm):
    """Form to specify CSV properties for download."""

    delimiter = StringField('Delimiter', [InputRequired('Input is required.'), Length(min=1, max=1)])
    quotechar = StringField('Qoute character', [InputRequired('Input is required.'), Length(min=1, max=1)])
    nullrep = StringField('NULL representation', [InputRequired('Input is required.'), Length(min=1, max=10)])
# ENDCLASS

################################################################# TRANSFORMATION FORMS #################################################################

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

class DataTypeTransform(FlaskForm):
    """Form to change the datatype of a table attribute."""

    select_attr = SelectField('Attribute', choices=[])
    new_datatype = SelectField('New datatype', choices=[])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
        self.new_datatype.choices = ['VARCHAR(255)', 'CHAR(255)', 'INTEGER', 'FLOAT', 'DATE', 'TIME', 'TIMESTAMP']
    # ENDMETHOD
# ENDCLASS

class NormalizeZScore(FlaskForm):
    """Form to normalize an attribute by it's z-score."""

    select_attr = SelectField('Attribute', choices=[])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class OneHotEncoding(FlaskForm):
    """Form to perform the one-hot-encoding transformation."""
    select_attr = SelectField('Attribute', choices=[])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS