from flask_wtf import FlaskForm
from flask_wtf.file import FileField as FWFileField, FileRequired as FWFileRequired
from wtforms import StringField, PasswordField, TextAreaField, SelectField, HiddenField, FileField, BooleanField, IntegerField
from wtforms.widgets import HiddenInput
from wtforms.validators import Length, InputRequired, Email, EqualTo, Regexp
from View.form_utils import EnumCheck, FilenameCheck

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
    userid = IntegerField('UserID', widget = HiddenInput())

    def fillForm(self, user):
        self.userid.data = user.userid
    # ENDMETHOD
# ENDCLASS

class LeaveForm(FlaskForm):
    """Form to leave a dataset."""

    setid = IntegerField('Set id', widget = HiddenInput())

    def fillForm(self, dataset):
        self.setid.data = dataset.setid
    # ENDMETHOD
# ENDCLASS

class TableUploadForm(FlaskForm):
    """Form to upload tables."""
    data_file = FWFileField('File', [FWFileRequired("No file selected."), FilenameCheck("Invalid filename. Only alphanumeric characters and underscore allowed. Only csv, zip, sql and dump files allowed.", "[A-Za-z0-9][A-Za-z0-9_]+\\.(sql|csv|zip|dump)")])
    columnnames_included = BooleanField('Column names included in files? (CSV only)', default = True)
# ENDCLASS

class DownloadDatasetCSVForm(FlaskForm):
    """Form to download dataset as CSV."""
    
    delimiter = StringField('Delimiter', [InputRequired('Delimiter is required.'), Length(min=1, max=1)], default=",")
    quotechar = StringField('Qoute character', [InputRequired('Qoute character is required.'), Length(min=1, max=1)], default='"')
    nullrep = StringField('NULL representation', [InputRequired('NULL representation is required.'), Length(min=1, max=10)], default="NULL")
    original_check = BooleanField('Include original tables', default = False)
# ENDCLASS

class DownloadDatasetSQLForm(FlaskForm):
    """Form to download dataset as SQL."""

    original_check = BooleanField('Include original tables', default = False)
# ENDCLASS

class DownloadTableCSVForm(FlaskForm):
    """Form to download table as CSV."""
    delimiter = StringField('Delimiter', [InputRequired('Delimiter is required.'), Length(min=1, max=1)], default=",")
    quotechar = StringField('Qoute character', [InputRequired('Qoute character is required.'), Length(min=1, max=1)], default='"')
    nullrep = StringField('NULL representation', [InputRequired('NULL representation is required.'), Length(min=1, max=10)], default="NULL")
# ENDCLASS

class DownloadTableSQLForm(FlaskForm):
    """Form to download table as SQL."""
    pass
# ENDCLASS

class TableJoinForm(FlaskForm):
    '''Test Form for Dynamic Fields'''
    tablename1 = SelectField('First Table', choices=[], id='tablename1')
    attribute1 = SelectField('First Table Attribute', choices=[], id='attribute1')
    tablename2 = SelectField('Second Table', choices=[], id='tablename2')
    attribute2 = SelectField('Second Table Attribute', choices=[], id='attribute2')
    newname = StringField('New Table Name', [InputRequired(message="New table name is required."), Regexp('^[A-Za-z0-9][A-Za-z0-9]+$')])

    def fillForm(self, tables):
        self.tablename1.choices = [(table, table) for table in tables]
        self.attribute1.choices = []
        self.tablename2.choices = [(table, table) for table in tables]
        self.attribute2.choices = []
    # ENDMETHOD

    def fillTable1(self, attrs):
        self.attribute1.choices = [(attr, attr) for attr in attrs]
    # ENDMETHOD

    def fillTable2(self, attrs):
        self.attribute2.choices = [(attr, attr) for attr in attrs]
    # ENDMETHOD
# ENDCLASS

class AttributeForm(FlaskForm):
    """Form to select attribute for which stats will be displayed."""
    view_attr = SelectField('Attribute', choices=[], id="view_attr")

    def fillForm(self, attrs):
        self.view_attr.choices = [(attr, attr) for attr in attrs]
    # ENDMETHOD
# ENDCLASS

class HistoryForm(FlaskForm):
    options = SelectField('Table', choices=[], id="history_options")

    def fillForm(self, tables):
        self.options.choices = [(table, table) for table in tables]
        self.options.choices.insert(0, ("__dataset", "Entire Dataset"))
    # ENDMETHOD
# ENDCLASS
