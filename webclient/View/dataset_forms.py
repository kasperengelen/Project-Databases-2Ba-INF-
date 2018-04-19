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
    columnnames_included = BooleanField('Column names included in files? (CSV only)', default = True)
# ENDCLASS

class DownloadForm(FlaskForm):
    """Form to specify CSV properties for download."""

    delimiter = StringField('Delimiter', [InputRequired('Input is required.'), Length(min=1, max=1)])
    quotechar = StringField('Qoute character', [InputRequired('Input is required.'), Length(min=1, max=1)])
    nullrep = StringField('NULL representation', [InputRequired('Input is required.'), Length(min=1, max=10)])

# ENDCLASS

class TableJoinForm(FlaskForm):
    '''Test Form for Dynamic Fields'''
    tablename1 = SelectField('First Table', choices=[], id='tablename1')
    attribute1 = SelectField('First Table Attribute', choices=[], id='attribute1')
    tablename2 = SelectField('Second Table', choices=[], id='tablename2')
    attribute2 = SelectField('Second Table Attribute', choices=[], id='attribute2')
    newname = StringField('New Table Name', [InputRequired(message="Input is required."), Regexp('^[A-Za-z0-9][A-Za-z0-9]+$')])

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

class EntryCountForm(FlaskForm):
    """Form to select how many entries need to be displayed."""

    entry_count = SelectField('Entries', choices = [(10, '10'), (20, '20'), (50, '50'), (100, '100'), (500, '500')])
    cur_dataset = HiddenField('cur_dataset')
    cur_tablename = HiddenField('cur_tablename')

    def fillForm(self, dataset_id, tablename):
        self.cur_dataset.data = dataset_id
        self.cur_tablename.data = tablename
    # ENDMETHOD

# ENDCLASS

################################################################# TRANSFORMATION FORMS #################################################################

class FindReplaceForm(FlaskForm):
    """Form for the search and replace transformation."""
    select_attr = SelectField('Attribute', choices=[])
    search = StringField('Search', [InputRequired(message="Input is required.")])
    replacement = StringField('Replacement', [InputRequired(message="Input is required.")])
    exactmatch = BooleanField('Exact match', default=True)
    replace_full_match = BooleanField('Replace the full match', default=True)

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class RegexFindReplace(FlaskForm):
    """Form for the regex find and replace transformation."""
    select_attr = SelectField('Attribute', choices=[])
    regex = StringField('Search regex', [InputRequired(message="Input is required.")])
    replacement = StringField('Replacement', [InputRequired(message="Input is required.")])
    case_sens = BooleanField('Case sensitive', [InputRequired(message="Input is required.")], default=False)

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
    """Form for datatype conversion transformation."""
    select_attr = SelectField('Attribute', choices=[], id='attribute')
    new_datatype = SelectField('New Datatype', choices=[], id='typeOptions')
    char_amount = StringField('Character Amount', [InputRequired(message="input is required.")], default=1)
    date_type = SelectField('Date/Time Format', choices=[], id="date_type")

    def fillForm(self, attrs, datatypes, datetimetypes):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
        self.new_datatype.choices = [(datatype, datatype) for datatype in datatypes]
        self.date_type.choices = [(datetimetype, datetimetype) for datetimetype in datetimetypes]
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

class DiscretizeEqualWidth(FlaskForm):
    """Form to discretize values into equidistant intervals."""
    select_attr = SelectField('Attribute', choices=[])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class DiscretizeEqualFreq(FlaskForm):
    """Form to discretize values into equifrequent intervals."""
    select_attr = SelectField('Attribute', choices=[])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class DiscretizeCustomRange(FlaskForm):
    pass
# ENDCLASS

class DeleteOutlier(FlaskForm):
    """Form to replace outlier values with NULL."""
    select_attr = SelectField('Attribute', choices=[])
    select_comparison = SelectField('Larger/Smaller', choices=[(True, 'Larger'), (False, 'Smaller')])
    value = StringField('Value', [InputRequired(message="Input is required.")])
    
    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class FillNullsMean(FlaskForm):
    """Form to fill all NULL values with the mean value."""
    select_attr = SelectField('Attribute', choices=[])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class FillNullsMedian(FlaskForm):
    """Form to fill all NULL values with the median value."""
    select_attr = SelectField('Attribute', choices=[])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class FillNullsCustomValue(FlaskForm):
    """Form to fill all NULL values with custom value."""
    select_attr = SelectField('Attribute', choices=[])
    replacement = StringField('Replacement', [InputRequired(message="Input is required.")])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class PredicateForm(FlaskForm):
    """Form to handle predicates."""

    # attributes
    attr1 = SelectField('Attribute 1', choices = [])
    attr2 = SelectField('Attribute 2', choices = [])
    attr3 = SelectField('Attribute 3', choices = [])

    # comparator operators
    op1 = SelectField('Operator 1', choices = [('Equal', 'EQ'), ('Not Equal', 'NEQ'), ('Greater than', 'GT'), ('Less than', 'LT'), ('Contains', 'IN')])
    op2 = SelectField('Operator 2', choices = [('Equal', 'EQ'), ('Not Equal', 'NEQ'), ('Greater than', 'GT'), ('Less than', 'LT'), ('Contains', 'IN')])
    op3 = SelectField('Operator 3', choices = [('Equal', 'EQ'), ('Not Equal', 'NEQ'), ('Greater than', 'GT'), ('Less than', 'LT'), ('Contains', 'IN')])

    # inputs
    input1 = StringField('Input 1')
    input2 = StringField('Input 2')
    input3 = StringField('Input 3')

    # logic operators
    select1 = SelectField('Logic operator 1', choices = [('Finish', 'END'), ('And', 'AND'), ('Or', 'OR')])
    select2 = SelectField('Logic operator 2', choices = [('Finish', 'END'), ('And', 'AND'), ('Or', 'OR')])

    def fillForm(self, attrs1, attrs2, attrs3):
        self.attr1.choices = [(attrname, attrname) for attrname in attrs1]
        self.attr2.choices = [(attrname, attrname) for attrname in attrs2]
        self.attr3.choices = [(attrname, attrname) for attrname in attrs3]
    # ENDMETHOD
# ENDCLASS