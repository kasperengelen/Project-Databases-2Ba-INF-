from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, BooleanField, IntegerField
from wtforms.widgets import HiddenInput
from wtforms.validators import Length, InputRequired, Email, EqualTo, Regexp
from wtforms.form import Form
from View.form_utils import EnumCheck, FilenameCheck

class TransformationFormBase(FlaskForm):
    """Base class for all transformation forms."""

    make_new_table = BooleanField('Create New Table?', default = False)
    new_table_name = StringField('New Table Name', [InputRequired(message="Table name is required."), 
                                              Length(min=3, max=20, message="Tablename needs to be between 3 and 20 characters long."), 
                                              Regexp('^[A-Za-z0-9][A-Za-z0-9]+$')])
# ENDCLASS

class FindReplaceForm(TransformationFormBase):
    """Form for the search and replace transformation."""
    select_attr = SelectField('Attribute', choices=[])
    search = StringField('Search', [InputRequired(message="Search term is required.")])
    replacement = StringField('Replacement', [InputRequired(message="Replacement is required.")])
    exactmatch = BooleanField('Exact match', default=True)
    replace_full_match = BooleanField('Replace the full match', default=True)

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class RegexFindReplace(TransformationFormBase):
    """Form for the regex find and replace transformation."""
    select_attr = SelectField('Attribute', choices=[])
    regex = StringField('Search regex', [InputRequired(message="Regex is required.")])
    replacement = StringField('Replacement', [InputRequired(message="Replacement is required.")])
    case_sens = BooleanField('Case sensitive', default=False)

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class ExtractDateTimeForm(TransformationFormBase):
    """Form for the delete attribute/column transformation."""
    select_attr = SelectField('Attribute', choices=[])
    select_extracttype = SelectField('Attribute', choices=[("YEAR", "YEAR"), ("MONTH + YEAR", "MONTH + YEAR"), 
        ("MONTH", "MONTH"), ("DAY OF THE WEEK", "DAY OF THE WEEK")])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class DataTypeTransform(TransformationFormBase):
    """Form for datatype conversion transformation."""
    select_attr = SelectField('Attribute', choices=[], id='attribute')
    new_datatype = SelectField('New Datatype', choices=[], id='typeOptions')
    char_amount = IntegerField('Character Amount', [InputRequired(message="Character amount is required.")], default=1)
    date_type = SelectField('Date/Time Format', choices=[], id="date_type")

    do_force = BooleanField('Force conversion if needed?', default=False)
    force_mode = SelectField(choices = [('SET_NULL', 'Set to NULL'), ('DELETE_ROW', 'Delete row')])

    def fillForm(self, attrs, datatypes, datetimetypes):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
        self.new_datatype.choices = [(datatype, datatype) for datatype in datatypes]
        self.date_type.choices = [(datetimetype, datetimetype) for datetimetype in datetimetypes]
    # ENDMETHOD
# ENDCLASS

class NormalizeZScore(TransformationFormBase):
    """Form to normalize an attribute by it's z-score."""
    select_attr = SelectField('Attribute', choices=[])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class OneHotEncoding(TransformationFormBase):
    """Form to perform the one-hot-encoding transformation."""
    select_attr = SelectField('Attribute', choices=[])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class DiscretizeEqualWidth(TransformationFormBase):
    """Form to discretize values into equidistant intervals."""
    select_attr = SelectField('Attribute', choices=[])
    nr_of_bins = IntegerField('Number of Bins', [InputRequired(message="Number of bins is required.")])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class DiscretizeEqualFreq(TransformationFormBase):
    """Form to discretize values into equifrequent intervals."""
    select_attr = SelectField('Attribute', choices=[])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class DiscretizeCustomRange(TransformationFormBase):
    select_attr = SelectField('Attribute', choices=[])
    ranges = StringField('Ranges (comma separated values)', [InputRequired("Range specification is required.")])
    interval_spec = SelectField('Right/Left open', choices = [(True, '[a, b['), (False, ']a, b]')], coerce = lambda x : x == 'True')

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class DeleteOutlier(TransformationFormBase):
    """Form to replace outlier values with NULL."""
    select_attr = SelectField('Attribute', choices=[])
    select_comparison = SelectField('Larger/Smaller', choices=[(True, 'Larger'), (False, 'Smaller')], coerce = lambda x : x == 'True')
    value = StringField('Value', [InputRequired(message="Comparison value is required.")])
    
    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class FillNullsMean(TransformationFormBase):
    """Form to fill all NULL values with the mean value."""
    select_attr = SelectField('Attribute', choices=[])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class FillNullsMedian(TransformationFormBase):
    """Form to fill all NULL values with the median value."""
    select_attr = SelectField('Attribute', choices=[])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class FillNullsCustomValue(TransformationFormBase):
    """Form to fill all NULL values with custom value."""
    select_attr = SelectField('Attribute', choices=[])
    replacement = StringField('Replacement', [InputRequired(message="Replacement value is required.")])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class PredicateFormOne(TransformationFormBase):

    attr1 = SelectField('Attribute', choices = [], id="attr1")
    op1 = SelectField('Operator', choices = [('=', '='), ('!=', '!='), ('>', '>'), ('<', '<')], id="op1")
    input1 = StringField('Input 1', [InputRequired(message="Input 1 is required.")], id="input1")
    select1 = SelectField('Logic operator', choices = [('END', 'END'), ('And', 'AND'), ('Or', 'OR')], id="select1")

    def fillForm(self, attrs):
        self.attr1.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class PredicateFormTwo(TransformationFormBase):

    attr2 = SelectField('Attribute', choices = [], id="attr2")
    op2 = SelectField('Operator', choices = [('=', '='), ('!=', '!='), ('>', '>'), ('<', '<')], id="op2")
    input2 = StringField('Input 2', [InputRequired(message="Input 2 is required.")], id="input2")
    select2 = SelectField('Logic operator', choices = [('END', 'END'), ('And', 'AND'), ('Or', 'OR')], id="select2")

    def fillForm(self, attrs):
        self.attr2.choices = [(attrname, attrname) for attrname in attrs]

    #ENDMETHOD
# ENDCLASS

class PredicateFormThree(TransformationFormBase):

    attr3 = SelectField('Attribute', choices = [], id="attr3")
    op3 = SelectField('Operator', choices = [('=', '='), ('!=', '!='), ('>', '>'), ('<', '<')], id="op3")
    input3 = StringField('Input 3', [InputRequired(message="Input 3 is required.")], id="input3")

    def fillForm(self, attrs):
        self.attr3.choices = [(attrname, attrname) for attrname in attrs]

    #ENDMETHOD
# ENDCLASS

