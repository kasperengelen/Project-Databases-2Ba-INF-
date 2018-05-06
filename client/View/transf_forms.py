from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, BooleanField, IntegerField
from wtforms.widgets import HiddenInput
from wtforms.validators import Length, InputRequired, Email, EqualTo, Regexp
from View.form_utils import EnumCheck, FilenameCheck

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
    case_sens = BooleanField('Case sensitive', default=False)

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

class ExtractDateTimeForm(FlaskForm):
    """Form for the delete attribute/column transformation."""
    select_attr = SelectField('Attribute', choices=[])
    select_extracttype = SelectField('Attribute', choices=[("YEAR", "YEAR"), ("MONTH + YEAR", "MONTH + YEAR"), 
        ("MONTH", "MONTH"), ("DAY OF THE WEEK", "DAY OF THE WEEK")])

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class DataTypeTransform(FlaskForm):
    """Form for datatype conversion transformation."""
    select_attr = SelectField('Attribute', choices=[], id='attribute')
    new_datatype = SelectField('New Datatype', choices=[], id='typeOptions')
    char_amount = IntegerField('Character Amount', [InputRequired(message="input is required.")], default=1)
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
    select_attr = SelectField('Attribute', choices=[])
    ranges = StringField('Ranges (comma separated values)', [InputRequired("Input is required.")])
    interval_spec = SelectField('Right/Left open', choices = [(True, '[a, b['), (False, ']a, b]')], coerce = lambda x : x == 'True')

    def fillForm(self, attrs):
        self.select_attr.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class DeleteOutlier(FlaskForm):
    """Form to replace outlier values with NULL."""
    select_attr = SelectField('Attribute', choices=[])
    select_comparison = SelectField('Larger/Smaller', choices=[(True, 'Larger'), (False, 'Smaller')], coerce = lambda x : x == 'True')
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

# TODO reform predicate form: remove wtform

class PredicateFormOne(FlaskForm):

    attr1 = SelectField('Attribute', choices = [], id="attr1")
    op1 = SelectField('Operator', choices = [('=', '='), ('!=', '!='), ('>', '>'), ('<', '<')], id="op1")
    input1 = StringField('Input', [InputRequired(message="Input is required.")], id="input1")
    select1 = SelectField('Logic operator', choices = [('END', 'END'), ('And', 'AND'), ('Or', 'OR')], id="select1")

    def fillForm(self, attrs):
        self.attr1.choices = [(attrname, attrname) for attrname in attrs]
    # ENDMETHOD
# ENDCLASS

class PredicateFormTwo(FlaskForm):

    attr2 = SelectField('Attribute', choices = [], id="attr2")
    op2 = SelectField('Operator', choices = [('=', '='), ('!=', '!='), ('>', '>'), ('<', '<')], id="op2")
    input2 = StringField('Input', [InputRequired(message="Input is required.")], id="input2")
    select2 = SelectField('Logic operator', choices = [('END', 'END'), ('And', 'AND'), ('Or', 'OR')], id="select2")

    def fillForm(self, attrs):
        self.attr2.choices = [(attrname, attrname) for attrname in attrs]

    #ENDMETHOD
# ENDCLASS

class PredicateFormThree(FlaskForm):

    attr3 = SelectField('Attribute', choices = [], id="attr3")
    op3 = SelectField('Operator', choices = [('=', '='), ('!=', '!='), ('>', '>'), ('<', '<')], id="op3")
    input3 = StringField('Input', [InputRequired(message="Input is required.")], id="input3")

    def fillForm(self, attrs):
        self.attr3.choices = [(attrname, attrname) for attrname in attrs]

    #ENDMETHOD
# ENDCLASS

