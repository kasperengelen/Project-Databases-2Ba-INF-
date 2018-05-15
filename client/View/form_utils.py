import re
from wtforms.validators import ValidationError
from flask import flash

class EnumCheck:
    """Validator that checks if the supplied value is one of the listed possible values."""
    def __init__(self, message="", choices=[]):
        self.message = message
        self.choices = choices

    def __call__(self, form, field):
        if field.data not in self.choices:
            raise ValidationError(self.message)
# ENDCLASS

class FilenameCheck:
    """Validator to check if the filename of a field matches a regex."""
    def __init__(self, message="", regex=""):
        self.message=message
        self.regex = re.compile(regex)

    def __call__(self, form, field):
        if not self.regex.fullmatch(field.data.filename):
            raise ValidationError(self.message)
# ENDCLASS

def flash_errors(form):
    """Function that displays the errors contained 
    withing a form with flash.flash()."""

    for field, errors in form.errors.items():
        for error in errors:
            flash(message="Invalid input: '" + error + "'", category="error")
        # ENDFOR
    # ENDFOR
# ENDFUNCTION
