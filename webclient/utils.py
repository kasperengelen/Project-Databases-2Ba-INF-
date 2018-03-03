# file for various utility functions
import json
import user_utils

def sql_time_to_dict(sql_date_string):
    """Given a string of the format "YYYY:MM:DD HH:MM:SS.SSSSSS" this
    returns a dict containing the same data under the keys 'Y', 'M', 'D', 'hr', 'min', 'sec', 'sec_full'.
    With 'sec' containing the seconds rounded to an integer, and 'sec_full' containing the full original value."""

    date = sql_date_string.split(' ')[0] # split on space between date and time
    time = sql_date_string.split(' ')[1]

    year = int(date.split('-')[0])
    month = int(date.split('-')[1])
    day = int(date.split('-')[2])

    hour = int(time.split(':')[0])
    minute = int(time.split(':')[1])
    sec_full = float(time.split(':')[2])
    sec = int(sec_full)

    return {
        "Y": year,
        "M": month,
        "D": day,

        "hr": hour,
        "min": minute,
        "sec": sec,
        "sec_full": sec_full
    }


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, user_utils.UserInformation):
            return obj.toJson()

        return super().default(obj)