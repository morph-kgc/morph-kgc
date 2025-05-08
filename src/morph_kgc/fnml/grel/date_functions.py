from ..function_decorator import *
from datetime import datetime
from datetime import timedelta


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#unicodestring-s",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
)
def string_unicode(string):
    return [ord(e) for e in string]


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#date_toDate",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    format_code="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_pattern",
)
def date_to_date(string, format_code):
    from datetime import datetime

    # Map GREL format to Python format
    grel_to_python_format = {
        "yyyy": "%Y",
        "yy": "%y",
        "MMMM": "%B",
        "MMM": "%b",
        "MM": "%m",
        "dd": "%d",
        "EEEE": "%A",
        "EEE": "%a",
        "HH": "%H",
        "hh": "%I",
        "mm": "%M",
        "ss": "%S",
        "a": "%p",
        "Z": "%z",
        "X": "%Z",
        "SSS": "%f",
    }
    for grel, py in grel_to_python_format.items():
        format_code = format_code.replace(grel, py)
    return datetime.strptime(string, format_code).isoformat()


@bif(
    fun_id="https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#date_toDate",
    string="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter",
    format_code="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_pattern",
)
def date_to_python_date(string, format_code):
    from datetime import datetime

    return datetime.strptime(string, format_code).isoformat()


@bif(fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#date_now")
def date_now():
    return datetime.now().isoformat()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#date_diff",
    date_1="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_datetime_d",
    date_2="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_datetime_d2",
    unit="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_timeunit",
)
def date_diff(date_1: datetime, date_2: datetime, unit: str = None) -> str:
    if type(date_1) == str:
        date_1 = datetime.fromisoformat(date_1)
    date_1 = date_1.replace(tzinfo=None)
    if type(date_2) == str:
        date_2 = datetime.fromisoformat(date_2)
    date_2 = date_2.replace(tzinfo=None)
    timedelta = None
    if date_1 > date_2:
        timedelta = date_1 - date_2
    else:
        timedelta = date_2 - date_1
    if unit == "days" or unit == "d":
        return timedelta.days
    elif unit == "hours" or unit == "h":
        return timedelta.seconds * 60
    elif unit == "minutes" or unit == "m":
        return timedelta.days * 24 * 60 + timedelta.seconds // 60
    elif unit == "seconds" or unit == "s":
        return timedelta.seconds
    elif unit == "weeks" or unit == "w":
        return timedelta.days / 7

    return datetime.now().isoformat()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#date_datePart",
    date="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_date_d",
    unit="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_unit",
)
def date_diff(date, unit):
    if type(date) == str:
        date = datetime.fromisoformat(date)
    date = date.replace(tzinfo=None)
    if unit in ["years", "year"]:
        return date.year
    elif unit in ["months", "month"]:
        return date.month
    elif unit in ["weeks", "week", "w"]:
        return (date.day - 1) // 7 + 1
    elif unit in ["days", "day", "d"]:
        return date.day
    elif unit == "weekday":
        return date.strftime("%A")
    elif unit in ["hours", "hour", "h"]:
        return date.hour
    elif unit in ["minutes", "minute", "min"]:
        return date.minute
    elif unit in ["seconds", "sec", "s"]:
        return date.second
    elif unit in ["milliseconds", "ms", "S"]:
        return date.microsecond // 1000
    elif unit in ["nanos", "nano", "n"]:
        return date.microsecond * 1000
    elif unit == "time":
        epoch = datetime(1970, 1, 1)
        return int((date - epoch).total_seconds() * 1000)
    return datetime.now().isoformat()


@bif(
    fun_id="http://users.ugent.be/~bjdmeest/function/grel.ttl#date_inc",
    date="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_date_d",
    inc="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_dec_n",
    unit="http://users.ugent.be/~bjdmeest/function/grel.ttl#p_string_unit",
)
def date_diff(date, unit, inc):
    if type(date) == str:
        date = datetime.fromisoformat(date)
    inc = int(inc)
    date = date.replace(tzinfo=None)
    if unit in ["years", "year"]:
        return date.replace(year=date.year + inc)
    elif unit in ["months", "month"]:
        new_month = (date.month - 1 + inc) % 12 + 1
        new_year = date.year + (date.month - 1 + inc) // 12
        return date.replace(year=new_year, month=new_month)
    elif unit in ["weeks", "week", "w"]:
        return date + timedelta(weeks=inc)
    elif unit in ["days", "day", "d"]:
        return date + timedelta(days=inc)
    elif unit in ["hours", "hour", "h"]:
        return date + timedelta(hours=inc)
    elif unit in ["minutes", "minute", "min"]:
        return date + timedelta(minutes=inc)
    elif unit in ["seconds", "sec", "s"]:
        return date + timedelta(seconds=inc)
    elif unit in ["milliseconds", "ms", "S"]:
        return date + timedelta(milliseconds=inc)
    elif unit in ["nanos", "nano", "n"]:
        return date + timedelta(microseconds=inc // 1000)
    elif unit == "time":
        epoch = datetime(1970, 1, 1)
        return int((date - epoch).total_seconds() * 1000)
    return datetime.now().isoformat()
