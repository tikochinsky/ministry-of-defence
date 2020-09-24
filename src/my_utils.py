import datetime as dt


def get_type(string_type: str) -> type:
    if string_type == "<class 'int'>":
        return int

    if string_type == "<class 'str'>":
        return str

    if string_type == "<class 'datetime.datetime'>":
        return dt.datetime
