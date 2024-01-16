"""
Module containing a validate function to be called to run validation
of a list of sevricebus messages
"""

from jsonschema import validate, FormatChecker, ValidationError
from iso8601 import parse_date, ParseError

def is_iso8601_date_time(instance):

    """
    Function to check if a date matches ISO-8601 format
    """

    try:
        parse_date(instance)
        return True
    except ParseError:
        return False

def validate_data(data: list, schema: dict) -> list:

    """
    Function to validate a list of servicebus messages.
    Validation includes a format check against ISO-8601.
    """
    
    format_checker = FormatChecker()
    format_checker.checks("date-time")(is_iso8601_date_time)

    success = True

    for message in data:
        try:
            validate(instance = message, schema = schema, format_checker = format_checker)
        except ValidationError as e:
            print(e)
            success = False
            raise e
    return data if success else []