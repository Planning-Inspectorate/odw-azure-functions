"""
Module containing a validate function to be called to run validation
of a list of sevricebus messages.

The validation uses jsonschema with a format checker function to 
check that the date-time is compliant with ISO8601.
"""

from jsonschema import validate, FormatChecker, ValidationError
from iso8601 import parse_date, ParseError


def is_iso8601_date_time(instance) -> bool:
    """
    Function to check if a date matches ISO-8601 format
    """

    try:
        parse_date(instance)
        return True
    except ParseError:
        return False


def validate_data(message, schema: dict) -> bool:
    """
    Function to validate a list of servicebus messages.
    Validation includes a format check against ISO-8601.
    """

    format_checker = FormatChecker()
    format_checker.checks("date-time")(is_iso8601_date_time)

    try:
        validate(instance=message, schema=schema, format_checker=format_checker)
        is_valid = True
    except ValidationError as e:
        is_valid = False
        print(e)
    return is_valid
