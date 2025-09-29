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

    if instance is None:
        return True
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

    errors = []
    try:
        validate(instance=message, schema=schema, format_checker=format_checker)
    except ValidationError as e:
        error_path = "/".join([str(p) for p in e.path]) or "<root>"
        errors.append(f"{error_path}: {e.message}")
    return errors
