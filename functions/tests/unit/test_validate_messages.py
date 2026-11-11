from validate_messages import is_iso8601_date_time, validate_data


def test_validate_data_accepts_a_valid_payload():
    schema = {"type": "object", "required": ["name"]}

    assert validate_data({"name": "Ada"}, schema) == []


def test_validate_data_reports_required_property_errors():
    schema = {"type": "object", "required": ["name"]}

    errors = validate_data({}, schema)

    assert errors
    assert errors[0].startswith("<root>:")


def test_validate_data_checks_datetime_format():
    schema = {
        "type": "object",
        "properties": {"created": {"type": "string", "format": "date-time"}},
    }

    assert validate_data({"created": "not-a-date"}, schema)
    assert validate_data({"created": "2026-10-27T12:00:00Z"}, schema) == []


def test_iso8601_date_time_accepts_none_and_valid_values():
    assert is_iso8601_date_time(None)
    assert is_iso8601_date_time("2026-10-27T12:00:00Z")


def test_iso8601_date_time_rejects_invalid_values():
    assert not is_iso8601_date_time("not-a-date")
