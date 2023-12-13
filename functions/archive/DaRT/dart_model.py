"""
Module that contains a pydantic model whcih represents the data returned from
the DaRT API call which retrieves data from Azure Synapse on Cases.

The data from Synapse contains multiple rows for each case due to each case having
multiple contacts, specialisms etc. The API response needs to be one json object for
each case so the data is pre-processed before being validated by pydantic.
"""

from pydantic import BaseModel, ValidationError
import csv
from collections import defaultdict


class Contact(BaseModel):

    """
    Represents a contact.

    Args:
        contactid (str): The ID of the contact.
        title (str): The title of the contact.
        salutation (str): The salutation of the contact.
        firstname (str): The first name of the contact.
        lastname (str): The last name of the contact.
        typeofinvolvement (str): The type of involvement of the contact.
    """

    contactid: str
    title: str
    salutation: str
    firstname: str
    lastname: str
    typeofinvolvement: str


class Specialism(BaseModel):

    """
    Represents a specialism.

    Args:
        specialismid (int | str): The ID of the specialism.
        specialism (str): The specialism.
    """

    specialismid: int | str
    specialism: str


class GroundForAppeal(BaseModel):

    """
    Represents a ground for appeal.

    Args:
        groundforappeal (str): The ground for appeal.
        groundletter (str): The ground letter.
        groundforappealstartdate (str): The start date of the ground for appeal.
    """

    groundforappeal: str
    groundletter: str
    groundforappealstartdate: str


class Event(BaseModel):

    """
    Represents an event.

    Args:
        eventtype (str): The type of the event.
        startdateofevent (str): The start date of the event.
        starttimeofevent (str): The start time of the event.
        enddateofevent (str): The end date of the event.
        dateeventrequested (str): The date the event was requested.
    """

    eventtype: str
    startdateofevent: str
    starttimeofevent: str
    enddateofevent: str
    dateeventrequested: str


class ChartStatus(BaseModel):

    """
    Represents a chart status.

    Args:
        chartstatus (str): The chart status.
        estpreptime (str): The estimated preparation time.
        estsittime (str): The estimated sitting time.
        estreptime (str): The estimated reporting time.
        actualduration (str): The actual duration.
    """

    chartstatus: str
    estpreptime: str
    estsittime: str
    estreptime: str
    actualduration: str


class Cases(BaseModel):

    """
    Represents cases.

    Args:
        Case fields returned by the DaRT API
    """

    casereference: str
    appealrefnumber: str
    contacts: list[Contact] = []
    specialisms: list[Specialism] = []
    groundsforappeal: list[GroundForAppeal] = []
    eventtypes: list[Event] = []
    chartstatuses: list[ChartStatus] = []
    applicationtype: str
    appealtypereason: str
    appealtypegroup: str
    appealtype: str
    procedurename: str
    validity: str
    processingstate: str
    linkedappeal: str
    lastpublisheddate: str
    lpacode: str
    lpaname: str
    jurisdiction: str
    developmentorallegation: str
    caseprocess: str
    developmenttype: str
    relatedreferences: str
    appealsourceindicator: str
    level: str
    specialcircumstance: str
    appellantstatementsubmitted: str
    welshlanguage: str
    procedureappellant: str
    appealrecdate: str
    appealstartdate: str
    validto: str
    validitystatusdate: str
    callindate: str
    targetdate: str
    statementsdue: str
    thirdpartyrepsdue: str
    finalcommentsdue: str
    statementofcommongrounddue: str
    proofsdue: str
    consentdate: str
    ownershippermission: str
    modifydate: str
    lpadecisiondate: str
    outcomepublicsafety: str
    advertdetailsid: str
    reasonpublicsafety: str
    outcomeamenity: str
    reasonamenity: str
    advertinposition: str
    advertdescription: str
    lpaapplicationdate: str
    lpaapplicationreference: str
    section: str
    addressline1: str
    addressline2: str
    addresstown: str
    postcode: str
    easting: str
    northing: str
    inspectorneedtoentersite: str
    numberofresidences: str
    areaofsiteinhectares: str
    floorspaceinsquaremetres: str
    sitegreenbelt: str
    historicbuildinggrantmade: str
    agriculturalholding: str
    notificationofsv: str


class DartInstances(BaseModel):

    """
    Represents Dart instances.

    Args:
        dart_data (list[Cases]): The list of Dart cases.
    """

    dart_data: list[Cases]


_contact_fields = [k.lower() for k, v in Contact.__annotations__.items()]
_specialism_fields = [k.lower() for k, v in Specialism.__annotations__.items()]
_groundforappeal_fields = [
    k.lower() for k, v in GroundForAppeal.__annotations__.items()
]
_event_fields = [k.lower() for k, v in Event.__annotations__.items()]
_chartstatus_fields = [k.lower() for k, v in ChartStatus.__annotations__.items()]

_excluded_fields = (
    _contact_fields
    + _specialism_fields
    + _groundforappeal_fields
    + _event_fields
    + _chartstatus_fields
)

# temporary use of sample data from csv export from Synapse
# data should be changed to the result of the API call from the Function App
_file_name = r"C:\Users\ChrisTopping\Downloads\dart_sample_ordered.csv"

_data = []
with open(_file_name, encoding="utf-8-sig") as csvfile:
    _reader = csv.DictReader(csvfile)
    _data.extend(_reader)

_merged_data = defaultdict(dict)
for row in _data:
    _lowercase_row = {k.lower(): v for k, v in row.items()}
    _appealrefnumber = _lowercase_row["appealrefnumber"]
    if _appealrefnumber not in _merged_data:
        _merged_data[_appealrefnumber] = {
            k.lower(): v for k, v in row.items() if k.lower() not in _excluded_fields
        }
        _merged_data[_appealrefnumber]["contacts"] = []
        _merged_data[_appealrefnumber]["specialisms"] = []
        _merged_data[_appealrefnumber]["groundsforappeal"] = []
        _merged_data[_appealrefnumber]["events"] = []
        _merged_data[_appealrefnumber]["chartstatus"] = []

    _contacts = _merged_data[_appealrefnumber]["contacts"]
    _new_contact = {k: _lowercase_row[k] for k, v in Contact.__annotations__.items()}
    if _new_contact not in _contacts:
        _contacts.append(_new_contact)

    _specialisms = _merged_data[_appealrefnumber]["specialisms"]
    _new_specialism = {
        k: _lowercase_row[k] for k, v in Specialism.__annotations__.items()
    }
    if _new_specialism not in _specialisms:
        _specialisms.append(_new_specialism)

    _groundsforappeal = _merged_data[_appealrefnumber]["groundsforappeal"]
    _new_groundforappeal = {
        k: _lowercase_row[k] for k, v in GroundForAppeal.__annotations__.items()
    }
    if _new_groundforappeal not in _groundsforappeal:
        _groundsforappeal.append(_new_groundforappeal)

    _events = _merged_data[_appealrefnumber]["events"]
    _new_event = {k: _lowercase_row[k] for k, v in Event.__annotations__.items()}
    if _new_event not in _events:
        _events.append(_new_event)

    _chartstatus = _merged_data[_appealrefnumber]["chartstatus"]
    _new_chartstatus = {
        k: _lowercase_row[k] for k, v in ChartStatus.__annotations__.items()
    }
    if _new_chartstatus not in _chartstatus:
        _chartstatus.append(_new_chartstatus)

if _final_data := list(_merged_data.values()):
    try:
        DartInstances(dart_data=_final_data)
        print("VALIDATION SUCCEEDED!")
        print(f"{len(_final_data)} CASES PROCESSED")
    except ValidationError as e:
        print(e)
else:
    print("NO DATA TO PROCESS - VALIDATION OK")
