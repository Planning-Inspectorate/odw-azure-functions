"""
This module defines a pydantic model to be used for data validation. 
It defines the fields we expect to receive from the ODT Service Bus messages. 
The model does not accept extra fields and all fields are mandatory. 
Expected data types and constraints are also defined for each field.
"""

from pydantic import BaseModel, ValidationError, Field
from datetime import datetime
from servicebus_funcs import get_messages
import config

_NAMESPACE = config.ODT_NAMESPACE
_SUBSCRIPTION = config.NSIP_SUBSCRIPTION
_TOPIC = config.NSIP_TOPIC
_MAX_MESSAGE_COUNT = config.MAX_MESSAGE_COUNT
_CREDENTIAL = config.CREDENTIAL


def model() -> list[dict]:
    """
    The 'model' function defines a class 'NsipProject' and performs some operations on test data.

    Returns:
        data - a list of dictionaries containng Service Bus message data
    """

    class NsipProject(BaseModel):

        """
        NsipProject pydantic model with data fields expected from Service Bus
        """

        model_config = config.MODEL_CONFIG

        projectname: str = Field(description="Name of project")
        decision: str = Field(description="Application decision")
        publishstatus: str = Field(description="The publish status")
        examtimetablepublishstatus: str = Field(
            description="Published Status on website of examination timetable"
        )
        sector: str = Field(description="Case Sector")
        projecttype: str = Field(description="Subtype Of Project")
        stage: str = Field(description="Process stage")
        casereference: str = Field(description="Case Reference")
        caseid: int = Field(description="Back Office internal identifier")
        projectlocation: str = Field(description="Description of site location")
        projectemailaddress: str = Field(
            description="PINS Project email address published on website"
        )
        region: list[str] = Field(
            description="Geographical region of UK. Applications can cover multiple regions"
        )
        easting: int = Field(description="Project site Easting co-ordinate")
        northing: int = Field(description="Project site Northing co-ordinate")
        transboundary: bool = Field(
            description="Drives addition of Transboundary tab on website"
        )
        welshlanguage: bool = Field(description="Welsh Language translation required")
        mapzoomlevel: str = Field(
            description="Resolution of pinned map. Set when co-ordinates are created"
        )
        projectdescription: str = Field(
            description="Summary of Project (Project Description aka Summary)"
        )
        secretaryofstate: str = Field(description="Relevant Government Department")
        dateprojectappearsonwebsite: datetime = Field(
            description="Date Project Appears on Website"
        )
        dateofdcoacceptance: datetime = Field(
            description="Date Application is Formerly Accepted by PINS"
        )
        anticipateddateofsubmission: datetime = Field(
            description="Anticipated Submission Date of Application"
        )
        anticipatedsubmissiondatenonspecific: str = Field(
            description="Approximate Anticipated Submission Date of Application, e.g., Q3 2023"
        )
        dateofdcosubmission: datetime = Field(
            description="Date Application is submitted"
        )
        dateofrepresentationperiodopen: datetime = Field(
            description="Date at which point public can submit relevant reps"
        )
        dateofrelevantrepresentationclose: datetime = Field(
            description="Date at which point public can no longer submit relevant reps"
        )
        daterrepappearonwebsite: datetime = Field(
            description="Date at which relevant reps appear on the website"
        )
        confirmedstartofexamination: datetime = Field(
            description="ConfirmedStartOfExamination by panel"
        )
        datetimeexaminationends: datetime = Field(
            description="ConfirmedSEndOfExamination by panel"
        )
        stage4extensiontoexamclosedate: datetime = Field(
            description="Examination Period extended to this date"
        )
        stage5extensiontorecommendationdeadline: datetime = Field(
            description="Recommendation period extended to this date"
        )
        dateofrecommendations: datetime = Field(
            description="Date recomm report sent to Secretary of State"
        )
        confirmeddateofdecision: datetime = Field(
            description="Decision by Secretary of State"
        )
        stage5extensiontodecisiondeadline: datetime = Field(
            description="Decision period extended to this date"
        )
        dateprojectwithdrawn: datetime = Field(
            description="DateProjectWithdrawn by applicant"
        )
        section46notification: datetime = Field(
            description="Applicant must notify PINS of statutory consultation"
        )
        datepinsfirstnotifiedofproject: datetime = Field(
            description="Date at which applicant notify PINS of a project (pre-publishing)"
        )
        screeningopinionsought: datetime = Field(
            description="(TBC by Env. Services Team)"
        )
        screeningopinionissued: datetime = Field(
            description="(TBC by Env. Services Team)"
        )
        scopingopinionsought: datetime = Field(
            description="(TBC by Env. Services Team)"
        )
        scopingopinionissued: datetime = Field(
            description="(TBC by Env. Services Team)"
        )
        deadlineforacceptancedecision: datetime = Field(description="")
        datesection58noticereceived: datetime = Field(
            description="Applicant has notified all parties of application"
        )
        preliminarymeetingstartdate: datetime = Field(
            description="Meeting between all parties including public"
        )
        deadlineforcloseofexamination: datetime = Field(description="")
        deadlineforsubmissionofrecommendation: datetime = Field(description="")
        deadlinefordecision: datetime = Field(description="")
        jrperiodenddate: datetime = Field(description="Judicial Review")
        extensiontodaterelevantrepresentationsclose: str = Field(description="")
        operationsleadid: str = Field(description="Maps to [Employee]. [EmployeeID].")
        operationsmanagerid: str = Field(
            description="New NSIP role, Maps to [Employee].[EmployeeID]"
        )
        casemanagerid: str = Field(description="Maps to [Employee]. [EmployeeID]")
        nsipofficerid: list[str] = Field(
            description="{Set}: Maps to [Employee]. [EmployeeID]."
        )
        nsipadministrationofficerid: list[str] = Field(
            description="{Set}: Maps to [Employee]. [EmployeeID]."
        )
        leadinspectorid: str = Field(description="Maps to [Employee].[EmployeeID]")
        inspectorid: list[str] = Field(
            description="{Set}: Maps to [Employee].[EmployeeID]"
        )
        environmentalservicesofficerid: str = Field(
            description="Maps to [Employee].[EmployeeID]"
        )
        legalofficerid: str = Field(description="Maps to [Employee].[EmployeeID]")
        sourcesystem: str = Field(description="Where was the case created?")
        dateofnonacceptance: str = Field(
            description="Date when PINS decides not to accept the application for examination"
        )
        dateiapidue: str = Field(
            description="Provided by ODT this is not present in Horizon"
        )
        rule6letterpublishdate: str = Field(
            description="Date when PINS invite parties to preliminary meeting"
        )
        notificationdateforpmandeventsdirectlyfollowingpm: str = Field(
            description="Date we invite parties to the PM and subsequent hearings"
        )
        notificationdateforeventsdeveloper: str = Field(
            description="Date when developers must publish hearing notices in media?"
        )
        rule8letterpublishdate: str = Field(
            description="Date when exam timetable is confirmed to interested parties"
        )
        applicantid: str = Field(description="applicant id")

    class MessageInstances(BaseModel):

        """
        Represents a pydantic model for a list of NsipProject instances.

        Args:
            messagedata (list[NsipProject]): The list of NsipProject instances.

        Attributes:
            messagedata (list[NsipProject]): The list of NsipProject instances.
        """

        messagedata: list[NsipProject]

    _data = get_messages(
        _NAMESPACE, _CREDENTIAL, _TOPIC, _SUBSCRIPTION, _MAX_MESSAGE_COUNT
    )

    _messages_lower = []
    for message in _data:
        _lowercase_message = {k.lower(): v for k, v in message.items()}
        _messages_lower.append(_lowercase_message)
    if not _messages_lower:
        print("NO MESSAGES TO PROCESS - VALIDATION OK")
        return _data
    else:
        try:
            MessageInstances(messagedata=_messages_lower)
            print("VALIDATION SUCCEEDED!")
            print(f"{len(_messages_lower)} MESSAGES PROCESSED")
            return _data
        except ValidationError as e:
            print(e)
            raise e
