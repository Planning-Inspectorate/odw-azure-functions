
from service_bus_triggers import register_batch_trigger

register_batch_trigger(
    entity="appealdocument",
    topic_env="APPEAL_DOCUMENT_TOPIC",
    subscription_env="APPEAL_DOCUMENT_SUBSCRIPTION"
)
