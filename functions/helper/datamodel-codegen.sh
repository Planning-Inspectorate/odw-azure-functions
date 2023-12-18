echo "Generating pydantic model"

datamodel-codegen --input "C:\Users\ChrisTopping\Git\back-office\apps\api\src\message-schemas\events\service-user.schema.json" \
--output "C:\Users\ChrisTopping\Git\ODW-Service\functions\model_service_user.py" \
--input-file-type jsonschema \
--output-model-type pydantic_v2.BaseModel \
--use-standard-collections \
--use-union-operator \
--use-field-description \
--target-python-version 3.11 \
--use-schema-description

echo "pydantic model generated"