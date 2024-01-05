#!/bin/bash

echo "Generating Pydantic models"

# Array of schema names
declare -a schema_names=(
  "case-schedule"
  "employee"
  "nsip-document"
  "nsip-exam-timetable"
  "nsip-project-update"
  "nsip-project"
  "nsip-representation"
  "nsip-subscription"
  "s51-advice"
  "service-user"
  # Add more schema names as needed
)

for schema_name in "${schema_names[@]}"; do
    url="https://raw.githubusercontent.com/Planning-Inspectorate/data-model/main/schemas/${schema_name}.schema.json"
    filename="model_${schema_name}"
    output_path="C:\Users\ChrisTopping\Git\ODW-Service\functions\\${filename}.py"

    datamodel-codegen --url "$url" \
    --output "$output_path" \
    --input-file-type jsonschema \
    --output-model-type pydantic_v2.BaseModel \
    --use-standard-collections \
    --use-union-operator \
    --use-field-description \
    --target-python-version 3.11 \
    --use-schema-description

    echo "Pydantic model generated for $filename"

    # Rename the file to replace dashes with underscores
    renamed_filename="${filename//-/_}"  # Replace dashes with underscores
    new_path="C:\Users\ChrisTopping\Git\ODW-Service\functions\\${renamed_filename}.py"
    mv ${output_path} ${new_path}

  echo "File renamed to ${renamed_filename}.py"
done

echo "Pydantic models generated for all schemas"