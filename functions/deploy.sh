#!/bin/bash
function_app_dev="pins-fnapp01-odw-dev-uks"
function_app_preprod="pins-fnapp01-odw-test-uks"
function_app_prod="pins-fnapp01-odw-prod-uks"

subscription_dev="ff442a29-fc06-4a13-8e3e-65fd5da513b3"
subscription_preprod="6b18ba9d-2399-48b5-a834-e0f267be122d"
subscription_prod="a82fd28d-5989-4e06-a0bb-1a5d859f9e0c"

echo "Starting deployment..."

func azure functionapp publish $function_app_dev --subscription $subscription_dev
# func azure functionapp publish $function_app_preprod --subscription $subscription_preprod
# func azure functionapp publish $function_app_prod --subscription $subscription_prod

echo "Deployment completed"