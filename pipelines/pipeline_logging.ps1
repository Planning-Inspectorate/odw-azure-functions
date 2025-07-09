# To run this from the terminal you can use powershell or bash
# This is saved as .ps1 file so to run it, open a powershell prompt and type .\pipeline_logging.ps1
# Enter a file path to save the output. Relative paths are ok here, e.g. ./output.txt
# For bash, simply copy the az command into the terminal and replace the back ticks with \ to split the lines
# For bash, replace Write-Host with echo to print something

$filePath = Read-Host "Enter the file path to save the output (e.g., C:\output.txt):"

$azOutput = az pipelines runs list -p operational-data-warehouse `
    --organization https://dev.azure.com/planninginspectorate/ `
    --query "[?((definition.name=='Platform Deploy' || definition.name=='Synapse Release' || definition.name=='function-app-deploy') && templateParameters.environment=='Prod')] | [].{Name: definition.name, Environment: templateParameters.environment, BuildNumber: buildNumber, RequestedBy: requestedBy.displayName, SourceBranch: sourceBranch, Result: result, StartTime: startTime, FinishTime: finishTime}" `
    --output table

# Write the output to the specified file
$azOutput | Out-File -FilePath $filePath

Write-Host "Output saved to $filePath"