# LocationClosureAggregatorLambda

This repository contains the code used by the [LocationClosureAggregator-qa](https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/LocationClosureAggregator-qa?newFunction=true&tab=code) and [LocationClosureAggregator-production](https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/LocationClosureAggregator-production?newFunction=true&tab=code) AWS lambda functions. It primarily aggregates a series of closure alerts picked up by the [LocationClosureAlertPoller](https://github.com/NYPL/location-hours). Specifically, it does the following:
1. Determines the true length of each closure based on the alert data and the time the alerts were polled. See the [TAD](https://docs.google.com/document/d/1eiu2257Nf8nnODA_2Cz79kHqJRLs2CRmVHTZLTQTzB4/edit?usp=sharing) for specifics.
2. Inserts the resulting closures into Redshift
3. Deletes the closure alerts from the Redshift staging table

## Git workflow
This repo uses the [Main-QA-Production](https://github.com/NYPL/engineering-general/blob/main/standards/git-workflow.md#main-qa-production) git workflow.

`main` has the latest and greatest commits, `qa` has what's in our QA environment, and `production` has what's in our production environment.

## Deployment
CI/CD is not enabled. To deploy a new version of this function, first modify the code in the git repo and open a pull request to the appropriate environment branch. Then run `source deployment_script.sh` and upload the resulting zip. Note that if any files are added or deleted, this script must be modified. For more information, see the directions [here](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html).
