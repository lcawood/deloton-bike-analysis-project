# Terraform
This folder should contain all code and resources required to handle the infrastructure of the project.

# 📝 Project Description
- This folder contains all the terraform code and details that are needed to setup the cloud services required in our project.

## 🔐 Terraform Variables
`.tfvars` keys used:
- `DATABASE_IP` -> ARN to your AWS RDS.
- `DATABASE_NAME` -> Name of your database.
- `DATABASE_USERNAME` -> Your database username.
- `DATABASE_PASSWORD` -> Password to access your database.
- `DATABASE_PORT` -> Port used to access the database.
- `AWS_ACCESS_KEY_ID_ `  -> Personal AWS ACCESS KEY available on AWS.
- `AWS_SECRET_ACCESS_KEY_` -> Personal AWS SECRET ACCESS KEY available on AWS.

## 🏃 Running the script

Run the terraform with `terraform init` and then `terraform apply`.
Remove the terraform with `terraform destroy`

## :card_index_dividers: Files Explained
- `main.tf`
    - A terraform script to create all resources and services needed within the project. These services include:
     - `RDS`
      - RDS Instance to store the Deloton data.
     - `RDS Security Group`
      - Security Group setup for the RDS to allow access on port 5432.
     - `ECR Daily Report`
      - An ECR to store the daily report container.
     - `Lambda Function`
      - A Lambda function that sends a html report of the daily rides to a S3 bucket and returns the html body in the lambda handler return.
     - `State Function`
      - A State Function that runs the above Lambda and uses the html body sent from the Lambda. The body is used for a SES V2 send email that sends an email to the ceo of Deloton about the daily rides.
     - `EventBridge Schedule`
      - An EventBridge Schedule that occurs everyday at 9am and activates the State Function to send the previous days data inside a report to the Deloton ceo.
     - `ECR Pipeline`
      - An ECR to store the pipeline container.
     - `Pipeline Task Definition`
      - A task definition that runs the pipeline image.
     - `Pipeline ECS Service`
      - A ECS Service that runs the pipeline Task Definition.
     - `ECR API`
      - An ECR to store the API container.
     - `API Task Definition`
      - A task definition that runs the API image.
     - `API Security Group`
      - Security Group setup for the API to allow access on port 5000.
     - `API ECS Service`
      - A ECS Service that runs the API Task Definition.