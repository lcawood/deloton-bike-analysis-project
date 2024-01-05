# Terraform
This folder should contain all code and resources required to handle the infrastructure of the project.

# 📝 Project Description
- This folder contains all the terraform code and details that are needed to setup the cloud services required in our project.

## :hammer_and_wrench: Getting Setup

`.tfvars` keys used:

- `database_username`
- `database_password`
- `aws_access_key_id`
- `aws_secret_access_key`
- `database_username`
- `database_password`
- `database_ip`
- `database_port`
- `database_name`

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
     - `Lambda Function`
      - A Lambda function that sends a html report of the daily rides to a S3 bucket and returns the html body in the lambda handler return.
     - `State Function`
      - A State Function that runs the above Lambda and uses the html body sent from the Lambda. The body is used for a SES V2 send email that sends an email to the ceo of deloton about the daily rides.
     - `EventBridge Schedule`
      - An EventBridge Schedule that occurs everyday at 9am and activates the State Function to send the previous days data inside a report to the deloton ceo.