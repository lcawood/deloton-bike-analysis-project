# Terraform
This folder should contain all code and resources required to handle the infrastructure of the project.

# 📝 Project Description
- This folder contains all the terraform code and details that are needed to setup the cloud services required in our project.

## :hammer_and_wrench: Getting Setup

`.tfvars` keys used:

- `database_username`
- `database_password`

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