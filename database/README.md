# Database
This folder should contain all code and resources required to handle the Database of the project.

# 📝 Project Description
- This folder contains all the Database instructions and details that are needed to setup the Database required in our project.

## :hammer_and_wrench: Getting Setup

`.env` keys used:

- `DATABASE_USERNAME`
- `DATABASE_PASSWORD`
- `DATABASE_IP`
- `DATABASE_PORT`
- `DATABASE_NAME`

## 🏃 Running the script

Run the database creation with `bash reset_db.sh`

## :card_index_dividers: Files Explained
- `deloton_schema.sql`
    - A sql schema that contains all the tables required in the database to store the deloton data.
- `reset_db.sh`
    - A bash script to reset the database. This drops all tables currently in the database and creates them again.