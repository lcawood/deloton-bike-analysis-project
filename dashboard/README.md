# Deleton Bike Analysis Dashboard

This folder contains all code and resources required to create a Streamlit dashboard from the data generated in the pipeline file. The files in this folder are used to connect to the database, make visualisations and deploy them to a Streamlit app.

## 🛠️ Getting Setup

- It is recommended before stating any installations that you make a new virtual environment (`venv`).

- A new environment will be required for each folder in this repository.

- Install all requirements for this folder by running `pip3 install -r requirements.txt`.

- Create a `.env` file with the following information:
- `DATABASE_IP` -> ARN to your AWS RDS.
- `DATABASE_NAME` -> Name of your database.
- `DATABASE_USERNAME` -> Your database username.
- `DATABASE_PASSWORD` -> Password to access your database.
- `DATABASE_PORT` -> Port used to access the database.

- You need a database called `postgres`.

## 🏃 Running the dashboard locally

To run locally use the command `streamlit run dashboard.py`

## 📊 Wireframe

Collapsed:
![Dashboard wireframe design (collapsed)](./../diagrams/Dashboard_Wireframe_Collapsed.png)

Expanded:
![Dashboard wireframe design (expanded)](./../diagrams/Dashboard_Wireframe_Expanded.png)
