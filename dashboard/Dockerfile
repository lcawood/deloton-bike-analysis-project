FROM python:latest

EXPOSE 4321

WORKDIR /dashboard

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY dashboard.py . 
COPY database.py .
COPY utilities.py .
COPY visualisations.py .

COPY .streamlit /root/.streamlit

ENTRYPOINT ["streamlit", "run", "dashboard.py", "--server.port=4321", "--server.address=0.0.0.0"]
