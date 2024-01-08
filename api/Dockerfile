FROM python:3.8-slim

WORKDIR /api

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY api_functions.py .
COPY api_helper_functions.py .
COPY database_functions.py .
COPY api.py .
COPY \static .

EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0"]