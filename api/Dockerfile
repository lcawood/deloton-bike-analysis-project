FROM python:latest

WORKDIR /api

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . /api

EXPOSE 5000

ENV FLASK_APP=api.py

CMD ["flask", "run", "--host=0.0.0.0"]