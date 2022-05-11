FROM python:3.9-slim

RUN apt-get update

RUN pip install --upgrade pip

RUN pip install kafka-python

COPY ./write-tweets.py /opt/app/

COPY ./twcs.csv /opt/app/

ENTRYPOINT ["python", "/opt/app/write-tweets.py"]
