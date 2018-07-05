FROM python:3.4.3

ENV DEBIAN_FRONTEND noninteractive
RUN pip install --upgrade pip

RUN mkdir -p /opt/riffyn
COPY . /opt/riffyn/

RUN cd /opt/riffyn && pip install ./kafka-mongo-connector/

WORKDIR /

RUN mongo-connector -t localhost:9092 -d kafka_doc_manager -m mongodb://localhost:27017
