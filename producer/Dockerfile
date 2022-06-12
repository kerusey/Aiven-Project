FROM python:latest
WORKDIR /code

COPY ./producer ./
COPY ./service-secrets ../service-secrets

ENV kafka-bootstrap_servers "kafka-2859ed46-keruseyoff-1ead.aivencloud.com:26983"

RUN pip3 install -r requirements.txt
CMD ["python", "producer.py"]