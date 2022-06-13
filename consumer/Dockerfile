FROM python:latest
WORKDIR /code

COPY ./consumer ./
COPY ./service-secrets ../service-secrets

ENV database-name "host-statistics"
ENV database-username "avnadmin"
ENV database-password "AVNS_EAB_pkbi6QBQR882ehD"
ENV database-host "pg-3b73cfc1-keruseyoff-1ead.aivencloud.com"
ENV database-port "26981"

ENV kafka-bootstrap_servers "kafka-2859ed46-keruseyoff-1ead.aivencloud.com:26983"

RUN pip3 install -r requirements.txt
CMD ["python", "Consumer.py"]