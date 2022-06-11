FROM python:latest
WORKDIR /code

COPY workers/requirements.txt ./

RUN pip install -r requirements.txt
CMD ["python3", "./consumer.py"]