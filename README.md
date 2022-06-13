# Aiven-Project
### This is an entry homework code assignment for Aiven.io
Project stores:
Dockerfiles for producer and consumer worker both
### To run consumer
```shell
git clone https://github.com/kerusey/Aiven-Project
cd Aiven-Project
docker build -f consumer/Dockerfile -t consumer .
docker run consumer
```
### To run producer
```shell
git clone https://github.com/kerusey/Aiven-Project
cd Aiven-Project
docker build -f producer/Dockerfile -t producer .
docker run producer
```

### To run tests for producer
```shell
cd Aiven-Project/producer
pytest tests.py
```
### To run tests for consumer
```shell
cd Aiven-Project/consumer
pytest tests.py
```
It might require setting environments variables (such as bootstrap_servers for kafka)
