FROM python:3.8.2-slim

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

# Install linux dependencies
RUN apt-get update 
RUN apt-get install -y libssl-dev
RUN apt-get install -y gcc

# RUN apt-get install python-tk python3-tk tk-dev


WORKDIR /app

## -- Enter Virtual Env Installs our requirements into the Docker image
COPY ./requirements.txt .
RUN pip install -r requirements.txt

COPY ./requirements-debug.txt .
RUN pip install -r requirements-debug.txt

# set working directory


