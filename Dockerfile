FROM python:3.4

RUN pip install dumb-init

WORKDIR /usr/src
ADD requirements.txt .
RUN pip install -r requirements.txt
