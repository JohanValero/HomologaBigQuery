# syntax=docker/dockerfile:1
FROM python:3.10.5-slim-bullseye

#docker build -t flask_app .
#docker image ls
#docker run -p 81:81 flask_app

RUN mkdir wd
WORKDIR /wd

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ./ ./

CMD python3 main.py