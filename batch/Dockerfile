FROM python:3.7-alpine
RUN apk update && apk add redis
RUN pip3 install redis

WORKDIR /app
COPY . .
