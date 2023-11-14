FROM python:3.8-slim-buster

RUN mkdir /app
WORKDIR /app

RUN pip install pika==1.3.2

COPY ./src/ /app/

CMD ["python", "-u", "-m", "pikaapp.main"]
