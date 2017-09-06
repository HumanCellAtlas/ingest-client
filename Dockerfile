FROM jfloff/alpine-python:2.7-slim
MAINTAINER Simon Jupp "jupp@ebi.ac.uk"

RUN mkdir /app
COPY broker /app/broker
COPY templates /app/templates
COPY static /app/static
COPY broker-app.py requirements.txt /app/
WORKDIR /app

RUN pip install -r /app/requirements.txt

ENV INGEST_API=http://localhost:8080

EXPOSE 5000
ENTRYPOINT ["python"]
CMD ["broker-app.py"]
