FROM python:2-alpine
MAINTAINER Simon Jupp "jupp@ebi.ac.uk"

RUN apk update && \
    apk add gcc  && \
    apk add libc-dev  && \
    apk add openssl-dev && \
    apk add libffi-dev

RUN mkdir /app
COPY broker /app/broker
COPY broker/templates /app/templates
COPY broker/static /app/static
COPY broker/broker_app.py requirements.txt /app/
WORKDIR /app

RUN pip install -r /app/requirements.txt

RUN apk del gcc

ENV INGEST_API=http://localhost:8080

EXPOSE 5000
ENTRYPOINT ["python"]
CMD ["broker/broker_app.py"]
