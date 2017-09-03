FROM jfloff/alpine-python:2.7-slim
MAINTAINER Simon Jupp "jupp@ebi.ac.uk"

RUN mkdir /app
COPY broker /app/broker
COPY templates /app/templates
COPY app.py requirements.txt /app/
WORKDIR /app

RUN pip install -r /app/requirements.txt

EXPOSE 5001
ENTRYPOINT ["python"]
CMD ["app.py"]