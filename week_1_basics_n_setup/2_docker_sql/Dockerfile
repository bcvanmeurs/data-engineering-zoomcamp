FROM python:3.11.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app
COPY ingest_data.py ingest_data.py
COPY cmd_docker.sh cmd_docker.sh

# ENTRYPOINT [ "python", "ingest_data.py" ]
