FROM python
RUN pip install pandas sqlalchemy psycopg2
COPY titanic.csv /titanic.csv
COPY ingest.py /ingest.py
ENTRYPOINT ["python","/ingest.py"]
