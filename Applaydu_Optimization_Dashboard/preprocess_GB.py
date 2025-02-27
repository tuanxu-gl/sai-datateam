from google.cloud import bigquery
import os

# Set the path to the credentials file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'cre/gcp-gfb-sai-tracking-gold.json'

sqls = [
     "(1) Preprocess data - delete old from ident_campaign.sql",
     "(2) Preprocess data - insert new to ident_campaign.sql",
     "(3) Preprocess data - delete old from kpi_ident_campaign.sql",
     "(4) Preprocess data - insert new to kpi_ident_campaign.sql",
     "02. delete table sum toy unlock and scan.sql",
     "02. insert into table sum toy unlock and scan.sql",
   "03. delete table users.sql",
    "03. insert into table users.sql",
]
# Initialize a BigQuery client
client = bigquery.Client()

# Define the query
for sql in sqls:
    sql_file = 'scripts/preprocess_sql/'+sql
    print('Preprocessing data with %s'%sql_file)

    query = open('scripts/preprocess_sql/'+sql, 'r').read()

    # Run the query
    query_job = client.query(query)

    # Fetch the results
    results = query_job.result()

    # Print the results
    for row in results:
        print(row[0])