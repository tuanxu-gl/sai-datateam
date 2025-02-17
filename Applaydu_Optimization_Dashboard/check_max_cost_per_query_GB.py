from google.cloud import bigquery
import os
from datetime import datetime



def f_get_cost_from_GB(client):
    
    sql = """select query,total_bytes_billed/(1024*1024*1024) as total_gigabytes_billed
            --from region-us-east4.INFORMATION_SCHEMA.JOBS
            from `gcp-gfb-sai-tracking-gold.applaydu.jobs`
            where project_id = 'gcp-gfb-sai-tracking-gold'
            and date(creation_time) >= '2025-02-10'
            and total_bytes_billed > (10*1024*1024*1024)
            order by total_bytes_billed desc
            ;"""

    # Run the query
    query_job = client.query(sql)

    # Fetch the results
    results = query_job.result()

    # save to csv
    df = results.to_dataframe()
    today = datetime.today().strftime('%Y-%m-%d')
    df.to_csv(f'data/Applaydu-Cost-Per-Query-GB-{today}.csv', index=False)

def main():
    # Set the path to the credentials file
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'cre/gcp-gfb-sai-tracking-gold.json'

    client = bigquery.Client()

        
    #f_update_jobs_table(client)

    f_get_cost_from_GB(client)


main()