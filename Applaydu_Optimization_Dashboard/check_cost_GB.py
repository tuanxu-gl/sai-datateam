from google.cloud import bigquery
import os
from datetime import datetime

def f_get_max_date_from_jobs_table_sql(client):
    query = "select max(date(creation_time)) from `gcp-gfb-sai-tracking-gold.applaydu.jobs`"
    query_job = client.query(query)
    results = list(query_job.result())
    for row in results:
        if row[0] is not None:
            return row[0].strftime('%Y-%m-%d')
    return None
def f_update_jobs_table(client):
    max_date = f_get_max_date_from_jobs_table_sql(client)
    update_sql = """insert into `gcp-gfb-sai-tracking-gold.applaydu.jobs` 
        (
            select *
            from 
                `region-us-east4.INFORMATION_SCHEMA.JOBS`
            where 
                project_id = 'gcp-gfb-sai-tracking-gold'
                and date(creation_time) > 'istart_date'
        
        );"""
    update_sql = update_sql.replace('istart_date', max_date)

    # Run the query
    query_job = client.query(update_sql)

    # Fetch the results
    results = query_job.result()

    # Print the results
    for row in results:
        print(row[0])

def f_get_cost_from_GB(client):
    max_date = f_get_max_date_from_jobs_table_sql(client)
    sql = """select 
            date(creation_time) as creation_date,
            format("%.2f", sum(total_bytes_processed) / (1024*1024*1024)) as total_gigabytes_processed,
            format("%.2f", sum(total_bytes_billed) / (1024*1024*1024)) as total_gigabytes_billed,
            format("%.2f", sum(total_bytes_billed) / (1024*1024*1024*1024) * 5) as total_euros_billed
        from 
            `gcp-gfb-sai-tracking-gold.applaydu.jobs`
        group by 
            creation_date
        order by 
            creation_date asc;"""

    # Run the query
    query_job = client.query(sql)

    # Fetch the results
    results = query_job.result()

    # save to csv
    df = results.to_dataframe()
    today = datetime.today().strftime('%Y-%m-%d')
    df.to_csv(f'data/Applaydu-Cost-GB-{today}.csv', index=False)

def main():
    # Set the path to the credentials file
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'cre/gcp-gfb-sai-tracking-gold.json'

    client = bigquery.Client()

        
    f_update_jobs_table(client)

    f_get_cost_from_GB(client)


main()