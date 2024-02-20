import time

import pandas as pd
import boto3
from botocore.exceptions import ClientError

class DatabaseConnection:
    def __init__(self, database, workgroup):
        self.database = database
        self.workgroup = workgroup
        self.client = boto3.client('redshift-data')
        self.submitted_queries = []

    def submit_query(self, sql, org_query_id=0):
        try:
            response = self.client.execute_statement(
                Database=self.database,
                WorkgroupName=self.workgroup,
                Sql=sql
            )
            query_id = response['Id']
            self.submitted_queries.append((query_id, org_query_id))
            return query_id
        except ClientError as e:
            print("SQL exception")
            print(e)
            print(sql)
            return None

    def check_queries_status(self):
        results = []
        for (query_id, org_query_id) in self.submitted_queries:
            try:
                status_description = self.client.describe_statement(Id=query_id)
                query_status = status_description['Status']
                if query_status in ['FAILED', 'ABORTED']:
                    results.append((query_id, org_query_id, 'FAILED'))
                elif query_status == 'FINISHED':
                    results.append((query_id, org_query_id,'FINISHED'))
                else:
                    results.append((query_id, org_query_id,'RUNNING'))
            except ClientError as e:
                print(f"Error checking status for query {query_id}: {e}")
                results.append((query_id, org_query_id,'ERROR'))
        return results

    def wait_for_query(self, query_id):
        while True:
            status_description = self.client.describe_statement(Id=query_id)
            query_status = status_description['Status']
            if query_status in ['FAILED', 'ABORTED']:
                raise Exception(f"Query {query_id} failed: {status_description.get('Error', 'Unknown error')}")
            elif query_status == 'FINISHED':
                break
            print(f"Query {query_id} is {query_status}")
            time.sleep(1)  # Sleep for a short interval before checking again

    def get_query_results(self, query_id):
        try:
            self.wait_for_query(query_id)  # Ensure the query has finished
            result = self.client.describe_statement(Id=query_id)
            # column_names = [col['name'] for col in result['ColumnMetadata']]
            # Process rows to extract column values
            # rows = [[value for col in record for value in col.values()] for record in result['Records']]
            # return pd.DataFrame(data=rows, columns=column_names)
            # print(result)
            return result
        except self.client.exceptions.ResourceNotFoundException:
            print(f"Query {query_id} does not have a result or is not finished.")
            return None

def get_db_connection(database, workgroup):
    conny = DatabaseConnection(database=database, workgroup=workgroup)
    print("working on", database, "with workgroup", workgroup)
    return conny

if __name__ == "__main__":
    file_path = '../workloads/c2_with_queries.pkl'
    queries_df = pd.read_pickle(file_path)
    # write the first 10 records of queries_df to pickle file
    queries_df_10 = queries_df.iloc[:5]
    queries_df_10.to_pickle('../workloads/c2_with_queries_5.pkl')
    queries_df.to_csv('../workloads/c2_with_queries.csv')
    print(queries_df['sql'][2])