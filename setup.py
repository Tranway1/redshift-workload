
# PYTHONPATH=. python3 setup.py

import src.database as db
import sys
import pandas as pd
import uuid
import time

QUERY_PATH = "tpc-h/queries/"
QUERIES = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "dml_1", "dml_2", "dml_3", "dml_4"]
TABLES = ["region", "nation", "customer", "orders", "lineitem", "partsupp", "supplier", "part"]
SFS = [1, 3, 10, 30, 100, 300, 1000, 3000]
REDSHIFT_WORKGROUP_NAME = "arn:aws:redshift-serverless:us-east-1:126474391297:workgroup/350fb43c-052c-449b-81d1-4a469d8e072f"
REDSHIFT_DATABASE_NAME = "aws1"


def fix_tables(sql, sf):
    for table in TABLES:
        sql = sql.replace(f"$({table})", f"{table}_{sf}")
    return sql


def read_file(file):
    with open(file, 'r') as f:
        return f.read()


def test(conny):
    sql = f"select 1;\n"
    query_id = conny.submit_query(sql)
    # Optionally, wait for the query to finish and get results
    conny.wait_for_query(query_id)
    results = conny.get_query_results(query_id)
    print("test: ok")

def disable_result_cache(conny):
    sql = f'''SET enable_result_cache_for_session TO off;'''
    query_id = conny.submit_query(sql)
    # Optionally, wait for the query to finish and get results
    conny.wait_for_query(query_id)
    results = conny.get_query_results(query_id)
    print(f"disable_result_cache: {results}")

def enable_result_cache(conny):
    sql = f'''SET enable_result_cache_for_session TO on;'''
    query_id = conny.submit_query(sql)
    # Optionally, wait for the query to finish and get results
    conny.wait_for_query(query_id)
    results = conny.get_query_results(query_id)
    print(f"enable_result_cache: {results}")

def drop_schema(conny):
    for sf in SFS:
        print(f"sf={sf}")
        sql = ""
        for table in TABLES:
            sql += f"drop table if exists {table}_{sf} cascade;\n"
        sql = fix_tables(sql, sf)
        query_id = conny.submit_query(sql)
        # Optionally, wait for the query to finish
        conny.wait_for_query(query_id)


def create_schema(conny):
    for sf in SFS:
        print(f"sf={sf}")
        sql = read_file("tpc-h/schema.sql")
        sql = fix_tables(sql, sf)
        query_id = conny.submit_query(sql)
        # Optionally, wait for the query to finish
        conny.wait_for_query(query_id)
        # If you need to retrieve and process results, you can do so here
        # results = conny.get_query_results(query_id)


def load_tables(conny):
    for sf in SFS:
        print(f"sf={sf}")
        for table in TABLES:
            print(f"table={table}")
            sql = f"copy $({table}) from 's3://neural-science/aws/data/sf-{sf}/{table}.' iam_role 'arn:aws:iam::126474391297:role/MyRedshiftRole' delimiter '|';"
            sql = fix_tables(sql, sf)
            query_id = conny.submit_query(sql)
            # Optionally, wait for the query to finish
            conny.wait_for_query(query_id)


def validate(conny):
    def validate_table(conny, sf, table, min, max):
        sql = f"select count(*) from $({table});"
        sql = fix_tables(sql, sf)
        cnt = conny.run_query(sql).iloc[0][0]
        print(f"validating table={table} has={cnt} min={min} max={max}")
        if not (min <= cnt and cnt <= max):
            raise Exception(f"not in bounds")

    for sf in SFS:
        print(f"sf={sf}")
        validate_table(conny, sf, "region", 5, 5)
        validate_table(conny, sf, "nation", 25, 25)
        validate_table(conny, sf, "customer", sf * 150000, sf * 150000)
        validate_table(conny, sf, "orders", sf * 1500000, sf * 1500000)
        validate_table(conny, sf, "lineitem", (sf * 6000000) * 0.99, (sf * 6000000) * 1.01)
        validate_table(conny, sf, "partsupp", sf * 800000, sf * 800000)
        validate_table(conny, sf, "part", sf * 200000, sf * 200000)
        validate_table(conny, sf, "supplier", sf * 10000, sf * 10000)


def profile(conny, host):
    def profile_query(conny, file, sf):
        unique_id = uuid.uuid4()
        sql = read_file(file)
        sql = fix_tables(sql, sf)
        sql = f"/* profiler_id={unique_id} */ {sql}"
        conny.run_query(sql)

        profile_sql = f"""
    select query as query_id,
        NVL(query_cmd_type, 7) as cmd_type,
        user_perm_tables_accessed + volt_temp_tables_accessed as accessed_table_count,
        reported_peak_usage_bytes as memory,
        EXTRACT(ms from (endtime - starttime)) as runtime
    from stl_internal_query_details join stl_query using(query)
    where querytxt like '%{unique_id}%';
    """
        profile = conny.run_query(profile_sql)
        assert len(profile) == 1

        result = {"sql": sql, "file": file, "sf": sf}
        for col in profile.columns:
            result[col] = list(profile[col])[0]
        return result

    # Get queries
    data = []
    for sf in SFS:
        print(f"sf={sf}")
        print(f"running {QUERY_PATH}0_prepare.sql", f"{0}/{len(QUERIES)}")
        conny.run_query(read_file(f"{QUERY_PATH}0_prepare.sql"))
        for idx, query in enumerate(QUERIES):
            file = f"{QUERY_PATH}{query}.sql"
            print(f"running {file}", f"{idx+1}/{len(QUERIES)}")
            data.append(profile_query(conny, file, sf))
    df = pd.DataFrame(data=data)
    df.to_pickle(f"profiles/{host}/profile.pkl")

def submit_all_queries(conny, file_path='workloads/c2_with_queries.pkl'):
    # read queries from pkl file
    queries_df = pd.read_pickle(file_path)
    # sort queries by starttime column
    queries_df = queries_df.sort_values(by=['starttime'])
    min_date = queries_df['starttime'].min()
    max_date = queries_df['starttime'].max()
    print(f"min_date={min_date} max_date={max_date}")

    simulation_start_time = time.time()

    # Assuming the DataFrame has a column named 'query' that contains the SQL queries
    for index, row in queries_df.iterrows():
        sql = row['sql']
        original_query_id = row['query']
        sql_query = f"/* profiler_id={original_query_id} */ {sql}"

        offset_timestamp = (row['starttime'] - min_date).total_seconds()
        # shrink the query waiting time by scale.
        scale=1

        print(f"starttime {row['starttime']} real offset {offset_timestamp} seconds, scale offset {offset_timestamp/scale}" )

        offset_timestamp = offset_timestamp / scale

        sim_offset = (time.time() - simulation_start_time)
        offset_diff = offset_timestamp - sim_offset
        # Kick off the query to Redshift at the corresponding timestamp
        if offset_diff > 0:
            time.sleep(offset_diff)

        # Submit the query using the submit_query method of the DatabaseConnection object
        query_id = conny.submit_query(sql_query,original_query_id)
        # Optionally, you can store the query_id or handle it further
        print(f"Submitted redshift query ID: {query_id} with original query ID: {original_query_id} at offset scaled time {offset_timestamp} sim_offset {sim_offset} offset_diff {offset_diff} ")
    # write submitted_queries to file
    submmited_queries_df = pd.DataFrame(data=conny.submitted_queries, columns=['query_id', 'original_query_id'])
    submmited_queries_df.to_csv(f"workloads/c2_submitted_queries.csv")



def check_all_queries_status(conny):
    statuses = conny.check_queries_status()
    for (query_id, org_query_id, status) in statuses:
        print(f"Query ID {query_id} with original id {org_query_id} status: {status}")

def get_query_results(conny):
    results=[]
    statuses = conny.check_queries_status()
    for (query_id, org_query_id, status) in statuses:
        conny.wait_for_query(query_id)
        result = conny.get_query_results(query_id)
        results.append((query_id, org_query_id, result))
    # write results to file
    results_df = pd.DataFrame(data=results, columns=['query_id', 'original_query_id', 'result'])
    results_df.to_pickle(f"workloads/c2_results.pkl")
    return results

def run_workload(conny):
    submit_all_queries(conny)
    res = get_query_results(conny)
    print(res)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise Exception(f"Usage: {sys.argv[0]} [job] [host_id]")

    job = sys.argv[1]
    print(f"job={job}")

    # Update these with your Redshift database name and workgroup ARN
    conny = db.get_db_connection(REDSHIFT_DATABASE_NAME, REDSHIFT_WORKGROUP_NAME)
    disable_result_cache(conny)

    if job == "test":
        test(conny)
    elif job == "create":
        create_schema(conny)
    elif job == "drop":
        drop_schema(conny)
    elif job == "load":
        load_tables(conny)
    elif job == "validate":
        validate(conny)
    elif job == "profile":
        None
        # profile(conny, host)
    elif job == "submit":
        submit_all_queries(conny)
    elif job == "benchmark":
        run_workload(conny)
    elif job == "check_status":
        check_all_queries_status(conny)
    else:
        raise Exception(f"unknown job {job}")

    print("all done!")
