
# Generate TPC-H data and upload to s3

We used this fork of the TPC-H generator: https://github.com/alexandervanrenen/tpch-dbgen

Here is a sample script to generate and upload TPC-H to s3.
The s3 path needs to exist (adjust path!!).
The script generates TPC-H in 10 chunks (to reduce size of each file).
```bash
SF=1000

for i in `seq 1 10`; do
    mkdir -p sf-$SF
    ./dbgen -s $SF -S $i -C 10
    mv *tbl* sf-$SF/
    aws s3 cp sf-$SF/ s3://my-s3-path-to-tpch/data/sf-$SF/ --recursive
    rm -rf sf-$SF
done
```

# Create database and load TPC-H

There is the `setup.py` python script (python3.9).
The script accepts a number of commands to facility the database setup:

```bash
PYTHONPATH=. python3 setup.py [command] [database_url]
```

Here are the command types:
- **test** Checks if the script can connect to the database.
- **create** Creates the schema: one table for each scalefactor: `region_1`, `region_3`, `region_10` etc.
- **drop** Drops all tables.
- **load** Loads data into tables (probably need to adjust s3 path in code).
- **validate** Checks that all tables have the correct number of tuples loaded.
- **profile** Creates performance profile of TPC-H queries. Only required for the matching tool.
