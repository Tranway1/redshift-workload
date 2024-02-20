import logging
import psycopg2
import boto3
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS credentials (replace with your credentials or use IAM roles/environment variables)
# aws_access_key_id = 'YOUR_ACCESS_KEY'
# aws_secret_access_key = 'YOUR_SECRET_KEY'

# Redshift connection details
host = 'default-workgroup.126474391297.us-east-1.redshift-serverless.amazonaws.com'
port = 5439
dbname = 'dev'
user = 'admin'

# Initialize a session using your AWS credentials
session = boto3.session.Session(
    # aws_access_key_id=aws_access_key_id,
    # aws_secret_access_key=aws_secret_access_key,
    region_name='us-east-1'  # Replace with your region if different
)

# Create a Secrets Manager client
secrets_manager_client = session.client(service_name='secretsmanager')

# Retrieve the secret
try:
    secret_value = secrets_manager_client.get_secret_value(
        SecretId='arn:aws:secretsmanager:us-east-1:126474391297:secret:redshift!default-namespace-admin-rOvT60'
    )
    admin_password = json.loads(secret_value['SecretString'])['password']
    logger.info("Successfully retrieved admin password from Secrets Manager.")
except Exception as e:
    logger.error(f"Failed to retrieve secret from Secrets Manager: {e}")
    raise

# Establish the connection
try:
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=admin_password,
        host=host,
        port=port,
        connect_timeout=10  # Set a timeout for the connection attempt
    )
    logger.info("Connection to Redshift serverless instance successful.")
except psycopg2.OperationalError as e:
    logger.error(f"Connection to Redshift serverless instance failed: {e}")
except Exception as e:
    logger.error(f"An unexpected error occurred: {e}")
    raise

# If connection is successful, proceed with querying
if conn:
    try:
        with conn.cursor() as cur:
            # Execute a query
            cur.execute('select 1;\n')

            # Fetch the results
            rows = cur.fetchall()

            for row in rows:
                logger.info(row)

            # Commit any changes
            conn.commit()
    except Exception as e:
        logger.error(f"An error occurred while querying: {e}")
    finally:
        # Close the cursor and connection
        conn.close()
        logger.info("Connection closed.")


