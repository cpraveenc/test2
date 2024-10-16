import json
from pyathenajdbc import connect
from pyathenajdbc.util import as_pandas

def lambda_handler(event, context):
    # Athena configuration
    s3_staging_dir = 's3://my-athena-query-results-bucket/'
    region = 'us-east-1'  # Replace with your AWS region
    database = 'your_database_name'

    # Athena query
    query = "SELECT * FROM your_table LIMIT 10"

    # Path to Athena JDBC driver in Lambda Layer
    jdbc_driver_path = '/opt/lib/AthenaJDBC42.jar'

    # Athena JDBC connection string
    conn_str = f"jdbc:awsathena://AwsRegion={region};S3OutputLocation={s3_staging_dir}"

    try:
        # Connect to Athena via JDBC
        conn = connect(s3_staging_dir=s3_staging_dir, region_name=region, database=database, driver_path=jdbc_driver_path)
        cursor = conn.cursor()

        # Execute query
        cursor.execute(query)
        result = cursor.fetchall()

        # Optionally convert to Pandas DataFrame (useful for further processing)
        # df = as_pandas(cursor)

        # Convert query results to JSON
        result_json = json.dumps(result)

        cursor.close()
        conn.close()

        return {
            'statusCode': 200,
            'body': result_json
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
