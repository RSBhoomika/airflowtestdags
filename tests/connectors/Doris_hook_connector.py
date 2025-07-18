import datetime
import time
from airflow.hooks.base import BaseHook
import pandas as pd
import pymysql
import requests
import io

class DorisHook(BaseHook):
    def __init__(self, conn_id: str = "doris_default"):
        super().__init__()
        self.conn = self.get_connection(conn_id)
        self.host = self.conn.host
        self.port = self.conn.port or 9030
        self.login = self.conn.login
        self.password = self.conn.password
        self.schema = self.conn.schema or "default"
        extra = self.conn.extra_dejson
        self.http_port = extra.get("http_port")

    def get_lookup_table(self, table_name: str):
        conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.login,
            password=self.password,
            database=self.schema
        )
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df

    def stream_load(self, df: pd.DataFrame, table_name: str):
        url = f"http://{self.host}:{self.http_port}/api/{self.schema}/{table_name}/_stream_load"
        csv_data = io.StringIO()
        df.to_csv(csv_data,index=False, header=False, sep='\t')
        csv_data.seek(0)

        # if file_name:
        #     label = f"{table_name}_{file_name.replace('.', '_')}_load"

        # headers = {
        #     "label": f"{table_name}_load",
        #     "Content-Type": "text/csv"
        # }
        headers = {
            "Content-Type": "text/csv"
        }
        
        # Prepare auth with connection password only if password is set else use empty string
        auth = (self.login, self.password) if self.password else (self.login, '')

        response = requests.put(
            url,
            headers=headers,
            data=csv_data.getvalue(),
            auth=auth
        )

        if response.status_code != 200:
            raise Exception(f"Stream load failed: {response.text}")
    
        # Further check if Doris indicates failure even if status code is 200
        response_json = response.json()
        if response_json.get("Status") != "Success":
            raise Exception(f"Stream load failed: {response_json.get('Message')}")
        # Get accurate stats from response
        num_loaded = response_json.get("NumberLoadedRows")
        num_rejected = response_json.get("NumberFilteredRows")
        print(f"Stream load successful! Loaded {num_loaded} rows, rejected {num_rejected} rows into table {table_name}")
        return num_loaded, num_rejected

    def insert_log(self, table_name: str, log_data: dict):
        try:
            conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.login,
                password=self.password,
                database=self.schema
            )
            cursor = conn.cursor()

            # Prepare INSERT SQL query
            columns = ", ".join(log_data.keys())
            values_placeholder = ", ".join(["%s"] * len(log_data))  # %s for placeholders
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder})"
            cursor.execute(insert_query, tuple(log_data.values()))
            conn.commit()

            print(f"Successfully inserted log into table {table_name}")

        except Exception as e:
            print(f"Failed to insert log into table {table_name}: {e}")

        finally:
            cursor.close()
            conn.close()

    def run_sql(self, sql: str):
     """
     Executes a SQL command on the Doris database.
     Returns: query result (for SELECT), else None
     """
     conn = pymysql.connect(
         host=self.host,
         port=self.port,
         user=self.login,
         password=self.password,
         database=self.schema
     )
     try:
         with conn.cursor() as cursor:
             cursor.execute(sql)
             # Fetch result if SELECT
             if sql.strip().lower().startswith("select"):
                 result = cursor.fetchall()
                 # cursor.description gives column names
                 columns = [desc[0] for desc in cursor.description]
                 # Return as list of dicts
                 return [dict(zip(columns, row)) for row in result]
             else:
                 conn.commit()
                 return None
     finally:
         conn.close()
    
    def brokerload_data(self, table_name: str, s3_path: str, columns: str, s3endpoint: str, s3access_key: str, s3secret_key: str, context=None, database: str = ""):
        conn = None
        cursor = None
        try:
            database = database or self.schema
            conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.login,
                password=self.password,
                database=database
            )
            cursor = conn.cursor()
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            label = f"{table_name}_load_{timestamp}"

            query = f"""
            LOAD LABEL {database}.{label} (
                DATA INFILE('{s3_path}')
                INTO TABLE {table_name}
                COLUMNS TERMINATED BY ','
                ({columns})
            )
            WITH S3 (
                "s3.endpoint" = "{s3endpoint}",
                "s3.access_key" = "{s3access_key}",
                "s3.secret_key" = "{s3secret_key}",
                "format" = "csv",
                "s3.use_aws_sdk_default_behavior" = "false",
                "s3.region" = "us-east-1",
                "enable_ssl" = "false",
                "use_instance_profile" = "false",
                "use_path_style_access" = "true",
                "s3.multi_part_size" = "512MB",
                "s3.request_timeout_ms" = "120000"
            )
            PROPERTIES (
                "timeout" = "21600",
                "max_filter_ratio" = "0.1",
                "strict_mode" = "false",
                "load_parallelism" = "16",
                "send_batch_parallelism" = "4",
                "skip_lines" = "0",
                "priority" = "HIGH"
            );
            """

            print(f"Generated LABEL: {label}")
            print(f"Executing query: {query}")
            start_time = time.time()
            cursor.execute(query)
            result = cursor.fetchall()
            end_time = time.time()
            print(f"Execution time: {end_time - start_time:.6f} seconds")

            # Push label to XCom if context is provided
            if context:
                context['ti'].xcom_push(key='load_label', value=label)
                print(f"Pushed to XCom: key='load_label', value='{label}'")

        except Exception as e:
            print(f"[ERROR] Failed to execute load: {e}")
            raise

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
