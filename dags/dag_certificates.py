import os
import tempfile
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.ssh.hooks.ssh import SSHHook
import snowflake.connector

DEFAULT_ARGS = {
    'owner': 'centectdf',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True,
}

@dag(
    dag_id='sync_certificates_to_snowflake',
    default_args=DEFAULT_ARGS,
    description='Incrementally extracts PDFs, loads them to Stage, and runs UDF to extract text',
    schedule='@daily',
    start_date=datetime(2025, 12, 1),
    catchup=False,
    max_active_runs=1,
    tags=['certificates', 'snowflake', 'ingestion']
)
def sync_certificates_to_snowflake():

    @task
    def extract_load_and_parse_missing_pdfs():
        remote_dir = '/var/www/html/storage/app/certificados'
        stage_name = '@CERTIFICATES_STAGE'
        
        ssh_hook = SSHHook(ssh_conn_id='ubuntu_lab_ssh')
        
        sf_conn = None
        cursor = None

        try:
            # Connection is established within the try block for proper error handling
            sf_conn = snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                role=os.getenv('SNOWFLAKE_ROLE', 'AIRFLOW_ROLE'), # Toma la del compose
                warehouse='COMPUTE_WH',
                database='ISO17025_AUDIT_SYSTEM',
                schema='RAW_DATA'
            )
            cursor = sf_conn.cursor()

            # 1. Fetch existing files in Snowflake Stage
            print(f"Fetching existing files from {stage_name}...")
            cursor.execute(f"LIST {stage_name}")
            sf_existing_files = {os.path.basename(row[0]) for row in cursor.fetchall()}
            
            with tempfile.TemporaryDirectory() as local_tmp_dir:
                with ssh_hook.get_conn() as ssh_client:
                    sftp = ssh_client.open_sftp()
                    
                    # 2. Fetch remote file list
                    remote_files = sftp.listdir(remote_dir)
                    pdf_files = [f for f in remote_files if f.lower().endswith('.pdf')]
                    
                    # 3. Filter missing files
                    files_to_process = [f for f in pdf_files if f not in sf_existing_files]
                    
                    if not files_to_process:
                        print("No new files to process. Exiting cleanly.")
                        return "0 new files."

                    total_new = len(files_to_process)
                    print(f"To process: {total_new} new files.")

                    successful_uploads = []

                    # 4. Process and upload incrementally
                    for i, pdf in enumerate(files_to_process, 1):
                        remote_path = f"{remote_dir}/{pdf}"
                        local_path = os.path.join(local_tmp_dir, pdf)
                        
                        try:
                            sftp.get(remote_path, local_path)
                            
                            put_command = f"PUT file://{local_path} {stage_name} AUTO_COMPRESS=FALSE"
                            cursor.execute(put_command)
                            
                            os.remove(local_path)
                            successful_uploads.append(pdf)
                            
                        except Exception as e:
                            print(f"Failed to process {pdf}: {str(e)}")

            
            if successful_uploads:
                print(f"Running Python UDF on {len(successful_uploads)} new files...")
                
                # Refresh the directory so Snowflake detects the new files
                cursor.execute(f"ALTER STAGE {stage_name.replace('@', '')} REFRESH")

                # Build a tuple of strings to filter the SQL query
                if len(successful_uploads) == 1:
                    files_tuple = f"('{successful_uploads[0]}')"
                else:
                    files_tuple = str(tuple(successful_uploads))

                # Insert ONLY the files we just uploaded using the UDF
                insert_query = f"""
                INSERT INTO CERTIFICATES_TEXT_BASE (pdf_filename, file_date, raw_text)
                SELECT 
                    RELATIVE_PATH,
                    LAST_MODIFIED,
                    EXTRACT_TEXT_PDF(BUILD_SCOPED_FILE_URL({stage_name}, RELATIVE_PATH))
                FROM DIRECTORY({stage_name})
                WHERE RELATIVE_PATH IN {files_tuple}
                """
                cursor.execute(insert_query)
                print("UDF processing complete and data inserted into CERTIFICATES_TEXT_BASE.")

            summary_msg = f"Sync complete. Uploaded and parsed {len(successful_uploads)} files."
            return summary_msg

        finally:
            if cursor:
                cursor.close()
            if sf_conn:
                sf_conn.close()

    extract_load_and_parse_missing_pdfs()

dag = sync_certificates_to_snowflake()