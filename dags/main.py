from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from api.video_stats import get_playlist_ID, get_video_IDs, extract_video_stats, save_to_json
from api.datawarehouse.dwh import staging_table, core_table
# Define the local timezone
local_tz = pendulum.timezone("America/Toronto")

# Default Args
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 2, 1, tzinfo=local_tz),
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

with DAG(
    dag_id='produce_json', # name - ultimately this dag will produce json files under the data directory
    default_args=default_args,
    description='A simple DAG to extract video stats from the YouTube API and save them as JSON files.',
    schedule_interval='0 14 * * *', # run every day at 2 PM (called cron format)
    catchup=False, # if true, when you start the dag, it will run all the past dates since the start date. If false, it will only run from the current date onwards.
) as dag:
    
    #define tasks
    playlist_id = get_playlist_ID()
    video_ids = get_video_IDs(playlist_id)
    extracted_data = extract_video_stats(video_ids)
    save_to_json_task = save_to_json(extracted_data)
    trigger_load = TriggerDagRunOperator(
        task_id="trigger_database_load",
        trigger_dag_id="update_db",
        wait_for_completion=True,
    )
    
    # define dependencies, in what order will the tasks run from  left to right?
    playlist_id >> video_ids >> extracted_data >> save_to_json_task >> trigger_load
    # Thats it! 
    
    # can manually trigger DAG on the airflow web ui. 
    # Find the port from docker-compose and do localhost:port in your browser. 
    # Then find the DAG and click the play button to trigger it. 
    # You can also set it to run on a schedule, which we have done above with the schedule_interval parameter in the DAG definition.
    
    
with DAG (
    dag_id='update_db', # name - ultimately this dag will load the data from the json files into the database
    default_args=default_args,
    description='A simple DAG to load data from JSON files into staging and core tables.',
    schedule_interval=None,
    catchup=False,
) as load_dag:
    
    staging = staging_table()
    core = core_table()
    
    staging >> core
