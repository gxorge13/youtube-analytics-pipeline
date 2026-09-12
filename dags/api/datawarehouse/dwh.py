from api.datawarehouse.data_util import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from api.datawarehouse.data_loading import load_data
from api.datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from api.datawarehouse.data_transformations import transform_data
import logging 
from airflow.decorators import task


logger = logging.getLogger(__name__)
table = "yt_api"

@task
def staging_table():
    '''
    Questions to ask:
    - 
    '''
    
    schema = "staging"
    conn, cur = None, None
    try:
        conn, cur = get_conn_cursor()
        create_schema(schema)
        create_table(schema) #this is just the mother table, we will be inserting and updating rows in it.
        data = load_data() # this is the raw data, which is a list of dictionaries
        
        curr_ids = get_video_ids(cur, schema) # this is a list of video ids that are CURRENTLY in the staging table. We will use this to determine whether to insert a new row or update an existing row.
        
        for row in data:
            
            # if the video id already exists, we want to update the row. If it doesn't exist, we want to insert a new row.
            if row['video_id'] in curr_ids:
                update_rows(cur, conn, schema, row)
            else:
                insert_rows(cur, conn, schema, row)
        
        # we also want to delete any rows that are in the staging table but not in the new data. This is because the YouTube API might have removed some videos from the playlist, and we want our staging table to reflect that.
        
        # See the ID's in today's json
        new_ids = set([row['video_id'] for row in data])
        curr_ids = set(curr_ids)
        ids_to_delete = curr_ids - new_ids
        
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
        
        logger.info(f"Staging table updated successfully with {len(data)} rows.")
        
    except Exception as e:
        logger.error(f"Error updating staging table: {e}")
        raise e
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)

@task 
def core_table():
    '''
    This task will transform the data in the staging table and load it into the core table. 
    The core table will have a slightly different schema than the staging table, and we will use the transform_data function to transform the data from the staging schema to the core schema.
    '''
        
    schema = "core"
    conn, cur = None, None
    try:
        conn, cur = get_conn_cursor()
        create_schema(schema)
        create_table(schema) #this is just the mother table, we will be inserting and updating rows in it.
        
        cur.execute(f""" SELECT * FROM staging.{table}; """)
        staging_data = cur.fetchall() # this is a list of dictionaries, where each dictionary represents
        
        curr_ids = get_video_ids(cur, schema) # this is a list of video ids that are CURRENTLY in the core table. 
        # We will use this to determine whether to insert a new row or update an existing row.
        
        for row in staging_data:
            transformed_row = transform_data(row) # this will transform the data from the staging schema to the core schema
            
            # if the video id already exists, we want to update the row. If it doesn't exist, we want to insert a new row.
            if transformed_row['Video_ID'] in curr_ids:
                update_rows(cur, conn, schema, transformed_row)
            else:
                insert_rows(cur, conn, schema, transformed_row)
        
        # we also want to delete any rows that are in the staging table but not in the new data. This is because the YouTube API might have removed some videos from the playlist, and we want our staging table to reflect that.
        
        # See the ID's in today's json
        new_ids = set([row['Video_ID'] for row in staging_data])
        curr_ids = set(curr_ids)
        ids_to_delete = curr_ids - new_ids
        
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
        
        logger.info(f"Core table updated successfully with {len(staging_data)} rows.")
        
    except Exception as e:
        logger.error(f"Error updating core table: {e}")
        raise e
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)
    
    