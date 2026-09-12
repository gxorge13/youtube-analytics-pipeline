from airflow.providers.postgres.hooks.postgres import PostgresHook

from psycopg2.extras import RealDictCursor

table = 'yt_api'

def get_conn_cursor():
    pg_hook = PostgresHook(postgres_conn_id='postgres_db_yt_elt', database='elt_db')
    con = pg_hook.get_conn()
    cursor = con.cursor(cursor_factory=RealDictCursor)
    return con, cursor

def close_conn_cursor(con, cursor):
    cursor.close()
    con.close()
    
def create_schema(schema):
    con, cursor = get_conn_cursor()
    cursor.execute(f"""
        CREATE SCHEMA IF NOT EXISTS {schema};
    """)
    
    con.commit()
    
    close_conn_cursor(con, cursor)
    
def create_table(schema):
    con, cursor = get_conn_cursor()
    
    if schema == "staging":
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
            );
        """
    else:
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" TIME NOT NULL,
                "Video_Type" VARCHAR(10) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
            );
        """
    cursor.execute(table_sql)
    con.commit()
    
    close_conn_cursor(con, cursor)
    
def get_video_ids(cur, schema):
    cur.execute(f"""
        SELECT "Video_ID" FROM {schema}.{table};
    """)
    ids = cur.fetchall()
    # Will give us ids = [{'Video_ID': 'id1'}, {'Video_ID': 'id2'}, ...]
    
    video_ids = [id['Video_ID'] for id in ids]
    
    return video_ids