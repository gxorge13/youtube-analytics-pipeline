import logging

logger = logging.getLogger(__name__)
table = "yt_api"


def insert_rows(cur, conn, schema, row):

    try:

        if schema == "staging":

            video_id = "video_id"

            cur.execute(
                f"""
                INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(video_id)s, %(title)s, %(published_at)s, %(duration)s, %(views)s, %(likes)s, %(comment_count)s);
                """,
                row,
            )

        else:

            video_id = "Video_ID"

            cur.execute(
                f"""
                INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s)
                """,
                row,
            )

        conn.commit()

        logger.info(f"Inserted row with Video_ID: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error inserting row with Video_ID: {row[video_id]} for schema: {schema} - {e}")
        raise e


def update_rows(cur, conn, schema, row):

    try:
        # staging
        if schema == "staging":
            video_id = "video_id"
            upload_date = "published_at"
            video_title = "title"
            video_views = "views"
            likes_count = "likes"
            comments_count = "comment_count"
        # core
        else:
            video_id = "Video_ID"
            upload_date = "Upload_Date"
            video_title = "Video_Title"
            video_views = "Video_Views"
            likes_count = "Likes_Count"
            comments_count = "Comments_Count"

        cur.execute(
            f"""
            UPDATE {schema}.{table}
            SET "Video_Title" = %({video_title})s,
                "Video_Views" = %({video_views})s, 
                "Likes_Count" = %({likes_count})s, 
                "Comments_Count" = %({comments_count})s
            WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
            """,
            row,
        )

        conn.commit()

        logger.info(f"Updated row with Video_ID: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error updating row with Video_ID: {row[video_id]} - {e}")
        raise e


def delete_rows(cur, conn, schema, ids_to_delete):

    try:

        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" = ANY(%s);
            """,
            (list(ids_to_delete),),
        )

        conn.commit()
        logger.info("Deleted %d rows from %s", len(ids_to_delete), schema)

    except Exception as e:
        logger.error("Error deleting rows from %s: %s", schema, e)
        raise e
