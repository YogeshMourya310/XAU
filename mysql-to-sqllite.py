import mysql.connector
import sqlite3
import datetime

# MySQL Database Connection
mysql_conn = mysql.connector.connect(
    host="localhost",
    user="unorootsrm",
    password="Onida@srm101",
    database="erprmwise"
)
mysql_cursor = mysql_conn.cursor(dictionary=True)  # Fetch results as dictionaries

# Fetch data from MySQL
mysql_cursor.execute("SELECT * FROM dailyreportmst_drm")
drm_rows = mysql_cursor.fetchall()

mysql_cursor.execute("SELECT * FROM engineermst_em")
em_rows = mysql_cursor.fetchall()

if not drm_rows:
    print("No data found in DRM table.")
elif not em_rows:
    print("No data found in EM table.")
else:
    # SQLite Database Connection
    sqlite_conn = sqlite3.connect("python-keystone.db")
    sqlite_cursor = sqlite_conn.cursor()

    # Set a password for the SQLite database
    sqlite_cursor.execute("PRAGMA key = 'Keystone@sqllite'")
    print("Password applied.")

    ####################### Identify Datetime Columns in DRM #######################
    drm_columns = list(drm_rows[0].keys())  # Extract column names
    drm_datetime_cols = {col for col in drm_columns if isinstance(drm_rows[0][col], datetime.datetime)}

    # Convert datetime columns to string format
    for row in drm_rows:
        for col in drm_datetime_cols:
            row[col] = row[col].strftime('%Y-%m-%d %H:%M:%S') if row[col] else None

    # Create SQLite table drm if not exists
    create_table_drm = f"""
    CREATE TABLE IF NOT EXISTS drm (
        {', '.join([col + ' TEXT' for col in drm_columns])}
    )
    """
    sqlite_cursor.execute(create_table_drm)
    print("Created DRM table.")

    # Insert data into SQLite
    drm_values = [tuple(row[col] for col in drm_columns) for row in drm_rows]
    insert_query = f"INSERT INTO drm VALUES ({', '.join(['?' for _ in drm_columns])})"
    sqlite_cursor.executemany(insert_query, drm_values)

    ####################### Identify Datetime Columns in EM #######################
    em_columns = list(em_rows[0].keys())  # Extract column names
    em_datetime_cols = {col for col in em_columns if isinstance(em_rows[0][col], datetime.datetime)}

    # Convert only if datetime columns exist in EM
    if em_datetime_cols:
        for row in em_rows:
            for col in em_datetime_cols:
                row[col] = row[col].strftime('%Y-%m-%d %H:%M:%S') if row[col] else None

    # Create SQLite table em if not exists
    create_table_em = f"""
    CREATE TABLE IF NOT EXISTS em (
        {', '.join([col + ' TEXT' for col in em_columns])}
    )
    """
    sqlite_cursor.execute(create_table_em)
    print("Created EM table.")

    # Insert data into SQLite
    em_values = [tuple(row[col] for col in em_columns) for row in em_rows]
    insert_query = f"INSERT INTO em VALUES ({', '.join(['?' for _ in em_columns])})"
    sqlite_cursor.executemany(insert_query, em_values)

    # Commit and Close Connections
    sqlite_conn.commit()
    sqlite_cursor.close()
    sqlite_conn.close()

    print(f"Data transfer complete! {len(drm_rows)} rows inserted in DRM.")
    print(f"Data transfer complete! {len(em_rows)} rows inserted in EM.")

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()
