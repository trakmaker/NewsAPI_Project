import snowflake.connector as sf
from dotenv import load_dotenv
import os

def connect_snowflake():
    load_dotenv()  # Loads the .env file
    user = os.environ.get('user')
    password = os.environ.get('password')
    account = os.environ.get('account')

    
    conn = sf.connect(
        user=user,
        password=password,
        account=account
        )
    try:
        conn.cursor().execute("USE WAREHOUSE COMPUTE_WH")
        conn.cursor().execute("USE DATABASE TRAKMAKER")
        print(conn.cursor().execute("SELECT CURRENT_DATABASE()"))
    except Exception as e:
        print(f"Connection failed: {e}")