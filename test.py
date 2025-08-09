import snowflake.connector as sf
import os
from dotenv import load_dotenv

load_dotenv()

con = sf.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

print(type(con))  # should be <class 'snowflake.connector.connection.SnowflakeConnection'>

cs = con.cursor()
print(type(cs))   # should be <class 'snowflake.connector.cursor.SnowflakeCursor'>

cs.execute("SELECT CURRENT_TIMESTAMP()")  # should work
print(cs.fetchone())

cs.close()
con.close()