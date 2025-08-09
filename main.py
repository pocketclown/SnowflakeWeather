import os, json, hashlib
from urllib.parse import urlencode
import requests
import snowflake.connector as sf
from dotenv import load_dotenv

# ---------- env / config ----------
load_dotenv()

def clean(k):
    v = os.getenv(k)
    return v.strip().strip('"').strip("'") if v else None

def sf_conn():
    kwargs = dict(
    account=clean("SNOWFLAKE_ACCOUNT"),
    user=clean("SNOWFLAKE_USER"),
    password=clean("SNOWFLAKE_PASSWORD"),
    warehouse=clean("SNOWFLAKE_WAREHOUSE"),
    database=clean("SNOWFLAKE_DATABASE"),
    schema=clean("SNOWFLAKE_SCHEMA"),
    )
    role = clean("SNOWFLAKE_ROLE")
    if role: kwargs["role"] = role
    return sf.connect(**kwargs)

BASE = os.path.dirname(__file__)

def read_sql(relpath: str) -> str:
    p = os.path.join(BASE, relpath)
    with open(p, "r", encoding="utf-8") as f:
        return f.read()

def sha256(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode()).hexdigest()

# ---------- API fetch ----------
def fetch_open_meteo(lat, lon, tz):
    base = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation",
        "timezone": tz,
    }
    url = f"{base}?{urlencode(params)}"
    r = requests.get(url, timeout=30); r.raise_for_status()
    return url, params, r.json()

# ---------- setup + upsert ----------

def upsert_payload(cs, source, endpoint, params, payload):
    phash = sha256({"source": source, "endpoint": endpoint, "params": params, "payload": payload})
    sql = read_sql("sql/upsert_payload.sql")
    binds = {
        "source": source,
        "endpoint": endpoint,
        "params": json.dumps(params, separators=(",", ":")),
        "payload": json.dumps(payload, separators=(",", ":")),
        "phash": phash,
    }
    cs.execute(sql, binds)

def run_once():
    with sf_conn() as con:
        cs = con.cursor()
        try:
            # optional tiny sanity
            cs.execute("SELECT %(x)s::STRING", {"x": "hello"}); cs.fetchone()
            endpoint, params, data = fetch_open_meteo(40.7128, -74.0060, "America/New_York")
            upsert_payload(cs, "open-meteo", endpoint, params, data)
            con.commit()
            print("Ingested 1 payload into WEATHERLAB.RAW.API_INGEST")
        finally:
            cs.close()

if __name__ == "__main__":
    run_once()