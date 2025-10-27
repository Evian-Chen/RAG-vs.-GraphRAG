import os, psycopg2
from dotenv import load_dotenv

load_dotenv()
PG_URI = os.getenv("PG_URI") 

csv_path = "../../data/tables/test.csv"

with psycopg2.connect(PG_URI) as conn, conn.cursor() as cur, open(csv_path, "r", encoding="utf-8", newline="") as f:
    cur.copy_expert('COPY public."_p_GameConsume" FROM STDIN WITH (FORMAT csv, HEADER true)', f)

print("COPY done.")
