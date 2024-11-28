from dotenv import load_dotenv
import os

DB_HOST=os.getenv("DB_HOST"),
DB_NAME=os.getenv("DB_NAME"),
DB_USER=os.getenv("DB_USER"),
DB_PASS=os.getenv("DB_PASS"),
DB_PORT=os.getenv("DB_PORT")

print(DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER)