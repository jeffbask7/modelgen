from dotenv import load_dotenv
import os
load_dotenv()

DB_HOST=os.getenv("DB_HOST")
DB_NAME=os.getenv("DB_NAME")
DB_USER=os.getenv("DB_USER")
DB_PASS=os.getenv("DB_PASS")
DB_PORT=os.getenv("DB_PORT")


filename = f'env_test.txt'
out_str = f"{DB_HOST}, {DB_NAME}, {DB_PASS}, {DB_PORT}, {DB_USER}"
# Write the formatted string to the new file
with open(filename, 'w') as file:
    file.write(out_str)

    #print(DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER)


