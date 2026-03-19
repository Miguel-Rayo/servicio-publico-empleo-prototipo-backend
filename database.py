# database.py
# import mysql.connector
# from mysql.connector import pooling
# import os
# from dotenv import load_dotenv

# load_dotenv()

# pool = pooling.MySQLConnectionPool(
#     pool_name="empleo_pool",
#     pool_size=5,
#     host=os.getenv("DB_HOST"),
#     user=os.getenv("DB_USER"),
#     password=os.getenv("DB_PASSWORD"),
#     database=os.getenv("DB_NAME"),
#     charset="utf8mb4"
# )

# def get_connection():
#     return pool.get_connection()

# database.py
import mysql.connector
from mysql.connector import pooling
import os
from dotenv import load_dotenv

load_dotenv()

pool = pooling.MySQLConnectionPool(
    pool_name="empleo_pool",
    pool_size=5,
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
    port=int(os.getenv("DB_PORT", 3306)),
    charset="utf8mb4",
    ssl_disabled=False,
    ssl_verify_cert=False,
)

def get_connection():
    return pool.get_connection()