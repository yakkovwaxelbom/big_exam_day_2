from fastapi import Request, FastAPI
from mysql.connector.pooling import MySQLConnectionPool, MySQLConnection


def get_sql_conn(r: Request):

    pool:MySQLConnectionPool =  r.app.state.mysql_pool

    return pool.get_connection()

