from mysql.connector import pooling, MySQLConnection

def get_sql_connection(config):

    pool = pooling.MySQLConnectionPool(
        pool_name='dev',
        pool_size=3,
        **config
    )

    return pool






class MySqlConn:

    _instance = None
    _initialized = False


    def __new__(cls, **config):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, **config):
        if not self._initialized:

            self._pool = pooling.MySQLConnectionPool(
                pool_name='dev',
                pool_size=3,
                **config
            )

            self._initialized = True