import psycopg2 
import pandas as pd

conn_params = {
    'dbname': 'de_stocks',
    'user': 'adminuser',
    'password': 'adminuser',
    'host': 'localhost',  # or the IP address of the PostgreSQL server
    'port': 5432  # default port for PostgreSQL
}

def db_connect(func):
    def wrapper(*args, **kwargs):
        result = None
        try:
            conn = psycopg2.connect(**conn_params)
            cur = conn.cursor()
            result = func(cur,conn, *args, **kwargs)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            conn.close()
        return result
    return wrapper


@db_connect
def create_table(cur,conn,sql=None):
    if not sql:
        sql = """
            CREATE TABLE IF NOT EXISTS stock_data(
                                    ID  SERIAL PRIMARY KEY,
                                    OPEN varchar(255) ,
                                    CLOSE varchar(255) ,
                                    HIGH varchar(255) ,
                                    LOW varchar(255) ,
                                    DATE TIMESTAMP ,
                                    VOLUME varchar(255) 
                                    )
            """
    cur.execute(sql)



@db_connect
def insert_data(cur,conn,sql=None,data=None,date=None):
    if not sql:
        sql = f"""
            INSERT INTO stock_data 
                    (open,close,high,low,date,volume) VALUES
                    ({data.get('1. open')},{data.get('2. high')},{data.get('3. low')},{data.get('4. close')},'{date}', {data.get('5. volume')});                      
            """
    cur.execute(sql)


@db_connect
def fetch_all_records(cur,conn,sql=None):
    if not sql:
        sql = "SELECT * FROM stock_data"
    cur.execute(sql)
    return cur.fetchall()

@db_connect
def fetch_single_record(cur,conn,sql=None, date=None):
    if not sql:
        sql = f"SELECT * FROM stock_data WHERE date='{date}'"
    cur.execute(sql)
    return cur.fetchone()


@db_connect
def get_df_data(cur,conn,sql=None):
    if not sql:
        sql = "SELECT * FROM stock_data"

    return pd.read_sql_query(sql,conn)


    
