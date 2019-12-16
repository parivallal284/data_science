import psycopg2

_connection = None

def get_connection():
    global _connection
    if not _connection:
        # Enter the values for you database connection
        dsn_database = "data_pier"  # e.g. "compose"
        dsn_hostname = "data-pier-production.cl8qfdl47mtr.ap-southeast-1.rds.amazonaws.com"  # e.g.: "aws-us-east-1-portal.4.dblayer.com"
        dsn_port = "5432"  # e.g. 11101
        dsn_uid = "pari"  # e.g. "admin"
        dsn_pwd = "<need to paste actual pwd>"  # e.g. "xxx"
        try:
            conn_string = "host=" + dsn_hostname + " port=" + dsn_port + " dbname=" + dsn_database + " user=" + dsn_uid + " password=" + dsn_pwd
            print("Connecting to database\n  ->%s" % (conn_string))
            _connection = psycopg2.connect(conn_string)
            print("Connected!\n")
        except Exception as e:
            print("Unable to connect to the database.%s" % e)
    return _connection

if __name__ == '__main__':
    get_connection()
