import psycopg2

_connection = None

def get_connection():
    global _connection
    if not _connection:
        # Enter the values for you database connection
        dsn_database = "data_pier"  # e.g. "compose"
        dsn_hostname = "data-pier-staging.cl8qfdl47mtr.ap-southeast-1.rds.amazonaws.com"  # e.g.: "aws-us-east-1-portal.4.dblayer.com"
        dsn_port = "5432"  # e.g. 11101
        dsn_uid = "data_team"  # e.g. "admin"
        dsn_pwd = "Z1QxYKXO9qjnXQwNVZlmofAWRjMth1nx"  # e.g. "xxx"
        try:
            conn_string = "host=" + dsn_hostname + " port=" + dsn_port + " dbname=" + dsn_database + " user=" + dsn_uid + " password=" + dsn_pwd
            print("Connecting to database\n  ->%s" % (conn_string))
            _connection = psycopg2.connect(conn_string)
            print("Connected!\n")
        except Exception as e:
            print("Unable to connect to the database.%s" % e)
    return _connection


