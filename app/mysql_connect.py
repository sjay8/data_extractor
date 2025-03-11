import pymysql

def get_mysql_connection():
    """
    Establish and return a connection to the MySQL database.
    """
    try:
        connection = pymysql.connect(
            user='root',
            password='DSCI551!',
            host='localhost',
            database='551_final'  # Replace with your actual database name
        )
        return connection
    except pymysql.MySQLError as e:
        print(f"Error connecting to MySQL: {e}")
        return None


