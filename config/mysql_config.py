from dotenv import load_dotenv
import os
from urllib.parse import urlparse

def get_database_config():
    load_dotenv()

    jdbc_url = os.getenv("DB_URL")
    jdbc_driver = os.getenv("DB_DRIVER")

    parser_url = urlparse(jdbc_url.replace("jdbc:","",1))
    # print(parser_url)
    host = parser_url.hostname
    port = parser_url.port
    database = parser_url.path.strip('/')
    # print(host,port,database)

    user = os.getenv("DB_USER")
    password = os.getenv('DB_PASSWORD')
    # print(user,password)

    return {
        "host": host,
        'port': port,
        'user': user,
        'password' : password,
        'database' : database,
        'mysql_url' : jdbc_url,
        'mysql_driver' : jdbc_driver
    }


