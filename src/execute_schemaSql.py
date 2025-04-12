import mysql.connector
from mysql.connector import Error
from config.mysql_config import get_database_config
from pathlib import Path

DATABASE_NAME = 'github_data'
SQL_FILE_PATH = Path('/home/victo/PycharmProjects/Big_Data_Project/src/schema.sql')


def connect_to_sql(config):
    try:
        connection = mysql.connector.connect(**config)
        return connection
    except Error as e:
        raise Exception(f"---------Can't connect to MySql: {e}-------------") from e

def create_database(cursor,db_name):
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name}')
    print(f'===========Database {db_name} create or exists!================')

def execute_sql_file(cursor,file_path):
    with open(file_path, 'r') as file:
        sql_script = file.read()

    commands = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]

    for command in commands:
        try:
            cursor.execute(command)
            print(f'----------------Execute: {command.strip()[:50]}...---------------')
        except Error as e:
            print(f'Can not execute to sql command: {e}----------------------')


def main():
    try:
        db_config = get_database_config()
        initial_config = {k:v for k,v in db_config.items() if k not in ['database','mysql_url','mysql_driver'] }

        connection = connect_to_sql(initial_config)
        cursor = connection.cursor()

        create_database(cursor,DATABASE_NAME)
        connection.database = DATABASE_NAME

        execute_sql_file(cursor,SQL_FILE_PATH)
        connection.commit()
        print('================Schema.src file executed successfully!===============')
    except Exception as e:
        print(f'---------------Error: {e}----------------')
        if connection and connection.is_connected():
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print('----------------Disconnect to MySQL---------------')
if __name__ == "__main__":
    main()

