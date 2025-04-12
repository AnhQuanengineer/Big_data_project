import mysql.connector

db_config = {
    "host" : '172.17.0.2',
    'port' : 3306,
    'user' : 'root',
    'password' : 'anhquan2002'
}

database_name = 'github_data'

sql_file_path = '/src/schema.src'

connection = mysql.connector.connect(**db_config)

cursor = connection.cursor()
cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database_name}')
print(f'===========Database {database_name} create or exists!================')
connection.database = database_name

with open(sql_file_path,'r') as file:
    sql_script = file.read()

sql_commands = sql_script.split(';')

for command in sql_commands:
    if command.strip():
        cursor.execute(command)
        print(f'Execute: {command.strip()[:50]}...')

connection.commit()
print('================Schema.src file executed successfully!===============')