import sqlite3
connection = sqlite3.connect('resource.db')
cursor = connection.cursor()
with open('resource.sql') as sql_file:
    cursor.executescript(sql_file.read())
connection.commit()
connection.close()
