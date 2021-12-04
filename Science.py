import mysql.connector
from mysql.connector import errorcode
import os
from os import listdir
import pandas as pd

def conecta_db():
    config = {'user':'root','password':'root','host':'localhost','port':'3306','raise_on_warnings':True}
    try:cnx = mysql.connector.connect(**config)#user='root',password='root',host='127.0.0.1',port='3306',raise_on_warnings=True
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:print("Senha ou usuario incorretos")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:print("Database não existe")
        else:print(err)
    else:print("Conectado em {}:{}, usuario: {}".format(config['host'],config['port'],config['user']))
    return cnx


cnx = conecta_db()
try:cursor = cnx.cursor()
except:print("Erro definindo cursor")
database = 'DATASUS'
cursor.execute("USE {}".format(database))
cursor.execute('SELECT * FROM RDPR WHERE CNES = "2384299"')

table_rows = cursor.fetchall()

pd.read

df = pd.DataFrame(table_rows)
print(df)

cursor.close()
cnx.close()
