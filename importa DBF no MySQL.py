import mysql.connector
from mysql.connector import errorcode

import os



#verifica existencia do executavel blast-dbf.exe



localatual = os.path.dirname(__file__)+str('\\')
print("blast-dbf.exe" in listdir(localatual))



for diretorio, subpastas, arquivos in os.walk(localatual):
    pass