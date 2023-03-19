import DB_STRUCT
import psycopg

DB_NAME = 'datasus'

def conectar():
    global DB_NAME
    config = {'user':'postgres','password':'qweasd123','host':'127.0.0.1','port':'5432','dbname':DB_NAME,'autocommit':True}
    try:
        cnx = psycopg.connect(**config)
    except psycopg.Error as err:
        print(err)
        try:
            config['dbname'] = 'postgres'
            cnx = psycopg.connect(**config)
            create_database(cnx, DB_NAME)
            cnx.close()
            config['dbname'] = DB_NAME
        except psycopg.Error as err:
            print(err)
            exit(1)
        else:
            cnx = psycopg.connect(**config)
    finally:
        print("Conected on {}:{}/{} - user: {}".format(config['host'],config['port'],config['dbname'],config['user']))
        return cnx

def create_database(cnx, DB_NAME):
    if DB_NAME in bancos_in_pg(cnx):
        print('Database {} already exists'.format(DB_NAME))
        return True
    cursor = cnx.cursor()
    try:
        cursor.execute("CREATE DATABASE {}".format(DB_NAME))
    except psycopg.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)
    else:
        print('Database {} created'.format(DB_NAME))
        cursor.close()
        return True
        
def bancos_in_pg(cnx):
    try:
        bancos = [banco[0] for banco in cnx.cursor().execute('SELECT datname FROM pg_database').fetchall()]          
    except psycopg.Error as err:
        print(err)
        exit(1)
    else:
        return bancos

def create_tables(cnx, TABLES):
    cursor = cnx.cursor()
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except psycopg.Error as err:
            print(err)
        else:print("OK")
    cursor.close()
    
#a função recreate_tables apaga as tabelas e as cria novamente
def recreate_tables(cnx, TABLES):
    cursor = cnx.cursor()
    cursor.execute("drop schema public cascade")
    cursor.execute("CREATE SCHEMA public")
    create_tables(cnx, TABLES)
    cursor.close()
    print("Tabelas Recriadas")
    

if __name__ == '__main__':    
    cnx = conectar()
    TABLES = DB_STRUCT.TABLES
    create_database(cnx, DB_NAME)
    create_tables(cnx, TABLES)
    recreate_tables(cnx, TABLES)
    cnx.close()