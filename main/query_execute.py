from cnx_db_tables import conectar
from dateutil import parser
import pandas as pd
import sys
#import os

#COMPETENCIA, DATA_MODIFICACAO_ARQ, DATA_OPERACAO, OPERACAO, MODULO_EXECUTANTE
def query_historico(lista_acao):    
    "insere registros na tabela historico do banco de dados"
    with conectar() as cnx:
        with cnx.cursor() as cursor:
            for hist in lista_acao:
                try:                           
                    cursor.execute("INSERT INTO historico (COMPETENCIA, DATA_MODIFICACAO_ARQ, DATA_OPERACAO, OPERACAO, MODULO_EXECUTANTE) VALUES ('{}', '{}', '{}', '{}', '{}')".format(hist[0], parser.parse(hist[1]), parser.parse(hist[2]), hist[3], hist[4]))
                except Exception as err:
                    print("ERRO AO EXECUTAR NO BANCO DE DADOS: {}".format(err))
        cnx.commit()
        print("Registro inserido com sucesso")

def query_consulta_historico(lista_itens):
    with conectar() as cnx:
        with cnx.cursor() as cursor:
            dados = []
            for item in lista_itens:
                try:
                    cursor.execute("{}".format(item))
                except Exception as err:
                    print("ERRO AO EXECUTAR NO BANCO DE DADOS: {}".format(err))
                else:
                    #dados.append()
                    pass
        cnx.commit()
    return dados

def query_execute():
    """executa query no banco de dados"""
    with conectar() as cnx:
        with cnx.cursor() as cursor:
            try:
                cursor.execute("UPDATE historico SET operacao='dbc2dbf_convertido' WHERE operacao='CONVERTIDO'")
            except Exception as err:
                print("ERRO AO EXECUTAR NO BANCO DE DADOS: {}".format(err))
        cnx.commit()
        print("Registro atualizado com sucesso")
        
def query_select():
    """executa query no banco de dados"""
    with conectar() as cnx:
        with cnx.cursor() as cursor:
            try:
                select = cursor.execute("SELECT * FROM historico WHERE operacao='download-dbc'")
                print(select.fetchall())
                #df = pd.read_sql_query('select * from "Stat_Table"',con=engine)
                #dataframe = psql.read_sql('SELECT * FROM product_product', connection)
                #df = pd.read_sql('select * from Stat_Table', con=engine)
            except Exception as err:
                print("ERRO AO EXECUTAR NO BANCO DE DADOS: {}".format(err))
        cnx.commit()

def verifica_dt_comp_pgsql(comp, dt_mod_arq):
    #verifica se a competencia ja existe no banco de dados
    #verifica a data de alteração do arquivo que importou a competencia no banco de dados
    #retorna true apenas se a data informada for maior do que a alteração do arquivo no banco de dados
    #retorna false se a competencia não existir no banco de dados    
         
    with conectar() as cnx:
        with cnx.cursor() as cursor:
            try:
                #conta a quantidade de linhas de acordo com o where informado
                qnt_linhas = cursor.execute("SELECT COUNT(*) FROM {} WHERE ANO_CMPT='{}' and MES_CMPT='{}'".format(comp[:2], "20"+comp[5:7], comp[7:9]))
                #se a quantidade de linhas for maior que 0, a competencia existe no banco de dados
            except Exception as err:
                nome_funcao = sys._getframe().f_code.co_name
                print("ERRO AO EXECUTAR FUNÇÃO {} NO BANCO DE DADOS: {}".format(nome_funcao,err))
                exit(1)
            else:
                if qnt_linhas == 0:
                    return False
                elif qnt_linhas > 0:
                    pass
                    
                

if __name__ == '__main__':
    # query_historico([['201801', '2018-01-01 00:00:00', 'teste_funcao'],
    #            ['201802', '2018-02-01 00:00:00', 'teste_funcao'],
    #            ['201803', '2018-03-01 00:00:00', 'teste_funcao']])

    query_select()    