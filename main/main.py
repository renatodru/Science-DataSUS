#definir hospitais de referencia que utilizam prontuario digital
#escopo de procedimentos de alto custo com OPM (verificar se possivel apenas cardiologia)
import down_ftp
import dbc2dbf
import carrega_df
import os
import pandas as pd
from termcolor import colored
import matplotlib as mpl
import matplotlib.pyplot as plt

if __name__ == "__main__":
    os.system("cls")    
    print(colored("BEM-VINDO","green"))
    
    while True:
        print("\n        DIGITE A OPÇÃO ESCOLHIDA PARA CONTINUAR:")
        print("        ",colored("1","blue"),"- CRIA PASTA 'arquivos' E SUBPASTAS PA, RD, SP e rotulos. FAZ DOWNLOAD DE ARQUIVOS DO DATASUS PARA AS SUBPASTAS.")
        print("        ",colored("2","blue"),"- DESCOMPACTA OS ARQUIVOS DBC PARA DBF DAS PASTAS PA, RD E SP.")
        print("        ",colored("3","blue"),"- CARREGAR ARQUIVOS NO DATAFRAME.")
        print("        ",colored("4","blue"),"- VISUALIZAR DADOS E ESTATISTICAS.")
        print("        ",colored("0","yellow"),"- SAIR DO PROGRAMA.")
        cursor = input("\nOPÇÃO: ")
        
        if cursor == "0":
            break
        
        elif cursor == "1":
            print("SERÁ NECESSÁRIO INFORMAR OS SEGUINTES DADOS: DATA_INICIAL, DATA_FINAL, ESTADO, TIPO")
            data_inicial = input("DATA INICIAL (FORMATO AAAA/MM): ")
            data_final = input("DATA FINAL (FORMATO AAAA/MM): ")
            estado = input("ESTADO (PR,SC,RS,SP): ")
            tipo = input("TIPO DO BANCO DE DADOS (PA, RD E SP): ")
            down_ftp.baixaftp(data_inicial,data_final,estado,tipo)
            print("DOWNLOAD CONCLUIDO")
            
        elif cursor == "2":
            pasta_alvo = "arquivos"
            dbc2dbf.dbc2dbf(pasta_alvo)
            
        elif cursor == "3":
            while True:
                print("\nLISTANDO BANCOS DE DADOS DENTRO DA PASTA ARQUIVOS QUE ESTÃO PRONTOS PARA SEREM CARREGADOS: ")
                possibilidades = dbc2dbf.lista_alvos("arquivos", formato='dbf')
                for key,values in possibilidades.items():
                    print("PASTA {}: {}".format(key, values))
                print("""\n       DIGITE O NOME DA PASTA QUE DESEJA CARREGAR: 
                        1 - PA (tempo de carregamento 2min e 30s)
                        2 - RD (tempo de carregamento 5s)
                        3 - SP (tempo de carregamento 30s)
                        4 - BASES DE DADOS PARA RELACIONAMENTO: CID10, CBO, TB_SIGTAP, TCNESBR E MUNICIPIOS (tempo de carregamento 2s)
                        5 - RELACIONAMENTO ENTRE RD E (CID10, CBO, TB_SIGTAP, TCNESBR E MUNICIPIOS):
                        6 - EXCLUI COLUNAS QUE NÃO SERÃO UTILIZADAS (RD)
                        0 - VOLTAR PARA O MENU ANTERIOR.""")
                cursor2 = input("\nOPÇÃO: ")
                dataframe = []
                if cursor2 == "0":
                    break
                
                elif cursor2 == "1":#1 - PA (tempo de carregamento 2min e 30s)
                    for arquivo in possibilidades['PA']:                        
                        dataframe.append(carrega_df.carrega_df('PA', arquivo))                        
                    df_PA = pd.concat(dataframe)
                                    
                elif cursor2 == "2":#2 - RD (tempo de carregamento 5s)
                    for arquivo in possibilidades['RD']:
                        dataframe.append(carrega_df.carrega_df('RD', arquivo))                        
                    df_RD = pd.concat(dataframe)                    
                                    
                elif cursor2 == "3":#3 - SP (tempo de carregamento 30s)
                    for arquivo in possibilidades['SP']:
                        dataframe.append(carrega_df.carrega_df('SP', arquivo))                        
                    df_SP = pd.concat(dataframe)
                                    
                elif cursor2 == "4":#4 - BASES DE DADOS PARA RELACIONAMENTO: CID10, CBO, TB_SIGTAP, TCNESBR E MUNICIPIOS (tempo de carregamento 2s)
                    subpasta = 'rotulos'
                    df_cbo = carrega_df.carrega_df(subpasta, 'CBO.dbf')
                    df_cid10 = carrega_df.carrega_df(subpasta, 'cid10.dbf')
                    df_tb_sigtap = carrega_df.carrega_df(subpasta, 'TB_SIGTAP.dbf')
                    df_TCNESBR = carrega_df.carrega_df(subpasta, 'TCNESBR.dbf')
                    df_municipios = carrega_df.carrega_df(subpasta, 'municipios.xlsx')
                
                elif cursor2 == "5":#5 - RELACIONAMENTO ENTRE RD E (CID10, CBO, TB_SIGTAP, TCNESBR E MUNICIPIOS):
                    df_cid10.rename(columns={'CD_COD':'COD_CID10' ,'CD_DESCR':'DESCR_CID10'}, inplace=True)
                    df_tb_sigtap.rename(columns={'CHAVE':'COD_SIGTAP' ,'DS_REGRA':'DESCR_SIGTAP'}, inplace=True)
                    df_TCNESBR.rename(columns={'CHAVE':'COD_CNES' ,'DS_REGRA':'DESCR_ESTABELECIMENTO'}, inplace=True)
                    
                    df_RD = df_RD.merge(df_cid10, left_on='DIAG_PRINC', right_on='COD_CID10')                    
                    df_RD = df_RD.merge(df_tb_sigtap, left_on='PROC_REA', right_on='COD_SIGTAP')
                    df_RD = df_RD.merge(df_TCNESBR, left_on='CNES', right_on='COD_CNES')
                    
                    lista = ['CO_MUNICDV', 'CO_STATUS', 'IN_CAPITAL', 'IN_AMAZLEG', 'IN_SEMIAR', 'IN_FRONTZN', 'IN_FRONTFX', 'IN_POBREZA', 'DT_INSTAL',
                             'DT_EXTIN', 'CO_SUCESS', 'NU_ORDEM', 'NU_ORDMAP', 'LATITUDE', 'LONGITUDE', 'NU_ALTITUD', 'NU_AREA', 'PAÍS', 'Pais-Estado-Municipio',
                             'Pais - Estado', 'ESTADO MUNICIPIO','CO_REGIAO', 'CO_UF', 'ESTADO', 'NOME-IBGE']
                    df_municipios.drop(lista, axis=1, inplace=True)
                    df_municipios = df_municipios.astype(str)
                    df_RD = df_RD.merge(df_municipios, left_on='MUNIC_RES', right_on='CO_MUNICIP')
                    print("RELACIONAMENTOS (RD) CONCLUIDO")
                    
                elif cursor2 == "6":#EXCLUI COLUNAS QUE NÃO SERÃO UTILIZADAS (RD)
                    lista_colunas = ['UF_ZI', 'ANO_CMPT', 'MES_CMPT', 'ESPEC', 'CGC_HOSP','IDENT','PROC_SOLIC','DIAG_SECUN', 'COBRANCA', 'NATUREZA', 'NAT_JUR',
                                     'GESTAO', 'RUBRICA','IND_VDRL', 'MUNIC_MOV','CPF_AUT', 'HOMONIMO','CID_NOTIF','CONTRACEP1', 'CONTRACEP2', 'GESTRISCO',
                                     'INSC_PN', 'CBOR', 'CNAER', 'VINCPREV', 'GESTOR_COD', 'GESTOR_TP', 'GESTOR_CPF', 'GESTOR_DT','CNPJ_MANT', 'INFEHOSP',
                                     'CID_ASSO', 'REMESSA', 'AUD_JUST', 'SIS_JUST','VAL_SH_FED', 'VAL_SP_FED', 'VAL_SH_GES', 'VAL_SP_GES', 'VAL_UCI\x00CI',
                                     'MARCA_UCI', 'DIAGSEC1', 'DIAGSEC2', 'DIAGSEC3', 'DIAGSEC4', 'DIAGSEC5', 'DIAGSEC6', 'DIAGSEC7', 'DIAGSEC8', 'DIAGSEC9',
                                     'TPDISEC1','TPDISEC2', 'TPDISEC3', 'TPDISEC4', 'TPDISEC5', 'TPDISEC6', 'TPDISEC7', 'TPDISEC8', 'TPDISEC9', 'COD_CID10',
                                     'COD_SIGTAP', 'COD_CNES', 'CO_MUNICIP']
                    df_RD.drop(lista_colunas, axis=1, inplace=True)
                    print("EXCLUSÃO DAS COLUNAS EM EXCESSO CONCLUIDA")
                    print("Se todas as opções foram executadas, voltar e selecionar a opção 4 para gerar os graficos.")
                else:
                    print("OPÇÃO '{}' INVALIDA".format(cursor2))                  
                dataframe = []

        elif cursor == "4":
            print("GERA GRAFICOS")
            
            df_graf = df_RD[(df_RD['MUNIC_RES']=='410690')][(df_RD['COMPLEX']=='03')]#FILTRA O DATAFRAME PARA PEGAR APENAS OS DADOS DO MUNICIPIO DE CURITIBA E ALTA COMPLEXIDADE
            plt.figure(figsize=(16, 10), dpi= 80, facecolor='w', edgecolor='k') #DEFININDO PARAMETROS PARA O GRAFICO
            plt.gca().set(  ylim=(0, 2000000),
                            xlabel='Estabelecimentos',
                            ylabel='Valor')
            plt.xticks(fontsize=12); plt.yticks(fontsize=12)
            plt.title("""
Valor total em milhões de R$ das contas hospitalares de procedimentos de alta complexidade
      de moradores de curitiba agrupados por estabelecimentos Contratados pelo SUS""", fontsize=18)
            categ_sum = df_graf.groupby('DESCR_ESTABELECIMENTO')['VAL_TOT'].sum()#AGRUPA DATAFRAME PELO NOME DO ESTABELECIMENTO E SOMA OS VALORES
            categ_sum.plot.bar()
            plt.show()

        else:
            os.system("cls")
            print("OPÇÃO '{}' INVALIDA!".format(colored(cursor,"red")))
            print(colored("TENTE NOVAMENTE","green"))
            
    print(colored("ENCERRANDO PROGRAMA","red"),"-",colored("BYE","blue"))

