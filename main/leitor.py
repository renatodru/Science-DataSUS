import os
from simpledbf import Dbf5
import pandas as pd
from os import listdir
import os


def lista_alvos(pasta_alvo, formato='dbc'):
    dict_pasta_arquivo = {}
    caminho = os.path.join(os.path.dirname(__file__), pasta_alvo)# caminho para a pasta_alvo
    subpastas_nome = [f.name for f in os.scandir(caminho) if f.is_dir()]# verifica se é uma pasta e adiciona na lista
    for subpasta in subpastas_nome:
        # verifica se é um arquivo com formato apontado e adiciona no dicionario
        lista_arq = [arq for arq in listdir(os.path.join(caminho, subpasta)) if arq[-3:] == formato]
        if len(lista_arq) > 0:
            dict_pasta_arquivo.update({subpasta: lista_arq})
    return dict_pasta_arquivo

def carrega_df(subpasta, arquivo, pasta_alvo = 'arquivos'):
    local = os.path.dirname(__file__)    
    caminho_arquivo = os.path.join(local,pasta_alvo,subpasta,arquivo)
    if subpasta == 'rotulos' and arquivo[-3:] == 'dbf':#define a variavel codec para carregamento correto do arquivo dbf
        codec_tipo='cp1250'
    else:
        codec_tipo='cp850'   
    try:#carrega o arquivo dbf e já o converte para dataframe 
        if arquivo[-3:] == 'dbf':
            dataframe = Dbf5(caminho_arquivo, codec=codec_tipo).to_dataframe(na='')
        elif arquivo[-4:] == 'xlsx':
            dataframe = pd.read_excel(caminho_arquivo)
    except Exception as err:
        print('Erro: {}'.format(err))
        return None
    else:
        return dataframe
    
def carregador():
    possibilidades = lista_alvos("arquivos", formato='dbf')
    dataframe = []

    for arquivo in possibilidades['RD']:#procura por outros arquivos do mesmo tipo para concatena-los
        dataframe.append(carrega_df('RD', arquivo))                        
    df_RD = pd.concat(dataframe)#concatena os dataframes em um só
    lista_colunas = ['UF_ZI', 'ANO_CMPT', 'MES_CMPT', 'ESPEC', 'CGC_HOSP','IDENT','PROC_SOLIC','DIAG_SECUN', 'COBRANCA', 'NATUREZA', 'NAT_JUR',
                        'GESTAO', 'RUBRICA','IND_VDRL', 'MUNIC_MOV','CPF_AUT', 'HOMONIMO','CID_NOTIF','CONTRACEP1', 'CONTRACEP2', 'GESTRISCO',
                        'INSC_PN', 'CBOR', 'CNAER', 'VINCPREV', 'GESTOR_COD', 'GESTOR_TP', 'GESTOR_CPF', 'GESTOR_DT','CNPJ_MANT', 'INFEHOSP',
                        'CID_ASSO', 'REMESSA', 'AUD_JUST', 'SIS_JUST','VAL_SH_FED', 'VAL_SP_FED', 'VAL_SH_GES', 'VAL_SP_GES', 'VAL_UCI\x00CI',
                        'MARCA_UCI', 'DIAGSEC1', 'DIAGSEC2', 'DIAGSEC3', 'DIAGSEC4', 'DIAGSEC5', 'DIAGSEC6', 'DIAGSEC7', 'DIAGSEC8', 'DIAGSEC9',
                        'TPDISEC1','TPDISEC2', 'TPDISEC3', 'TPDISEC4', 'TPDISEC5', 'TPDISEC6', 'TPDISEC7', 'TPDISEC8', 'TPDISEC9',  'FAEC_TP',
                        'REGCT', 'RACA_COR', 'ETNIA', 'SEQUENCIA', 'CID_MORTE', 'UTI_MES_IN', 'VAL_SADT', 'VAL_RN', 'VAL_ACOMP', 'VAL_ORTP',
                        'VAL_SANGUE', 'VAL_SADTSR', 'VAL_TRANSP', 'VAL_OBSANG', 'VAL_PED1AC', 'UTI_MES_AN', 'UTI_MES_AL', 'UTI_MES_TO',
                        'MARCA_UTI', 'UTI_INT_IN', 'UTI_INT_AN','UTI_INT_AL', 'UTI_INT_TO', 'DIAR_ACOM']
    df_RD.drop(lista_colunas, axis=1, inplace=True) #removendo colunas que não serão utilizadas
    
    subpasta = 'rotulos'
    #df_cbo = carrega_df(subpasta, 'CBO.dbf')
    df_cid10 = carrega_df(subpasta, 'cid10.dbf')
    df_tb_sigtap = carrega_df(subpasta, 'TB_SIGTAP.dbf')
    df_TCNESBR = carrega_df(subpasta, 'TCNESBR.dbf')
    df_municipios = carrega_df(subpasta, 'municipios.xlsx')

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
    
    
    lista_colunas2 = ['COD_CID10','COD_SIGTAP', 'COD_CNES', 'CO_MUNICIP']
    df_RD.drop(lista_colunas2, axis=1, inplace=True)  #removendo colunas que não serão utilizadas
    return df_RD


if __name__ == '__main__':
    print(carregador())
    