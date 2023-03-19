import os
from os import listdir
#from cnx_db_tables import conectar

def lista_alvos(pasta_alvo, formato='dbc'):
    dict_pasta_arquivo = {}
    caminho = os.path.join(os.path.dirname(__file__), pasta_alvo)# caminho para a pasta_alvo
    subpastas_nome = [f.name for f in os.scandir(caminho) if f.is_dir()]# verifica se é uma pasta e adiciona na lista
    for subpasta in subpastas_nome:
        # verifica se é um arquivo com formato 'dbc' e adiciona no dicionario
        lista_arq = [arq for arq in listdir(os.path.join(caminho, subpasta)) if arq[-3:] == formato]
        if len(lista_arq) > 0:
            dict_pasta_arquivo.update({subpasta: lista_arq})            
    #dict_pasta_arquivo = limpa_alvos(dict_pasta_arquivo, formato)
    return dict_pasta_arquivo

def limpa_alvos(dict_pasta_arquivo):
    with conectar() as cnx:
        with cnx.cursor() as cursor:
            pass
            

if __name__ == '__main__':
    print(lista_alvos('arquivos'))
    print('limpa_alvos:')
    #print(limpa_alvos(lista_alvos('arquivos')))