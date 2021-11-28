
import os
from os import listdir

def lista_alvos():
    dict_pasta_arquivo = {}
    pasta_atual = os.path.dirname(__file__)+"\\"
    subpastas_nome = [f.name for f in os.scandir(pasta_atual) if f.is_dir()]
    
    for subpasta in subpastas_nome:
        lista_arq = [arq for arq in listdir(pasta_atual+subpasta) if arq[-3:]=="dbf"]
        if len(lista_arq)>0:dict_pasta_arquivo.update({subpasta:lista_arq})
        
    historico = [lista_h[:-1] for lista_h in open(pasta_atual+"DBF_importados_MySQL.txt",'r').readlines()]
    excluir = []
    for x in dict_pasta_arquivo:
        for h in historico:
            if h in dict_pasta_arquivo[x]:dict_pasta_arquivo[x].remove(h)
        if(len(dict_pasta_arquivo[x])==0):excluir.append(x)    
    if(len(excluir)>0):
        for exc in excluir: del dict_pasta_arquivo[exc]    
    return dict_pasta_arquivo


