import os
from os import listdir

def lista_alvos(pasta_alvo,formato):#retorna dict {subpasta:[arquivos]}
    dict_pasta_arquivo = {}
    caminho = os.path.dirname(__file__)+"\\{}\\".format(pasta_alvo)#path deste script python + pasta dados
    subpastas_nome = [f.name for f in os.scandir(caminho) if f.is_dir()]
    for subpasta in subpastas_nome:
        lista_arq = [arq for arq in listdir(caminho+subpasta) if arq[-3:]==formato]
        if len(lista_arq)>0:dict_pasta_arquivo.update({subpasta:lista_arq})
    return dict_pasta_arquivo

def dbc2dbf(arquivos_alvo,pasta_alvo):
    local_atual = os.path.dirname(__file__)+"\\"
    if "blast-dbf.exe" not in listdir(local_atual):print("blast-dbf.exe não está na pasta atual");return
    caminho = local_atual+"{}\\".format(pasta_alvo)

    for subpasta in arquivos_alvo:
        for arquivo in arquivos_alvo[subpasta]:
            input_file = os.path.join(caminho, subpasta, arquivo)
            output_file = input_file.replace('.dbc', '.dbf')
            #if arquivo.replace('.dbc', '.dbf') in listdir(os.path.join(caminho, subpasta)):print("{} já existe".format(output_file));continue#pula para o proximo arquivo
            try:os.system("{}blast-dbf.exe {} {}".format(local_atual,input_file,output_file))
            except: print("Arquivo {} não convertido. Erro {}".format(arquivo,sys.exc_info()))
            else:print("Arquivo {} convertido".format(arquivo))
                    
pasta_alvo = "DADOS"
dbc = "dbc"

dbc2dbf(lista_alvos(pasta_alvo,dbc),pasta_alvo)