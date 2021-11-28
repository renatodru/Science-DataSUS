#procura os arquivos DBC em uma subpasta e os converte para DBF
import os
from os import listdir

localatual = os.path.dirname(__file__)+str('\\')#path deste script python

#mude o nome da pasta caso necessario
pasta = localatual+"RDPR"#pasta em que os arquivos DFC estão, necessario que seja uma subpasta do diretorio do script python

for diretorio, subpastas, arquivos in os.walk(pasta):
  for arquivo in arquivos:
    if arquivo[-3:]!="dbc":print("{} já existe".format(arquivo));continue
    if "blast-dbf.exe" not in listdir(localatual):print("blast-dbf.exe não está na pasta atual");break
    input_file = os.path.join(diretorio, arquivo)
    output_file = input_file.replace('.dbc', '.dbf')
    try:os.system("{}blast-dbf.exe {} {}".format(localatual,input_file,output_file))
    except: print("Arquivo {} não convertido. Erro {}".format(arquivo,sys.exc_info()))
    else:print("Arquivo {} convertido".format(arquivo))