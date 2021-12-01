
lista = list(range(98))
lista_t = len(lista)

rang = 7

passos = lista_t//rang
passos_resto = lista_t%rang

for i in range(passos+1):
    if i == passos+1: print(lista[-passos_resto])
    else:print(lista[rang*i:rang*(i+1)])