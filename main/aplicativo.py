# Desenvolva uma aplicação Python Dockerizada, seguindo o exemplo de dockerfile a seguir: python.zipBaixar python.zip 
# Você deverá desenvolver uma aplicação que você considere desafiadora/criativa, a entrega dessa atividade será um vídeo demonstrando a execução dessa aplicação.
# Nesse vídeo deverá ter as seguintes explicações:
# 1) Explicar a aplicação desenvolvida, justificando os pontos desafiadores ou criativos
# 2) Apresentar o Dockerfile
# 3) Apresentar o Código Fonte
# 4) Apresentar a aplicação em execução.
# O vídeo deverá ser hospedado no youtube como não listado e deverá ter duração de no máximo 2 minutos.

from flask import Flask, request, render_template, Response
from flask_restful import Resource, Api

import matplotlib.pyplot as plt

import io
import base64

import leitor

app = Flask(__name__)
api = Api(app)
class Greeting (Resource):
    @app.route("/aplicativo", methods=['GET','POST'])
    def index():
        complex = request.form['complexidade']
        financ = request.form['financiamento']
        cidade = request.form['cidade']
        if complex != None and financ != None:
            plot_url = figura(complex, financ, cidade)
            imagem = "data:image/png;base64,{}".format(plot_url)
        else:
            imagem="./static/images/blank.png"
        return render_template("index.html",figura=imagem)
        
    @app.route("/", methods=["GET"])
    def local():
        return render_template("index.html",figura="./static/images/blank.png")
    
    @app.route("/health", methods=["GET"])
    def health():
        return Response("OK",status=200)

def figura(complex, financ, cidade):
    df_graf = df_RD[(df_RD['MUNIC_RES']==cidade)][(df_RD['COMPLEX']==complex)][(df_RD['FINANC']==financ)]#FILTRA O DATAFRAME PARA PEGAR APENAS OS DADOS DO MUNICIPIO DE CURITIBA E ALTA COMPLEXIDADE
    categ_sum = df_graf.groupby('DESCR_ESTABELECIMENTO')['VAL_TOT'].sum()#AGRUPA DATAFRAME PELO NOME DO ESTABELECIMENTO E SOMA OS VALORES
    teto = str(round(categ_sum.max()*1.1))
    teto = int(str(int(teto[:1])+1)+len(teto[1:])*'0')    
    plt.figure(figsize=(16, 10), dpi= 80, facecolor='w', edgecolor='k') #DEFININDO PARAMETROS PARA O GRAFICO
    plt.gca().set(  ylim=(0, teto), xlabel='Estabelecimentos', ylabel='Valor')
    plt.xticks(fontsize=12); plt.yticks(fontsize=12)
    plt.title(f"""
    Valor total em R$ das contas hospitalares
    agrupados por estabelecimentos Contratados pelo SUS no estado do Paraná""", fontsize=18)    
    categ_sum.plot.bar()
    plt.tight_layout()
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    plot_url = base64.b64encode(img.getvalue()).decode()    
    plt.close()
    return plot_url

api.add_resource(Greeting, '/') # Rota

if __name__ == '__main__':
    df_RD = leitor.carregador()
    app.run('0.0.0.0',port=3333,debug=True)