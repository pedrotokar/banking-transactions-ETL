import pandas as pd
import random
import numpy as np

N_LINHAS = int(input("Defina o número de linhas do csv mock: "))
#Tabela EMAp
#Colunas:
    #posição da pessoa
    #idade da pessoa
    #ano de ingresso da pessoa
    #salario da pessoa

posicoes_pessoas = ["AlunoGrad", "Professor", "Secretario", "Diretor", "AlunoMesc"]
posicoes_coluna = [random.choice(posicoes_pessoas) for i in range(N_LINHAS)]
idade_coluna = np.random.randint(low = 18, high = 50, size = N_LINHAS)
ano_coluna = np.random.randint(low = 2012, high = 2024, size = N_LINHAS)
salario_coluna = 1000.0 + np.random.uniform(size = N_LINHAS) * 1000.0

dataframe = pd.DataFrame(
    {"posicao": posicoes_coluna,
     "idade": idade_coluna,
     "ano": ano_coluna,
     "salario": salario_coluna}
)

print(dataframe.info())
print(dataframe)
dataframe.to_csv("data/mock_emap.csv", index = False)

posicoes_filtro = ["Professor", "Secretario", "Diretor"]
df_filtrado = dataframe[dataframe["posicao"].isin(posicoes_filtro)]
df_agregados = df_filtrado.groupby("posicao").agg(
    {"idade": "sum",
     "ano": "count"}
).reset_index().rename(
    columns = {"ano": "contagem"}
)
df_agregados["media"] = df_agregados["idade"]/df_agregados["contagem"]

print(df_agregados.info())
print(df_agregados)
df_agregados.to_csv("data/mock_emap_processed.csv", index = False)
