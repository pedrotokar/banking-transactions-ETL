import os
import re
import pandas as pd
import numpy as np
from math import radians, sin, cos, sqrt, atan2

print("Iniciando")

DATA_DIR = "data"
OUTPUT_DIR = os.path.join(DATA_DIR, "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Carrega as informações geográficas das regiões brasileiras (E3)
df_regions = pd.read_csv(os.path.join(DATA_DIR, "regioes_estados_brasil.csv"))

# Lê os dados de usuários a partir do arquivo SQL gerado (E2)
def load_users_from_sql(sql_path: str) -> pd.DataFrame:
    pattern = re.compile(r"INSERT INTO .* VALUES \((.*)\);", re.IGNORECASE)
    rows = []
    with open(sql_path, "r") as f:
        for line in f:
            m = pattern.match(line.strip())
            if m:
                vals = [v.strip() for v in m.group(1).split(", ")]
                uid      = vals[0].strip("'")
                reg      = vals[1].strip("'")
                saldo    = float(vals[2])
                lim_pix  = float(vals[3])
                lim_ted  = float(vals[4])
                lim_doc  = float(vals[5])
                lim_bol  = float(vals[6])
                rows.append((uid, reg, saldo, lim_pix, lim_ted, lim_doc, lim_bol))
    return pd.DataFrame(rows, columns=[
        "id_usuario", "id_regiao_usuario", "saldo",
        "limite_PIX", "limite_TED", "limite_DOC", "limite_Boleto"
    ])

df_users = load_users_from_sql(os.path.join(DATA_DIR, "informacoes_cadastro_100k.sql"))

# Lê as transações brutas (E1)
df_tx = pd.read_csv(
    os.path.join(DATA_DIR, "transacoes_100k.csv"),
    parse_dates=["data_horario"]
).rename(columns={
    "id_regiao": "id_regiao_transacao",
    "id_usuario_pagador": "id_usuario"
})

# T1: Faz o merge entre transações e usuários
t1 = (
    df_tx
    .merge(df_users, on="id_usuario", how="left")
    .assign(
        aprovacao=True,  # começa com todas aprovadas
        id_transacao=lambda df: df["id_transacao"],
    )
    .loc[:, [
        "id_transacao", "id_usuario",
        "modalidade_pagamento", "valor_transacao",
        "saldo", "data_horario",
        "limite_PIX", "limite_TED", "limite_DOC", "limite_Boleto",
        "id_regiao_transacao", "id_regiao_usuario",
        "aprovacao"
    ]]
)
t1.to_csv(os.path.join(OUTPUT_DIR, "T1_base.csv"), index=False)

# T2: Reprova transações cujo valor excede o saldo do usuário
t2 = t1.copy()
mask_insuf = t2["saldo"] < t2["valor_transacao"]
t2.loc[mask_insuf, "aprovacao"] = False
t2.to_csv(os.path.join(OUTPUT_DIR, "T2_saldo.csv"), index=False)

# T3: Reprova transações que excedem o limite da modalidade de pagamento
t3 = t2.copy()
for idx, row in t3.iterrows():
    if row["aprovacao"]:
        lim_col = f"limite_{row['modalidade_pagamento']}"
        if row["valor_transacao"] > row[lim_col]:
            t3.at[idx, "aprovacao"] = False
t3.to_csv(os.path.join(OUTPUT_DIR, "T3_limite.csv"), index=False)

# T4: Adiciona coordenadas geográficas das regiões do usuário e da transação
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

t4 = (
    t1
    .merge(
        df_regions.rename(columns={
            "id_regiao":"id_regiao_transacao",
            "latitude":"lat_transacao",
            "longitude":"lon_transacao"
        }),
        on="id_regiao_transacao", how="left"
    )
    .merge(
        df_regions.rename(columns={
            "id_regiao":"id_regiao_usuario",
            "latitude":"lat_usuario",
            "longitude":"lon_usuario"
        }),
        on="id_regiao_usuario", how="left"
    )
)
t4.to_csv(os.path.join(OUTPUT_DIR, "T4_local.csv"), index=False)

# T5: Calcula score de risco com base na distância entre usuário e local da transação
t5 = t4[["id_transacao","lat_transacao","lon_transacao","lat_usuario","lon_usuario"]].copy()
t5["score_regiao"] = t5.apply(
    lambda r: haversine(r["lat_transacao"],r["lon_transacao"],r["lat_usuario"],r["lon_usuario"]),
    axis=1
)
# Normaliza para ficar entre 0 e 1
t5["score_regiao"] = (t5["score_regiao"] - t5["score_regiao"].min()) / (
    t5["score_regiao"].max() - t5["score_regiao"].min()
)
t5.to_csv(os.path.join(OUTPUT_DIR, "T5_risk_regiao.csv"), index=False)

# T6: Score de risco baseado no valor da transação relativo à média regional
t6 = t1[["id_transacao","valor_transacao","id_regiao_transacao"]].copy()
mean_by_region = df_regions.set_index("id_regiao")["media_transacional_mensal"]
t6["score_valor"] = t6["valor_transacao"] / t6["id_regiao_transacao"].map(mean_by_region)
t6["score_valor"] = (t6["score_valor"] - t6["score_valor"].min()) / (
    t6["score_valor"].max() - t6["score_valor"].min()
)
t6.to_csv(os.path.join(OUTPUT_DIR, "T6_risk_valor.csv"), index=False)

# T7: Score de risco com base na hora do dia (meio-dia = menor risco)
t7 = t1[["id_transacao","data_horario"]].copy()
t7["hour"] = t7["data_horario"].dt.hour
t7["score_horario"] = t7["hour"].apply(lambda h: abs(h - 12) / 12)
t7.to_csv(os.path.join(OUTPUT_DIR, "T7_risk_horario.csv"), index=False)

# T8: Agrega todos os scores de risco e determina aprovação final por risco
tau = 1.0
t8 = t5.merge(t6, on="id_transacao").merge(t7, on="id_transacao")
t8["score_total"] = t8[["score_regiao","score_valor","score_horario"]].sum(axis=1)
t8["aprovacao_risco"] = t8["score_total"] > tau
t8.to_csv(os.path.join(OUTPUT_DIR, "T8_risk_agg.csv"), index=False)

# Exporta arquivos separados com análise de risco por componente
for col,name in [("score_valor","valor"),("score_horario","horario"),("score_regiao","regiao")]:
    df_aux = t8[["id_transacao",col,"aprovacao_risco"]].rename(columns={col:f"score_{name}"})
    df_aux.to_csv(os.path.join(OUTPUT_DIR,f"T8_analise_risco_{name}.csv"), index=False)

# T9: Combina regras determinísticas com decisão baseada em risco (AND lógico)
t9 = (t3[["id_transacao","aprovacao"]]
      .merge(t8[["id_transacao","aprovacao_risco"]], on="id_transacao")
      .assign(aprovacao=lambda df: df["aprovacao"] & df["aprovacao_risco"])
      .drop(columns=["aprovacao_risco"]))
t9.to_csv(os.path.join(OUTPUT_DIR, "T9_combined.csv"), index=False)

# T10: Verifica se algum usuário aprovou mais do que tinha de saldo
t10 = t1[["id_transacao","id_usuario","valor_transacao","saldo"]].merge(t9, on="id_transacao")
soma_por_usuario = t10[t10["aprovacao"]].groupby("id_usuario")["valor_transacao"].sum()
t10 = t10.merge(soma_por_usuario.rename("soma_aprovada"), on="id_usuario", how="left").fillna(0)
t10.loc[t10["soma_aprovada"] > t10["saldo"], "aprovacao"] = False
t10.to_csv(os.path.join(OUTPUT_DIR, "T10_user_check.csv"), index=False)

# T11: Gera os outputs finais (transações e atualização de saldos)

# (1) Salva a decisão final de aprovação por transação
t10[["id_transacao","aprovacao"]].to_csv(
    os.path.join(OUTPUT_DIR, "L1_transacoes_aprovacao.csv"),
    index=False
)

# (2) Atualiza os saldos dos usuários após as transações (sem deixar saldo negativo)
orig_saldo = df_users.set_index("id_usuario")["saldo"]
soma_aprov = soma_por_usuario.reindex(orig_saldo.index).fillna(0)
novo_saldo = orig_saldo - soma_aprov
novo_saldo = novo_saldo.clip(lower=0)  # garante que ninguém fique com saldo negativo

df_users_out = df_users.copy().set_index("id_usuario")
df_users_out["saldo"] = novo_saldo
df_users_out.reset_index().to_csv(
    os.path.join(OUTPUT_DIR, "L2_usuarios_atualizados.csv"),
    index=False
)

print("Pipeline concluída. Resultados salvos em:", OUTPUT_DIR)
