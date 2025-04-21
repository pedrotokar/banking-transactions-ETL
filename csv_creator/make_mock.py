import pandas as pd
import numpy as np
import os
import uuid
from datetime import datetime, timedelta

output_dir = "data/"
os.makedirs(output_dir, exist_ok=True)
np.random.seed(42)  

# Tamanhos de geração
sizes = {
    #"1k": 1_000,
    "100k": 100_000,
    # "1M": 1_000_000
}

payment_methods = ["PIX", "TED", "DOC", "Boleto"]
estados_uf = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT",
    "MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO",
    "RR","SC","SP","SE","TO"
]


# se False, gera limites independentes ou dependentes por modalidade
same_limit_for_all = True


# 1) Tabela 3: Regiões
region_list = []
for uf in estados_uf:
    lat = np.round(-34 + np.random.rand() * 39, 6)
    lon = np.round(-74 + np.random.rand() * 40, 6)
    media_mens = np.round(1_000 + np.random.rand() * 30_000, 2)
    num_fraude = int(np.random.randint(0, 100))
    region_list.append([uf, lat, lon, media_mens, num_fraude])

df_regions = pd.DataFrame(
    region_list,
    columns=[
        "id_regiao",
        "latitude",
        "longitude",
        "media_transacional_mensal",
        "num_fraudes_ult_30d"
    ]
)
regions_csv = os.path.join(output_dir, "regioes_estados_brasil.csv")
df_regions.to_csv(regions_csv, index=False)
print(f"Tabela 3 salva em: {regions_csv}")


for label, N in sizes.items():
    print(f"\n=== Gerando mocks para {label} ({N} usuários/transações) ===")

    # 2) Tabela 2: Usuários (cada id_usuario é único)
    user_ids = [str(uuid.uuid4()) for _ in range(N)]
    usuarios = {
        "id_usuario": user_ids,
        "id_regiao": np.random.choice(estados_uf, size=N),
        "saldo": np.round(np.random.exponential(scale=5000, size=N), 2),
    }

    if same_limit_for_all:
        base_limits = np.round(100 + np.random.exponential(scale=5000, size=N), 2)
        usuarios.update({
            "limite_PIX":       base_limits,
            "limite_TED":       base_limits,
            "limite_DOC":       base_limits,
            "limite_Boleto":    base_limits,
        })
    else:
        usuarios.update({
            "limite_PIX":    np.round(100 + np.random.exponential(scale=5000, size=N), 2),
            "limite_TED":    np.round(100 + np.random.exponential(scale=5000, size=N), 2),
            "limite_DOC":    np.round(100 + np.random.exponential(scale=5000, size=N), 2),
            "limite_Boleto": np.round(100 + np.random.exponential(scale=5000, size=N), 2),
        })

    df_users = pd.DataFrame(usuarios)

    csv_path = os.path.join(output_dir, f"informacoes_cadastro_{label}.csv")
    df_users.to_csv(csv_path, index=False)
    print(f"Tabela 2 (CSV) salva em: {csv_path}")
    # Salva como .sql
    sql_path = os.path.join(output_dir, f"informacoes_cadastro_{label}.sql")
    with open(sql_path, "w") as f:
        f.write(
            "CREATE TABLE IF NOT EXISTS informacoes_cadastro (\n"
            "  id_usuario VARCHAR(36) PRIMARY KEY,\n"
            "  id_regiao VARCHAR(2),\n"
            "  saldo FLOAT,\n"
            "  limite_PIX FLOAT,\n"
            "  limite_TED FLOAT,\n"
            "  limite_DOC FLOAT,\n"
            "  limite_Boleto FLOAT\n"
            ");\n"
        )
        for row in df_users.itertuples(index=False):
            f.write(
                "INSERT INTO informacoes_cadastro "
                "(id_usuario, id_regiao, saldo, limite_PIX, limite_TED, limite_DOC, limite_Boleto) VALUES "
                f"('{row.id_usuario}', '{row.id_regiao}', {row.saldo:.2f}, "
                f"{row.limite_PIX:.2f}, {row.limite_TED:.2f}, {row.limite_DOC:.2f}, {row.limite_Boleto:.2f});\n"
            )
    print(f"Tabela 2 salva em: {sql_path}")

    df_tx = pd.DataFrame({
        "id_transacao":         [str(uuid.uuid4()) for _ in range(N)],
        "id_usuario_pagador":   np.random.choice(user_ids, size=N),
        "id_usuario_recebedor": np.random.choice(user_ids, size=N),
        "id_regiao":            np.random.choice(estados_uf, size=N),
        "modalidade_pagamento": np.random.choice(payment_methods, size=N),
        "data_horario": [
            datetime.now() - timedelta(
                seconds=int(np.random.rand() * 86400 * 365)
            )
            for _ in range(N)
        ],
        "valor_transacao": np.round(np.random.exponential(scale=1000, size=N), 2)
    })
    tx_csv = os.path.join(output_dir, f"transacoes_{label}.csv")
    df_tx.to_csv(tx_csv, index=False)
    print(f"Tabela 1 salva em: {tx_csv}")

    # Validação rápida: quantas transações têm saldo < valor?
    merged = df_tx.merge(
        df_users[["id_usuario","saldo"]], 
        left_on="id_usuario_pagador", right_on="id_usuario"
    )
    n_insuf = (merged["saldo"] < merged["valor_transacao"]).sum()
    print(f"Casos de saldo insuficiente: {n_insuf}/{N}")

print("\nGeração de dados mock concluída.")
