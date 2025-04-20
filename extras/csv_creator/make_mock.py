import pandas as pd
import numpy as np
import os
import uuid
import time
from datetime import datetime, timedelta

output_dir = "../../data/"
os.makedirs(output_dir, exist_ok=True)

sizes = {
    "1k": 1_000,
    #"100k": 100_000,
    # "1M": 1_000_000
}

payment_methods = ["PIX", "TED", "DOC", "Boleto"]
estados_uf = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT",
    "MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO",
    "RR","SC","SP","SE","TO"
]

for label, N in sizes.items():
    print(f"\n=== Gerando mock para {label} ({N} registros) ===")
    start_time = time.time()

    # 1) Tabela 1: Transações (salvar em CSV)
    df_transactions = pd.DataFrame({
        "id_transacao":         [str(uuid.uuid4()) for _ in range(N)],
        "id_usuario_pagador":   [str(uuid.uuid4()) for _ in range(N)],
        "id_usuario_recebedor": [str(uuid.uuid4()) for _ in range(N)],
        "id_regiao":            [np.random.choice(estados_uf) for _ in range(N)],
        "modalidade_pagamento": np.random.choice(payment_methods, size=N),
        "data_horario":         [datetime.now() - timedelta(seconds=int(np.random.rand() * 86400 * 365))
                                  for _ in range(N)],
        "valor_transacao":      np.round(np.random.rand(N) * 10_000, 2)
    })
    transactions_csv = os.path.join(output_dir, f"transacoes_{label}.csv")
    df_transactions.to_csv(transactions_csv, index=False)
    print(f"Tabela 1 salva em: {transactions_csv}")

    # 2) Tabela 2: Informações de cadastro (SQJ)
    sql_path = os.path.join(output_dir, f"informacoes_cadastro_{label}.sql")
    with open(sql_path, "w") as f:
        f.write(
            "CREATE TABLE IF NOT EXISTS informacoes_cadastro (\n"
            "  id_usuario VARCHAR(36),\n"
            "  id_regiao VARCHAR(20),\n"
            "  saldo FLOAT,\n"
            "  limite_modalidade FLOAT\n"
            ");\n"
        )
        for _ in range(N):
            user_id = str(uuid.uuid4())
            region_id = np.random.choice(estados_uf)
            saldo = float(np.round(np.random.rand() * 50_000, 2))
            limite = float(np.round(1_000 + np.random.rand() * 20_000, 2))
            f.write(
                f"INSERT INTO informacoes_cadastro "
                f"(id_usuario, id_regiao, saldo, limite_modalidade) VALUES "
                f"('{user_id}', '{region_id}', {saldo:.2f}, {limite:.2f});\n"
            )
    print(f"Tabela 2 salva em: {sql_path}")

    end_time = time.time()
    print(f"Tempo de geração para {label}: {end_time - start_time:.2f} segundos")

# 3) Tabela 3: Regiões (estados do Brasil)
region_list = []
for uf in estados_uf:
    # coordenadas aleatórias
    lat = np.round(-34 + np.random.rand() * 39, 6)
    lon = np.round(-74 + np.random.rand() * 40, 6)
    coord_str = f"({lat}, {lon})"
    media_mens = float(np.round(1_000 + np.random.rand() * 30_000, 2))
    num_fraude = int(np.random.randint(0, 100))
    region_list.append([
        uf,
        coord_str,
        media_mens,
        num_fraude
    ])

df_regions = pd.DataFrame(
    region_list,
    columns=[
        "id_regiao",
        "coordenadas_(lat,lon)",
        "media_transacional_mensal",
        "num_fraudes_ult_30d"
    ]
)

csv_path = os.path.join(output_dir, "regioes_estados_brasil.csv")
df_regions.to_csv(csv_path, index=False)
print(f"\nTabela 3 salva em: {csv_path}")
