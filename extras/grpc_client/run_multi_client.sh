#!/bin/bash

# Configurações padrão
SERVER="localhost:50051"
USERS=1000
TX_PER_CLIENT=1000
WORKERS=10
NUM_CLIENTS=3

# Parse das opções de linha de comando
while [[ $# -gt 0 ]]; do
  case $1 in
    --server)
      SERVER="$2"
      shift 2
      ;;
    --users)
      USERS="$2"
      shift 2
      ;;
    --tx)
      TX_PER_CLIENT="$2"
      shift 2
      ;;
    --workers)
      WORKERS="$2"
      shift 2
      ;;
    --clients)
      NUM_CLIENTS="$2"
      shift 2
      ;;
    *)
      echo "Opção desconhecida: $1"
      exit 1
      ;;
  esac
done

echo "Iniciando $NUM_CLIENTS clientes gRPC em paralelo"
echo "Servidor: $SERVER"
echo "Transações por cliente: $TX_PER_CLIENT"
echo ""

# Executar os clientes em paralelo
for i in $(seq 1 $NUM_CLIENTS); do
  SEED=$((42 + $i))
  echo "Iniciando cliente $i com seed $SEED"
  python3 transaction_client.py --server $SERVER --transactions $TX_PER_CLIENT --workers $WORKERS --seed $SEED &
  
  # Pequeno delay para não sobrecarregar o CPU instantaneamente
  sleep 0.5
done

# Aguardar todos os processos em background terminarem
echo "Todos os clientes foram iniciados. Aguardando conclusão..."
wait

echo "Todos os clientes concluíram a execução." 
