# Cliente gRPC para Simulação de Transações Bancárias

Este diretório contém a implementação de um cliente gRPC em Python para simulação de transações bancárias.

## Estrutura

- `transaction_client.py`: Cliente gRPC simplificado
- `generate_pb.sh`: Script para gerar os stubs a partir do arquivo .proto
- `requirements.txt`: Dependências necessárias

## Guia de Configuração:

### 1. Instalação de Dependências

Instale as dependências Python necessárias:

```bash
python3 -m pip install -r requirements.txt
```

### 2. Geração dos Stubs Python

Execute o comando para gerar os stubs Python a partir do arquivo .proto:

```bash
python3 -m grpc_tools.protoc -I../../proto --python_out=../../proto --grpc_python_out=../../proto ../../proto/transaction.proto
```

### 3. Correção da Importação nos Stubs Gerados

O protoc gera arquivos com imports relativos que precisam ser corrigidos:

```bash
# Se encontrar "import transaction_pb2", corrija para "import proto.transaction_pb2"
sed -i 's/import transaction_pb2 as transaction__pb2/import proto.transaction_pb2 as transaction__pb2/g' ../../proto/transaction_pb2_grpc.py
```

### 4. Execução do Cliente Básico

```bash
# Execute o cliente básico com parâmetros reduzidos para teste
python3 transaction_client.py --server localhost:50051
```

### 5. Execução de Múltiplos Clientes

```bash
# Torne o script executável
chmod +x run_multi_client.sh

# Execute múltiplos clientes em paralelo com parâmetros reduzidos
./run_multi_client.sh --clients 2
```

## Descrição dos Parâmetros

- `--server`: Endereço do servidor gRPC (padrão: localhost:50051)
- `--seed`: Semente para o gerador de números aleatórios
- `--clients`: Número de clientes a serem executados em paralelo (para o script run_multi_client.sh)

## Observações Importantes

1. **Erro "Connection refused"**: Este erro é esperado se não houver um servidor gRPC em execução na porta especificada.

2. **Modificação Manual dos Stubs**: Sempre que regenerar os stubs, será necessário modificar manualmente o arquivo `transaction_pb2_grpc.py` para corrigir a importação.

