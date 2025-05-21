#!/bin/bash

# Diretório do arquivo proto
PROTO_DIR="../../proto"
OUTPUT_DIR="../../proto"

# Verificar se o diretório existe, se não existir, criar
mkdir -p $OUTPUT_DIR

# Gerar os stubs Python para o arquivo transaction.proto
python -m grpc_tools.protoc \
    -I$PROTO_DIR \
    --python_out=$OUTPUT_DIR \
    --grpc_python_out=$OUTPUT_DIR \
    $PROTO_DIR/transaction.proto

echo "Stubs Python gerados com sucesso!" 