import grpc
import time

import random
import numpy as np
import os
import uuid
from datetime import datetime, timedelta

import argparse
import sys
from concurrent import futures

# Importar os stubs gerados
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import proto.transaction_pb2 as transaction_pb2
import proto.transaction_pb2_grpc as transaction_pb2_grpc

# Constantes
payment_methods = ["PIX", "TED", "DOC", "Boleto"]
estados_uf = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT",
    "MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO",
    "RR","SC","SP","SE","TO"
]

NUM_USERS = 100_000
USER_IDS = [str(uuid.uuid4()) for _ in range(NUM_USERS)]

class TransactionClient:
    def __init__(self, server_address, num_transactions = 100, seed=42):
        self.server_address = server_address
        self.num_transactions = num_transactions
        self.channel = grpc.insecure_channel(server_address)
        self.stub = transaction_pb2_grpc.TransactionServiceStub(self.channel)
        np.random.seed(seed)
        
        # Gerar IDs de usuários
        self.user_ids = USER_IDS
        print(f"Cliente iniciado - Conectado a {server_address}")

    def generate_transaction(self):
        """Gera uma transação aleatória"""
        tx_id = str(uuid.uuid4())
        pagador = self.user_ids[random.randint(0, NUM_USERS-1)]
        recebedor = self.user_ids[random.randint(0, NUM_USERS-1)]
        regiao = np.random.choice(estados_uf)
        modalidade = np.random.choice(payment_methods)
        data_horario = datetime.now() - timedelta(
            seconds=int(np.random.rand() * 86400 * 30)
        )
        valor = np.round(np.random.exponential(scale=1000), 2)
        
        # Criar objeto Transaction para o gRPC
        transaction = transaction_pb2.Transaction(
            id_transacao=tx_id,
            id_usuario_pagador=pagador,
            id_usuario_recebedor=recebedor,
            id_regiao=regiao,
            modalidade_pagamento=modalidade,
            data_horario=data_horario.isoformat(),
            valor_transacao=valor,
            timestamp_envio=int(time.time() * 1000)  # Marca o tempo em milissegundos
        )
        
        return transaction

    def transaction_iterator(self, total_transactions):
        current = 0
        while True:
            print("Current: ", current)
            transaction = self.generate_transaction()
            yield transaction
            current += 1
            if total_transactions == current:
                break


    def sender_thread(self, total_transactions = -1):
        self.stub.SendTransaction(self.transaction_iterator(total_transactions))

    #ANTIGO - PARA TRANSAÇÃO SEM STREAM
    def send_transaction(self, transaction):
        """Envia uma transação para o servidor via gRPC"""
        try:
            response = self.stub.SendTransaction(transaction)
            latency = int(time.time() * 1000) - transaction.timestamp_envio
            print(f"Transação {transaction.id_transacao} enviada - Resposta: {response.success} - Latência: {latency}ms")
            return response
        except grpc.RpcError as e:
            print(f"Erro ao enviar transação: {e.code()}: {e.details()}")
            return None


    def run(self, max_workers=10):
        """Executa o cliente gerando e enviando transações"""
        print(f"Iniciando envio de {self.num_transactions} transações...")
        
        # Usando ThreadPoolExecutor para envio paralelo
        with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            transaction_futures = []
            
            # Gera e submete transações para serem enviadas em paralelo
            for _ in range(self.num_transactions):
                transaction = self.generate_transaction()
                future = executor.submit(self.send_transaction, transaction)
                transaction_futures.append(future)
            
            # Acompanha o progresso
            completed = 0
            for future in futures.as_completed(transaction_futures):
                completed += 1
                if completed % 100 == 0:
                    print(f"Progresso: {completed}/{self.num_transactions} transações enviadas")
        
        print(f"Envio de transações concluído: {self.num_transactions} transações enviadas.")


def main():
    parser = argparse.ArgumentParser(description='Cliente gRPC para envio de transações bancárias')
    parser.add_argument('--server', type=str, default='localhost:50051',
                        help='Endereço do servidor gRPC (padrão: localhost:50051)')
    parser.add_argument('--transactions', type=int, default=1000,
                        help='Número de transações a serem enviadas (padrão: 1000)')
    parser.add_argument('--workers', type=int, default=10,
                        help='Número máximo de workers paralelos (padrão: 10)')
    parser.add_argument('--seed', type=int, default=42,
                        help='Semente para o gerador de números aleatórios (padrão: 42)')
    
    args = parser.parse_args()
    
    # Inicializa e executa o cliente
    client = TransactionClient(
        server_address=args.server,
        num_transactions=args.transactions,
        seed=args.seed
    )
    # client.run(max_workers=args.workers)
    client.sender_thread()


if __name__ == "__main__":
    main() 
