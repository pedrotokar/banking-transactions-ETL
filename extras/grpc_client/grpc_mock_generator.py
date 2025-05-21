import grpc
import time
import pandas as pd
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

class MockTransactionGenerator:
    def __init__(self, server_address, num_users=1000, seed=42):
        """
        Inicializa o gerador de transações simuladas como cliente gRPC
        
        Args:
            server_address: Endereço do servidor gRPC (host:porta)
            num_users: Número de usuários a serem gerados
            seed: Semente para o gerador de números aleatórios
        """
        self.server_address = server_address
        self.num_users = num_users
        self.channel = grpc.insecure_channel(server_address)
        self.stub = transaction_pb2_grpc.TransactionServiceStub(self.channel)
        np.random.seed(seed)
        
        print(f"Inicializando gerador de transações com {num_users} usuários")
        print(f"Conectado ao servidor gRPC: {server_address}")
        
        # Gerar usuários
        self.generate_users()
    
    def generate_users(self):
        """Gera os usuários para as transações"""
        self.user_ids = [str(uuid.uuid4()) for _ in range(self.num_users)]
        self.user_regions = np.random.choice(estados_uf, size=self.num_users)
        self.user_balances = np.round(np.random.exponential(scale=5000, size=self.num_users), 2)
        
        # Limites para diferentes modalidades
        base_limits = np.round(100 + np.random.exponential(scale=5000, size=self.num_users), 2)
        self.user_limits = {
            "PIX": base_limits,
            "TED": base_limits,
            "DOC": base_limits,
            "Boleto": base_limits,
        }
        
        print(f"Usuários gerados: {self.num_users}")
    
    def generate_transaction(self):
        """Gera uma transação aleatória"""
        tx_id = str(uuid.uuid4())
        pagador_idx = np.random.randint(0, self.num_users)
        recebedor_idx = np.random.randint(0, self.num_users)
        
        pagador = self.user_ids[pagador_idx]
        recebedor = self.user_ids[recebedor_idx]
        regiao = self.user_regions[pagador_idx]
        modalidade = np.random.choice(payment_methods)
        
        # Gerar data e hora aleatória dos últimos 30 dias
        data_horario = datetime.now() - timedelta(
            seconds=int(np.random.rand() * 86400 * 30)
        )
        
        # Gerar valor aleatório baseado em distribuição exponencial
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
    
    def generate_and_send(self, num_transactions, max_workers=10):
        """Gera e envia várias transações para o servidor"""
        print(f"Gerando e enviando {num_transactions} transações...")
        
        # Usando ThreadPoolExecutor para envio paralelo
        with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            transaction_futures = []
            
            # Gera e submete transações para serem enviadas em paralelo
            for _ in range(num_transactions):
                transaction = self.generate_transaction()
                future = executor.submit(self.send_transaction, transaction)
                transaction_futures.append(future)
            
            # Acompanha o progresso
            completed = 0
            for future in futures.as_completed(transaction_futures):
                completed += 1
                if completed % 100 == 0:
                    print(f"Progresso: {completed}/{num_transactions} transações enviadas")
        
        print(f"Envio de transações concluído: {num_transactions} transações enviadas.")


def main():
    # Parse dos argumentos de linha de comando
    parser = argparse.ArgumentParser(description='Gerador de transações bancárias via gRPC')
    parser.add_argument('--server', type=str, default='localhost:50051',
                        help='Endereço do servidor gRPC (padrão: localhost:50051)')
    parser.add_argument('--users', type=int, default=1000,
                        help='Número de usuários para gerar transações (padrão: 1000)')
    parser.add_argument('--transactions', type=int, default=1000,
                        help='Número de transações a serem enviadas (padrão: 1000)')
    parser.add_argument('--workers', type=int, default=10,
                        help='Número máximo de workers paralelos (padrão: 10)')
    parser.add_argument('--seed', type=int, default=42,
                        help='Semente para o gerador de números aleatórios (padrão: 42)')
    
    args = parser.parse_args()
    
    # Inicializa e executa o gerador
    generator = MockTransactionGenerator(
        server_address=args.server,
        num_users=args.users,
        seed=args.seed
    )
    generator.generate_and_send(
        num_transactions=args.transactions,
        max_workers=args.workers
    )


if __name__ == "__main__":
    main() 