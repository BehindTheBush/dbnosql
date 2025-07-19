import psycopg2
import random
import time
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine
from datetime import datetime, timedelta

class ComparativoBanco:
    def __init__(self):
        # PostgreSQL
        self.conexao = psycopg2.connect(
            host="localhost",
            database="trabalhofinal",
            user="postgres",
            password="admin",
            port="5432"
        )
        self.cursor = self.conexao.cursor()
        self.engine = create_engine("postgresql+psycopg2://postgres:admin@localhost:5432/trabalhofinal")

        # MongoDB
        self.mongo_client = MongoClient("mongodb://root:example@localhost:27017/")
        self.mongo_db = self.mongo_client["comparativo"]
        self.colecao_produtos = self.mongo_db["produtos"]
        self.colecao_carrinhos = self.mongo_db["carrinhos"]

    def gerar_dados(self, total=2000):
        print(f"\nGerando {total} registros em cada banco de dados...")
        tempo_inicio_pg = time.time()

        nomes = ["João", "Maria", "Ana", "Carlos", "Lucas", "Fernanda", "Julia", "Roberto"]
        sobrenomes = ["Silva", "Souza", "Oliveira", "Pereira", "Almeida", "Costa"]

        # PostgreSQL - Clientes
        cliente_ids = []
        for i in range(total):
            nome = f"{random.choice(nomes)} {random.choice(sobrenomes)}"
            email = f"{nome.replace(' ', '.').lower()}@exemplo.com"
            data = datetime.now() - timedelta(days=random.randint(0, 1000))
            self.cursor.execute(
                "INSERT INTO clientes (nome, email, data_cadastro) VALUES (%s, %s, %s) RETURNING id",
                (nome, email, data)
            )
            cliente_id = self.cursor.fetchone()[0]
            cliente_ids.append(cliente_id)

        # PostgreSQL - Produtos
        produto_ids = []
        for i in range(100):
            nome_prod = f"Produto_{i+1}"
            preco = round(random.uniform(10, 1000), 2)
            self.cursor.execute(
                "INSERT INTO produtos (nome, preco) VALUES (%s, %s) RETURNING id",
                (nome_prod, preco)
            )
            produto_id = self.cursor.fetchone()[0]
            produto_ids.append(produto_id)

        for i in range(total):
            cliente_id = cliente_ids[i]
            data_pedido = datetime.now() - timedelta(days=random.randint(0, 365))
            self.cursor.execute(
                "INSERT INTO pedidos (cliente_id, data_pedido) VALUES (%s, %s) RETURNING id",
                (cliente_id, data_pedido)
            )
            pedido_id = self.cursor.fetchone()[0]
            num_itens = random.randint(1, 3)
            produtos_usados = set()
            for _ in range(num_itens):
                produto_id = random.choice(produto_ids)
                while produto_id in produtos_usados:
                    produto_id = random.choice(produto_ids)
                produtos_usados.add(produto_id)
                quantidade = random.randint(1, 5)
                preco_unitario = round(random.uniform(10, 1000), 2)
                self.cursor.execute(
                    "INSERT INTO itens_pedido (pedido_id, produto_id, quantidade, preco_unitario) VALUES (%s, %s, %s, %s)",
                    (pedido_id, produto_id, quantidade, preco_unitario)
                )

        self.conexao.commit()
        tempo_fim_pg = time.time()
        tempo_total_pg = tempo_fim_pg - tempo_inicio_pg

        tempo_inicio_mongo = time.time()
        # MongoDB - Produtos
        produtos_mongo = []
        for i in range(100):
            produto = {
                "produto_id": i + 1,
                "nome": f"Produto_{i+1}",
                "preco": round(random.uniform(10, 1000), 2),
                "data_cadastro": datetime.now() - timedelta(days=random.randint(0, 1000))
            }
            produtos_mongo.append(produto)
        self.colecao_produtos.insert_many(produtos_mongo)

        # MongoDB - Carrinhos
        carrinhos = []
        for i in range(total):
            carrinho = {
                "cliente_id": random.randint(1, total),
                "itens": [
                    {"produto_id": random.randint(1, 100), "quantidade": random.randint(1, 5)}
                    for _ in range(random.randint(1, 3))
                ],
                "ultima_atualizacao": datetime.now()
            }
            carrinhos.append(carrinho)
        self.colecao_carrinhos.insert_many(carrinhos)
        tempo_fim_mongo = time.time()
        tempo_total_mongo = tempo_fim_mongo - tempo_inicio_mongo
        print(f"Dados gerados no PostgreSQL em {tempo_total_pg:.2f} segundos.")
        print(f"Dados gerados no MongoDB em {tempo_total_mongo:.2f} segundos.")

        return tempo_total_mongo, tempo_total_pg

    def exportar_produtos_para_postgres(self):
        print("Exportando produtos do MongoDB para PostgreSQL...")
        inicio = time.time()
        produtos = list(self.colecao_produtos.find({}, {"_id": 0}))
        for produto in produtos:
            self.cursor.execute(
                """
                INSERT INTO produtos (id, nome, preco)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    nome = EXCLUDED.nome,
                    preco = EXCLUDED.preco
                """,
                (
                    produto.get("produto_id"),
                    produto.get("nome"),
                    produto.get("preco"),
                )
            )
        self.conexao.commit()
        fim = time.time()
        print(f"Exportação concluída em {fim - inicio:.2f} segundos.")
        return fim - inicio

    def consulta_postgres(self):
        print("Consulta JOIN no PostgreSQL:")
        inicio = time.time()
        query = """
            SELECT c.nome, COUNT(p.id) AS total_pedidos, SUM(i.quantidade * i.preco_unitario) AS total_gasto
            FROM clientes c
            JOIN pedidos p ON c.id = p.cliente_id
            JOIN itens_pedido i ON p.id = i.pedido_id
            GROUP BY c.nome
            ORDER BY total_gasto DESC
            LIMIT 5;
        """
        self.cursor.execute(query)
        resultado = self.cursor.fetchall()
        fim = time.time()
        duracao = fim - inicio
        for r in resultado:
            print(f"Cliente: {r[0]} | Pedidos: {r[1]} | Total Gasto: R${r[2]:.2f}")
        return duracao

    def consulta_mongo(self):
        print("\nConsulta agregada no MongoDB - Carrinhos:")
        inicio = time.time()
        pipeline = [
            {"$unwind": "$itens"},
            {"$group": {
                "_id": "$cliente_id",
                "total_itens": {"$sum": "$itens.quantidade"},
                "produtos_diferentes": {"$addToSet": "$itens.produto_id"}
            }},
            {"$project": {
                "cliente_id": "$_id",
                "total_itens": 1,
                "qtd_produtos": {"$size": "$produtos_diferentes"}
            }},
            {"$sort": {"total_itens": -1}},
            {"$limit": 5}
        ]
        resultado = list(self.colecao_carrinhos.aggregate(pipeline))
        fim = time.time()
        duracao = fim - inicio
        for r in resultado:
            print(f"Cliente ID: {r['cliente_id']} | Itens: {r['total_itens']} | Produtos: {r['qtd_produtos']}")
        return duracao


    def consulta_total_clientes_postgres(self):
        print("\nConsulta total de clientes no PostgreSQL:")
        inicio = time.time()
        self.cursor.execute("SELECT COUNT(*) FROM clientes;")
        total = self.cursor.fetchone()[0]
        fim = time.time()
        print(f"Total de clientes: {total}")
        return fim - inicio

    def consulta_total_carrinhos_mongo(self):
        print("\nConsulta total de carrinhos no MongoDB:")
        inicio = time.time()
        total = self.colecao_carrinhos.count_documents({})
        fim = time.time()
        print(f"Total de carrinhos: {total}")
        return fim - inicio

    # Consulta 2: Produto mais vendido
    def consulta_produto_mais_vendido_postgres(self):
        print("\nConsulta produto mais vendido no PostgreSQL:")
        inicio = time.time()
        query = """
            SELECT p.nome, SUM(i.quantidade) AS total_vendido
            FROM produtos p
            JOIN itens_pedido i ON p.id = i.produto_id
            GROUP BY p.nome
            ORDER BY total_vendido DESC
            LIMIT 1;
        """
        self.cursor.execute(query)
        resultado = self.cursor.fetchone()
        fim = time.time()
        print(f"Produto mais vendido: {resultado[0]} | Quantidade: {resultado[1]}")
        return fim - inicio

    def consulta_produto_mais_vendido_mongo(self):
        print("\nConsulta produto mais vendido no MongoDB:")
        inicio = time.time()
        pipeline = [
            {"$unwind": "$itens"},
            {"$group": {
                "_id": "$itens.produto_id",
                "total_vendido": {"$sum": "$itens.quantidade"}
            }},
            {"$sort": {"total_vendido": -1}},
            {"$limit": 1}
        ]
        resultado = list(self.colecao_carrinhos.aggregate(pipeline))
        fim = time.time()
        if resultado:
            print(f"Produto mais vendido (ID): {resultado[0]['_id']} | Quantidade: {resultado[0]['total_vendido']}")
        return fim - inicio

    # Consulta 3: Clientes/carrinhos cadastrados nos últimos 30 dias
    def consulta_clientes_30dias_postgres(self):
        print("\nConsulta clientes cadastrados nos últimos 30 dias no PostgreSQL:")
        inicio = time.time()
        query = """
            SELECT COUNT(*) FROM clientes
            WHERE data_cadastro >= NOW() - INTERVAL '30 days';
        """
        self.cursor.execute(query)
        total = self.cursor.fetchone()[0]
        fim = time.time()
        print(f"Clientes cadastrados nos últimos 30 dias: {total}")
        return fim - inicio

    def consulta_carrinhos_30dias_mongo(self):
        print("\nConsulta carrinhos atualizados nos últimos 30 dias no MongoDB:")
        inicio = time.time()
        limite = datetime.now() - timedelta(days=30)
        total = self.colecao_carrinhos.count_documents({"ultima_atualizacao": {"$gte": limite}})
        fim = time.time()
        print(f"Carrinhos atualizados nos últimos 30 dias: {total}")
        return fim - inicio

    def relatorio_final(self):
        print("\nGerando relatório final...")
        tempo_insercao_mongo, tempo_insercao_pg = self.gerar_dados()
        tempo_export = self.exportar_produtos_para_postgres()
        tempo_pg = self.consulta_postgres()
        tempo_mongo = self.consulta_mongo()

        tempo_total_clientes_pg = self.consulta_total_clientes_postgres()
        tempo_total_carrinhos_mongo = self.consulta_total_carrinhos_mongo()

        tempo_produto_vendido_pg = self.consulta_produto_mais_vendido_postgres()
        tempo_produto_vendido_mongo = self.consulta_produto_mais_vendido_mongo()

        tempo_clientes_30dias_pg = self.consulta_clientes_30dias_postgres()
        tempo_carrinhos_30dias_mongo = self.consulta_carrinhos_30dias_mongo()

        print("\nRelatório comparativo:\n")
        print(f"Tempo de geração de 2.000 registros no MongoDB: {tempo_insercao_mongo:.2f} segundos")
        print(f"Tempo de geração de 2.000 registros no PostgreSQL: {tempo_insercao_pg:.2f} segundos")
        print(f"Tempo de exportação do MongoDB para PostgreSQL: {tempo_export:.2f} segundos")
        print(f"Tempo da consulta JOIN no PostgreSQL: {tempo_pg:.2f} segundos")
        print(f"Tempo da consulta agregada no MongoDB: {tempo_mongo:.2f} segundos")
        print(f"Tempo da consulta total de clientes no PostgreSQL: {tempo_total_clientes_pg:.4f} segundos")
        print(f"Tempo da consulta total de carrinhos no MongoDB: {tempo_total_carrinhos_mongo:.4f} segundos")
        print(f"Tempo da consulta produto mais vendido no PostgreSQL: {tempo_produto_vendido_pg:.4f} segundos")
        print(f"Tempo da consulta produto mais vendido no MongoDB: {tempo_produto_vendido_mongo:.4f} segundos")
        print(f"Tempo da consulta clientes cadastrados nos últimos 30 dias no PostgreSQL: {tempo_clientes_30dias_pg:.4f} segundos")
        print(f"Tempo da consulta carrinhos atualizados nos últimos 30 dias no MongoDB: {tempo_carrinhos_30dias_mongo:.4f} segundos")

        print("\nAnálise:")
        print("- PostgreSQL oferece maior consistência transacional (ACID), sendo indicado para sistemas críticos.")
        print("- MongoDB oferece consistência eventual e flexibilidade, sendo adequado para dados não estruturados.")
        print("- MongoDB apresenta inserções em lote mais rápidas, enquanto o PostgreSQL é mais eficiente para operações JOIN.")
        print("- Consultas simples (contagem, filtro por data) são rápidas em ambos os bancos.")
        print("- Consultas agregadas e de relacionamento são mais eficientes no PostgreSQL, enquanto MongoDB se destaca em flexibilidade de agregação.")

        print("\nRelatório concluído.\n")

if __name__ == "__main__":
    banco = ComparativoBanco()
    banco.relatorio_final()
