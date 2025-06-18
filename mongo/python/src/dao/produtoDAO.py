'''
Created on 09/06/2025
 
@author: nelsonjunior
'''
#coneção para o banco de dados postgres 


import psycopg2
from src.model.produto import Produto
 
 
class ProdutoDAO:
    def __init__(self):
        self.conexao = psycopg2.connect(
            host="localhost",
            database="comparativenosql",
            user="postgres",
            password="admin",
            port="5432"
        )
        self.cursor = self.conexao.cursor()
    
    def pesquisa_todos(self) -> list[Produto]:
        try:
            self.cursor.execute("SELECT * FROM produto")
            produtos = self.cursor.fetchall()
            lista_produtos = []
            if not produtos:
                print("Nenhum produto encontrado.")
                return lista_produtos
            
            for produto in produtos:
                p = Produto()
                p.id = int(produto[0])
                p.nome = produto[1]
                p.valor = float(produto[2])
                lista_produtos.append(p)
            return lista_produtos
        
        except Exception as e:
            print(f"Erro ao pesquisar produtos: {e}")
    
    def inserir(self, produto: Produto):
        try:
            with self.conexao.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO produto (nome, valor) VALUES (%s, %s)",
                    (produto.nome, produto.valor)
                )
            self.conexao.commit()
            print("Produto inserido com sucesso.")
        except Exception as e:
            print(f"Erro ao inserir produto: {e}")
            self.conexao.rollback()
    
    def atualizar(self, produto: Produto):
        try:
            self.cursor.execute(
                "UPDATE produto SET nome = %s, valor = %s WHERE id = %s",
                (produto.nome, produto.valor, produto.id)
            )
            self.conexao.commit()
            print("Produto atualizado com sucesso.")
        except Exception as e:
            print(f"Erro ao atualizar produto: {e}")
            self.conexao.rollback()