'''
Created on 09/06/2025
 
@author: nelsonjunior
'''
#coneção para o banco de dados postgres 


import psycopg2
from src.model.idh import idh 
 
 
class idhDAO:
    def __init__(self):
        self.conexao = psycopg2.connect(
            host="localhost",
            database="comparativenosql",
            user="postgres",
            password="admin",
            port="5432"
        )
        self.cursor = self.conexao.cursor()
    
    def pesquisa_todos(self) -> list[idh]:
        try:
            self.cursor.execute("SELECT * FROM idh")
            idhs = self.cursor.fetchall()
            lista_idhs = []
            if not idhs:
                print("Nenhum idh encontrado.")
                return lista_idhs
            
            for idh in idhs:
                i = idh()
                i.id = int(idh[0])
                i.cod_uf = int(idh[1])
                i.nome = idh[2]
                i.cod_municipio = int(idh[3])
                i.nome_municipio = idh[4]
                i.esperanca_de_vida = float(idh[5])
                i.mortalidade_infantil = float(idh[6])
                i.ano = int(idh[7]) if idh[7] is not None else None
                lista_idhs.append(i)
            return lista_idhs
        
        except Exception as e:
            print(f"Erro ao pesquisar idhs: {e}")
    
    def inserir(self, idh: idh):
        try:
            with self.conexao.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO idh (cod_uf, nome, cod_municipio, nome_municipio, esperanca_de_vida, mortalidade_infantil, ano) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (idh.cod_uf, idh.nome, idh.cod_municipio, idh.nome_municipio, idh.esperanca_de_vida, idh.mortalidade_infantil, idh.ano)
                )
            self.conexao.commit()
            print("idh inserido com sucesso.")
        except Exception as e:
            print(f"Erro ao inserir idh: {e}")
            self.conexao.rollback()
    
    def atualizar(self, idh: idh):
        try:
            self.cursor.execute(
                "UPDATE idh SET cod_uf = %s, nome = %s, cod_municipio = %s, nome_municipio = %s, esperanca_de_vida = %s, mortalidade_infantil = %s, ano = %s WHERE id = %s",
                (idh.cod_uf, idh.nome, idh.cod_municipio, idh.nome_municipio, idh.esperanca_de_vida, idh.mortalidade_infantil, idh.ano, idh.id)
            )
            self.conexao.commit()
            print("idh atualizado com sucesso.")
        except Exception as e:
            print(f"Erro ao atualizar idh: {e}")
            self.conexao.rollback()