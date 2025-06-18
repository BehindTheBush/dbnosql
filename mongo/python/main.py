#criar funções main

from datetime import datetime
from src.tarefas.tarefav import insert_idh
from src.MongoDB import MongoDBApp
from src.dao.produtoDAO import ProdutoDAO
from src.model.produto import Produto
from pymongo.errors import CollectionInvalid
from pymongo import MongoClient

def consulta_postgres():
    conexao = ProdutoDAO()
    produtos =  conexao.pesquisa_todos()
    
    if not produtos:
        print("Nenhum produto encontrado.")
        return
    
    for produto in produtos:
        print(produto)

    print("Pesquisa concluída com sucesso.")

def consulta_mongodb():
    #conectar no mongodb
    app = MongoDBApp()
 
    # Inserir um novo documento
    # novo_doc = {"nome": "mantega", "valor": 15.0, "quantidade": 10, "categoria": "laticínios"}
    # app.insert_document(novo_doc)
 
    # Alterar documentos onde nome = "Maria"
    # filtro = {"nome": "feijão"}
    # novos_dados = {"valor": 25.0}
    # app.update_document(query=filtro, update=novos_dados)

    # excluir tudo que o valor for maior que 20.0
    app.delete_document({"valor": {"$gt": 20.0}})
    # app.delete_document({"nome": "mantega"})
 
    # Consultar todos os documentos
    app.print_collection()

def popular_mongodb():
    db = MongoDBApp(collection_name="usuarios")

    # 1. Inserir usuário
    usuario = {
        "nome": "Carlos Silva",
        "email": "carlos@email.com",
        "senha": "123456",
        "data_cadastro": datetime.now()
    }
    usuario_id = db.collection.insert_one(usuario).inserted_id

    # 2. Inserir produto
    db = MongoDBApp(collection_name="produtos")
    produto = {
        "nome": "Camiseta Preta",
        "descricao": "Camiseta de algodão preta, tamanho M",
        "preco": 59.9,
        "estoque": 50,
        "ativo": True
    }
    produto_id = db.collection.insert_one(produto).inserted_id

    # 3. Inserir pedido (referenciando usuário)
    db = MongoDBApp(collection_name="pedidos")
    pedido = {
        "usuario_id": usuario_id,
        "data_pedido": datetime.now(),
        "status": "pendente",
        "total": 119.8
    }
    pedido_id = db.collection.insert_one(pedido).inserted_id

    # 4. Inserir item do pedido (referenciando pedido e produto)
    db = MongoDBApp(collection_name="itens_pedido")
    item_pedido = {
        "pedido_id": pedido_id,
        "produto_id": produto_id,
        "quantidade": 2,
        "preco_unitario": 59.9,
        "subtotal": 119.8
    }
    db.collection.insert_one(item_pedido)

    print("Dados inseridos com sucesso nas coleções: usuarios, produtos, pedido, itens_pedido.")


def main():
    # consulta_postgres()
    # consulta_mongodb()
    # popular_mongodb()
    insert_idh()


    

if __name__ == "__main__":
    main()


