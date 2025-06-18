#Com base no arquivo de IDH de Minas Gerais, incluir o arquivo .csv em anexo em um Banco Relacional e realizar a migrar'cao do Banco Relacional para um Banco NoSql.
import os
from src.MongoDB import MongoDBApp
from src.dao.idhDAO import idhDAO
from src.model.idh import idh
import pandas as pd

def insert_idh():
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, 'data', 'IDH2010.csv')
    df_idh = pd.read_csv(csv_path, sep=';', encoding='UTF-8', decimal=',', thousands='.')
    df_idh = df_idh.rename(columns={
        "ANO": "ano",
        "Código da Unidade da Federação": "cod_uf",
        "Nome da Unidade da Federação": "nome",
        "Código do Município": "cod_municipio",
        "Município": "nome_municipio",
        "Esperança de vida ao nascer": "esperanca_de_vida",
        "Mortalidade infantil": "mortalidade_infantil"
    })

    idh_dao = idhDAO()
    for index, row in df_idh.iterrows():
        idh_instance = idh()
        idh_instance.cod_uf = row['cod_uf']
        idh_instance.nome = row['nome']
        idh_instance.cod_municipio = row['cod_municipio']
        idh_instance.nome_municipio = row['nome_municipio']
        idh_instance.esperanca_de_vida = row['esperanca_de_vida']
        idh_instance.mortalidade_infantil = row['mortalidade_infantil']
        idh_instance.ano = row['ano']
        
        idh_dao.inserir(idh_instance)

    mongo_app = MongoDBApp(collection_name="idh")
    
    for index, row in df_idh.iterrows():
        document = {
            "cod_uf": row['cod_uf'],
            "nome": row['nome'],
            "cod_municipio": row['cod_municipio'],
            "nome_municipio": row['nome_municipio'],
            "esperanca_de_vida": row['esperanca_de_vida'],
            "mortalidade_infantil": row['mortalidade_infantil'],
            "ano": row['ano'] if pd.notna(row['ano']) else None
        }
        mongo_app.insert_document(document)
    
    mongo_app.close_connection()
    
    