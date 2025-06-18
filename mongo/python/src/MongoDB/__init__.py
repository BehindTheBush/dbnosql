from pymongo import MongoClient
import json
from bson import json_util

class MongoDBApp:
    def __init__(self, uri="mongodb://root:example@localhost:27017/", db_name="puc", collection_name="produto"):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def insert_document(self, document):
        try:
            result = self.collection.insert_one(document)
            print(f"Document inserted with id: {result.inserted_id}")
        except Exception as e:
            print(f"Error inserting document: {e}")

    def find(self, query=None):
        try:
            if query is None:
                query = {}
            documents = self.collection.find(query)
            return list(documents)
        except Exception as e:
            print(f"Error finding documents: {e}")
            return []
    
    def update_document(self, query, update):
        try:
            result = self.collection.update_one(query, {"$set": update})
            if result.matched_count > 0:
                print(f"Document updated, matched count: {result.matched_count}")
            else:
                print("No documents matched the query.")
        except Exception as e:
            print(f"Error updating document: {e}")

    def delete_document(self, query):
        try:
            result = self.collection.delete_one(query)
            if result.deleted_count > 0:
                print(f"Document deleted, deleted count: {result.deleted_count}")
            else:
                print("No documents matched the query.")
        except Exception as e:
            print(f"Error deleting document: {e}")

    def close_connection(self):
        self.client.close()
    
    def print_collection(self):
        #print docuements in the collection using json format
        try:
            documents = self.find()
            print(json.dumps(documents, default=json_util.default, indent=4))
        except Exception as e:
            print(f"Error printing collection: {e}")