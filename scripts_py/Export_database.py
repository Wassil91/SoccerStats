from pymongo import MongoClient
import json
from bson import json_util

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client.SoccerStats # Remplacer 'nom_de_votre_base_de_données' par le nom de votre base de données

# Créer un dictionnaire pour stocker les données de chaque collection
all_collections_data = {}


# Récupérer la liste des collections dans la base de données
collections = db.list_collection_names()

# Pour chaque collection, récupérer les données et les ajouter au dictionnaire
for collection_name in collections:
    collection = db[collection_name]
    documents = list(collection.find())
    all_collections_data[collection_name] = documents

# Écrire le dictionnaire dans un fichier JSON en utilisant json_util.dumps()
with open("toutes_les_collections.json", "w") as f:
    f.write(json_util.dumps(all_collections_data))

print("Export terminé.")