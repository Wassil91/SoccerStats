from pymongo import MongoClient
import json
from bson import json_util

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://localhost:27017')
db = client.SoccerStats # Remplacer 'nom_de_votre_base_de_données' par le nom de votre base de données


# Charger les données à partir du fichier JSON
with open("toutes_les_collections.json", "r") as f:
    data = json.load(f, object_hook=json_util.object_hook)

# Pour chaque collection, insérer les documents dans la base de données
for collection_name, documents in data.items():
    collection = db[collection_name]
    collection.insert_many(documents)

print("Importation terminée.")