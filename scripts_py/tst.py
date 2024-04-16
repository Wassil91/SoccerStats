import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]
collection = db["Resultats_Predits"]

# Filtrer les documents
filtre = {"Saison": "matchs_L1", "Journée": {"$gt": "26"}}
resultats = collection.find(filtre)

# Parcourir les documents et afficher les données pertinentes
for document in resultats:
    print("Saison:", document["Saison"])
    print("Journée:", document["Journée"])
    print("Equipe Domicile:", document["Equipe_Domicile"])
    print("Equipe Extérieur:", document["Equipe_Extérieur"])
    print("Stats Domicile:", document["Stats_Domicile"])
    print("Stats Extérieur:", document["Stats_Extérieur"])
    print("Score:", document["Score"])
    print("Résultat Prédit:", document["Resultat_Predit"])
    print("------------------------------------")

