import requests
from bs4 import BeautifulSoup
import pymongo
from datetime import datetime
import re

# Fonction pour extraire les données de la page
def scrap_maxifoot(url):
    # Faire une requête GET à l'URL
    response = requests.get(url)
    # Vérifier si la requête a réussi
    if response.status_code == 200:
        # Analyser le contenu HTML de la page
        soup = BeautifulSoup(response.text, 'html.parser')
        # Trouver le tableau de classement
        table = soup.find('div', {'id': 'clastot'}).find('table')
        # Initialiser une liste pour stocker les données
        data = []
        # Parcourir les lignes du tableau
        for row in table.find_all('tr'):
            # Extraire les données de chaque colonne de la ligne
            cols = row.find_all(['td', 'th'])
            cols = [col.text.strip() for col in cols]
            # Vérifier si la ligne contient suffisamment de données
            if len(cols) >= 10:
                # Supprimer le texte entre parenthèses de la deuxième colonne
                cols[1] = re.sub(r'\(.*\)', '', cols[1]).strip()
                # Ajouter les données à la liste
                data.append(cols)
        return data
    else:
        print("Erreur lors de la requête GET:", response.status_code)
        return None

# Fonction pour écrire les données dans MongoDB
def write_to_mongodb(data, mongodb_uri, db_name, collection_name):
    # Se connecter à MongoDB
    client = pymongo.MongoClient(mongodb_uri)
    # Sélectionner la base de données
    db = client[db_name]
    # Supprimer la collection si elle existe
    db.drop_collection(collection_name)
    # Sélectionner la collection (cela créera automatiquement la collection si elle n'existe pas)
    collection = db[collection_name]
    # Parcourir les données et les stocker dans MongoDB
    for row in data:
        # Exclure les lignes de données non valides
        if "Position" not in row:
            # Convertir les valeurs numériques en entiers
            for i in range(0, len(row)):
                if i in [0, 9]:
                    try:
                        row[i] = int(row[i])
                    except ValueError:
                        pass
            try:
                # Convertir les colonnes pertinentes en entiers
                row[2] = int(row[2])  # Pts
                row[3] = int(row[3])  # J
                row[4] = int(row[4])  # G
                row[5] = int(row[5])  # N
                row[6] = int(row[6])  # P
                row[7] = int(row[7])  # BP
                row[8] = int(row[8])  # BC
            except ValueError:
                pass
            # Créer le document MongoDB
            document = {
                "Position": row[0],
                "Equipe": row[1],
                "Pts": row[2],
                "J": row[3],
                "G": row[4],
                "N": row[5],
                "P": row[6],
                "BP": row[7],
                "BC": row[8],
                "Diff": row[9],
                "Date": datetime.now()
            }
            # Insérer le document dans la collection MongoDB
            collection.insert_one(document)
    print("Les données ont été écrites dans MongoDB avec succès.")
    # Supprimer le premier document s'il correspond aux valeurs spécifiques
    first_document = collection.find_one()
    if first_document and first_document["Position"] == "P" and first_document["Equipe"] == "Equipe" and first_document["Pts"] == "Pts" and first_document["J"] == "J" and first_document["G"] == "G" and first_document["N"] == "N" and first_document["P"] == "P" and first_document["BP"] == "Bp" and first_document["BC"] == "Bc" and first_document["Diff"] == "Diff":
        collection.delete_one({"_id": first_document["_id"]})

# URL de la page à scraper
url = "https://www.maxifoot.fr/resultat-ligue-1-france-2022.htm"
# Informations pour se connecter à MongoDB
mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
db_name = "SoccerStats"
collection_name = "classement_L1_2021-2022"

# Appel de la fonction de scraping
classement_data = scrap_maxifoot(url)

# Vérification des données extraites
if classement_data:
    # Écriture des données dans MongoDB
    write_to_mongodb(classement_data, mongodb_uri, db_name, collection_name)
else:
    print("Aucune donnée n'a été extraite.")
