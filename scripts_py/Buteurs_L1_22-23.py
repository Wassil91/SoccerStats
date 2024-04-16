import requests
from bs4 import BeautifulSoup
import pymongo

# Fonction pour se connecter à MongoDB et récupérer la base de données
def connect_to_mongodb():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    client = pymongo.MongoClient(mongodb_uri)
    db = client["SoccerStats"]
    return db

# Fonction pour supprimer une collection si elle existe
def drop_collection_if_exists(db, collection_name):
    if collection_name in db.list_collection_names():
        db.drop_collection(collection_name)
        print(f"La collection '{collection_name}' a été supprimée avec succès.")

# URL à scraper
url = "https://www.maxifoot.fr/classement-buteur-ligue-1-france-2023.htm"

# Faire la requête HTTP
response = requests.get(url)

# Vérifier si la requête a réussi
if response.status_code == 200:
    # Parser le contenu HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Connexion à la base de données MongoDB
    db = connect_to_mongodb()
    
    # Nom de la collection pour stocker les buteurs
    collection_name = "Buteurs_L1_22-23"
    
    # Supprimer la collection si elle existe déjà
    drop_collection_if_exists(db, collection_name)
    
    # Créer la collection pour stocker les buteurs
    collection = db[collection_name]
    
    # Trouver la table contenant les données des buteurs
    table = soup.find('div', class_='butd1').find('table')
    
    # Parcourir les lignes de la table
    for row in table.find_all('tr'):
        # Extraire les données de chaque cellule de la ligne
        cells = row.find_all('td')
        if len(cells) > 0:
            position = cells[0].text.strip()
            joueur = cells[1].text.strip()
            equipe = cells[2].text.strip()
            buts = cells[3].text.strip()
            derniers_buts = cells[4].text.strip()
            matchs = cells[5].text.strip()
            
            # Créer un document pour le buteur
            buteur = {
                "Position": position,
                "Joueur": joueur,
                "Equipe": equipe,
                "Buts": buts,
                "Derniers_buts": derniers_buts,
                "Matchs": matchs
            }
            
            # Insérer le document dans la collection Buteurs_L1
            collection.insert_one(buteur)
    
    print("Les données ont été extraites avec succès et stockées dans la collection 'Buteurs_L1' de la base de données 'SoccerStats'.")
else:
    print("La requête a échoué. Statut de la réponse :", response.status_code)
