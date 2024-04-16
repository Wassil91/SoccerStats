import requests
from bs4 import BeautifulSoup
import csv
import pymongo
from datetime import datetime

def scrap_bundesliga(url):
    # Faire la requête HTTP
    response = requests.get(url)
    if response.status_code == 200:
        # Parser le contenu HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Trouver le tableau contenant le classement
        table = soup.find('table', class_='clas1')
        if table:
            # Ouvrir un fichier CSV pour écrire les données
            with open("classement_bundesliga.csv", "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                # Écrire l'en-tête du CSV
                writer.writerow(["Position", "Équipe", "Pts", "J", "G", "N", "P", "BP", "BC", "Diff"])
                
                # Récupérer les lignes du tableau
                rows = table.find_all('tr')
                for row in rows[1:]:  # Ignorer la première ligne qui contient les titres des colonnes
                    # Récupérer les cellules de chaque ligne
                    cells = row.find_all('td')
                    if len(cells) >= 10:
                        position = cells[0].text.strip()
                        equipe = cells[1].text.strip()
                        points = cells[2].text.strip()
                        matches_joues = cells[3].text.strip()
                        victoires = cells[4].text.strip()
                        matches_nuls = cells[5].text.strip()
                        defaites = cells[6].text.strip()
                        buts_marques = cells[7].text.strip()
                        buts_encaisses = cells[8].text.strip()
                        diff = cells[9].text.strip()
                        
                        # Écrire les données dans le fichier CSV
                        writer.writerow([position, equipe, points, matches_joues, victoires, matches_nuls, defaites, buts_marques, buts_encaisses, diff])
                    else:
                        print("Erreur: données manquantes dans la ligne.")
            print("Les données ont été extraites avec succès et écrites dans le fichier classement_bundesliga.csv.")
        else:
            print("Tableau non trouvé.")
    else:
        print("Échec de la requête HTTP.")

# Appel de la fonction en passant l'URL comme argument
scrap_bundesliga("https://www.maxifoot.fr/resultat-bundesliga-allemagne-2022.htm")

# Connexion à MongoDB
mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
db_name = "SoccerStats"
collection_name = "classement_bundesliga_21-22"

# Insertion des données du fichier CSV dans MongoDB
def insert_into_mongodb(file_path, mongodb_uri, db_name, collection_name):
    # Connexion à MongoDB
    client = pymongo.MongoClient(mongodb_uri)
    # Sélection de la base de données
    db = client[db_name]

    # Supprimer la collection si elle existe
    if collection_name in db.list_collection_names():
        db.drop_collection(collection_name)
        print(f"La collection {collection_name} a été supprimée.")

    # Créer la collection
    collection = db[collection_name]

    # Lecture du fichier CSV et insertion des données dans la collection MongoDB
    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Ajouter la date du jour
            row["Date"] = int(datetime.now().strftime("%Y%m%d"))  # Format de date en int
            # Supprimer les clés vides du dictionnaire
            filtered_row = {key: value for key, value in row.items() if value}
            # Convertir les valeurs en int si possible
            for key, value in filtered_row.items():
                if isinstance(value, str) and value.isdigit():
                    filtered_row[key] = int(value)
            # Insérer les données dans la collection
            collection.insert_one(filtered_row)

    print(f"Les données du fichier {file_path} ont été insérées avec succès dans la collection {collection_name}.")

# Insertion des données du fichier CSV dans MongoDB
insert_into_mongodb("classement_bundesliga.csv", mongodb_uri, db_name, collection_name)
