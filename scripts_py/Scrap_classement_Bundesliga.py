import requests
from bs4 import BeautifulSoup
import csv
import pymongo
from datetime import datetime

# Extraction des données et écriture dans un fichier CSV
# Extraction des données et écriture dans un fichier CSV
def classement_bundesliga():
    url = "https://www.matchendirect.fr/allemagne/bundesliga-1/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    table = soup.find("table", class_="table table-striped tableau_classement")
    rows = table.find_all("tr")

    with open("classement_bundesliga.csv", "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Position", "Équipe", "Pts", "J", "G", "N", "P", "BP", "BC", "Diff"])

        for row in rows:
            cells = row.find_all(["th", "td"])
            if len(cells) > 1:
                position_text = cells[0].get_text(strip=True)
                # Vérifier si la cellule de la position n'est pas vide
                if position_text:
                    position = int(position_text)  # Convertir en entier
                else:
                    position = None  # Si la cellule est vide, définir la position comme None
                # Recherche de l'élément <a> pour obtenir le nom de l'équipe
                team_element = cells[1].find("a")
                if team_element:
                    team = team_element.get_text(strip=True)
                    if team == "Stuttgart":
                        team = "VfB Stuttgart"  # Modification du nom de l'équipe
                    elif team == "Bayer Leverkusen":
                        team = "B. Leverkusen"
                    elif team == "Borussia Dortmund":
                        team = "Bor. Dortmund"
                    elif team == "Eintracht Francfort":
                        team = "Eintr. Francfort"
                    elif team == "Werder Brême":
                        team = "Werder Breme"
                    elif team == "Heidenheim":
                        team = "FC Heidenheim"
                    elif team == "Borussia M'gladbach":
                        team = "B. M'Gladbach"
                    elif team == "Bochum":
                        team = "VfL Bochum"
                    elif team == "Cologne":
                        team = "FC Cologne"                    
                else:
                    team = cells[1].get_text(strip=True)
                # Vérifier si la cellule contient un en-tête ou une valeur
                if cells[2].get_text(strip=True).isdigit():
                    points = int(cells[2].get_text(strip=True))  # Convertir en entier
                else:
                    points = None  # Si la cellule contient un en-tête, définir les points comme None
                # Vérifier si la cellule contient un en-tête ou une valeur
                if cells[3].get_text(strip=True).isdigit():
                    joues = int(cells[3].get_text(strip=True))  # Convertir en entier
                else:
                    joues = None  # Si la cellule contient un en-tête, définir les joues comme None
                # Convertir les colonnes restantes en entiers si possible, sinon les définir comme None
                gagnes = int(cells[4].get_text(strip=True)) if cells[4].get_text(strip=True).isdigit() else None
                nuls = int(cells[5].get_text(strip=True)) if cells[5].get_text(strip=True).isdigit() else None
                perdus = int(cells[6].get_text(strip=True)) if cells[6].get_text(strip=True).isdigit() else None
                buts_marques = int(cells[7].get_text(strip=True)) if cells[7].get_text(strip=True).isdigit() else None
                buts_encaisses = int(cells[8].get_text(strip=True)) if cells[8].get_text(strip=True).isdigit() else None
                # Vérifier si la cellule contient un en-tête ou une valeur
                if cells[9].get_text(strip=True).isdigit():
                    difference = int(cells[9].get_text(strip=True))  # Convertir en entier
                else:
                    # Si la cellule de différence de buts est absente, calculer la différence
                    difference = buts_marques - buts_encaisses if buts_marques is not None and buts_encaisses is not None else None

                writer.writerow([position, team, points, joues, gagnes, nuls, perdus, buts_marques, buts_encaisses, difference])

    print("Le fichier CSV du classement de la Bundesliga a été créé avec succès !")


    # Suppression de la troisième ligne dans le fichier CSV

    # Nom du fichier CSV d'entrée
    input_file = "classement_bundesliga.csv"
    # Nom du fichier CSV de sortie
    output_file = "classement_bundesliga_sans_troisieme_ligne.csv"

    # Lire le fichier CSV d'entrée et écrire son contenu dans un nouveau fichier CSV sans la 3ème ligne
    with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
            open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # Utiliser une variable pour suivre le numéro de ligne
        line_count = 0
        
        for row in reader:
            # Écrire toutes les lignes sauf la 3ème ligne
            if line_count != 2:
                writer.writerow(row)
            line_count += 1

    print("La troisième ligne a été supprimée avec succès.")

    # Connexion à MongoDB
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    collection_name = "classement_bundesliga"

    # Fonction pour insérer les données dans MongoDB
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
                row["Date"] = int(datetime.now().strftime("%Y%m%d%H%M"))  # Format de date en int
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
    insert_into_mongodb(output_file, mongodb_uri, db_name, collection_name)

classement_bundesliga()