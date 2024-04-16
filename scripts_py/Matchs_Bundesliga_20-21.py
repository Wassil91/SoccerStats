import requests
from bs4 import BeautifulSoup
import csv
import re
from itertools import cycle
from pymongo import MongoClient
from datetime import datetime

def clean_date(date_str):
    # Nettoyer la date en enlevant les caractères indésirables
    cleaned_date = re.sub(r'^ top \d+journée, dimanche', '', date_str)
    cleaned_date = re.sub(r'\s+', ' ', cleaned_date)
    return cleaned_date.strip()

# Définir une fonction pour vérifier si une date est dans le passé ou non
def is_past_date(date_str):
    today = datetime.today().strftime('%Y-%m-%d')
    return datetime.strptime(date_str, '%d/%m/%Y') < datetime.strptime(today, '%Y-%m-%d')

def extract_and_save_to_csv(url):
    # Obtenir le contenu HTML de la page
    response = requests.get(url)
    if response.status_code == 200:
        html_content = response.text
        # Analyser le HTML
        soup = BeautifulSoup(html_content, 'html.parser')

        # Ouverture du fichier CSV en mode écriture
        with open("matchs2.csv", mode='w', newline='', encoding='utf-8') as csv_file:
            fieldnames = ["Journée", "Equipe_Domicile", "Score", "Equipe_Extérieur", "Date"]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

            # Écriture de l'en-tête CSV
            writer.writeheader()

            # Sélectionner tous les éléments correspondant à un match
            match_elements = soup.find_all('tr', class_=re.compile(r'^cl'))

            # Créer un itérateur pour les numéros de journée
            journees = cycle(range(1, 35))
            journee = 1  # initialiser la première journée

            # Boucle sur chaque match pour extraire les informations
            for i, match in enumerate(match_elements):
                # Extraire la date de la journée
                date = match.find_previous('tr', class_='ch3').text.strip()
                date = clean_date(date)

                # Extraire les informations spécifiques à chaque match
                teams = match.find_all('a', class_='eqc')
                team_home = teams[0].text.strip()
                team_away = teams[1].text.strip()
                
                # Vérifier s'il y a un score pour ce match
                score_element = match.find('th')
                if score_element and '-' in score_element.text.strip():
                    score_home, score_away = score_element.text.strip().split('-')
                    score = f"{score_home}-{score_away}"
                else:
                    score = ""

                # Écrire les données dans le fichier CSV
                if i % 9 == 0 and i != 0:
                    journee += 1  # passer à la prochaine journée
                writer.writerow({
                    "Journée": journee,
                    "Equipe_Domicile": team_home,
                    "Score": score,
                    "Equipe_Extérieur": team_away,
                    "Date": datetime.today().strftime('%Y-%m-%d')
                })

        print("Données enregistrées dans matchs2.csv")
        return "matchs2.csv"
    else:
        print("Erreur lors de la requête HTTP:", response.status_code)
        return None

def store_in_mongodb(csv_file):
    # Connexion à la base de données MongoDB
    client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
    # Sélectionner ou créer une base de données
    db = client['SoccerStats']
    # Supprimer la collection si elle existe
    db.drop_collection('matchs_bundesliga_20-21')
    # Sélectionner ou créer une collection
    collection = db['matchs_bundesliga_20-21']

    # Lire le fichier CSV et stocker les données dans la collection MongoDB
    with open(csv_file, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            collection.insert_one(row)

    print("Données enregistrées dans MongoDB.")

# URL de la page à scraper
url = "https://www.maxifoot.fr/calendrier-bundesliga-allemagne-2020-2021.htm"

# Appeler la fonction pour extraire et stocker les données dans un fichier CSV
csv_file = extract_and_save_to_csv(url)

# Si le fichier CSV est généré avec succès, stocker les données dans MongoDB
if csv_file:
    store_in_mongodb(csv_file)
