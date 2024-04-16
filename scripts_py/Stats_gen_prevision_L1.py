import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]



# Supprimer la collection si elle existe déjà
if "Stats_gen_prevision_L1" in db.list_collection_names():
    db["Stats_gen_prevision_L1"].drop()

# Recréer la collection
collection = db["Stats_gen_prevision_L1"]

# Données pour les trois collections
data_22_23 = [
    {"Équipe": "Paris SG", "Buts": 89, "Tirs pm": 15, "Possession%": 60.9, "PassesRéussies%": 90.5, "AériensGagnés": 6.6},
    {"Équipe": "Lyon", "Buts": 65, "Tirs pm": 13.7, "Possession%": 58.0, "PassesRéussies%": 85.1, "AériensGagnés": 12.0},
    {"Équipe": "Marseille", "Buts": 67, "Tirs pm": 14.6, "Possession%": 56.9, "PassesRéussies%": 83.2, "AériensGagnés": 13.1},
    {"Équipe": "Lens", "Buts": 68, "Tirs pm": 13.9, "Possession%": 55.8, "PassesRéussies%": 86.4, "AériensGagnés": 10.8},
    {"Équipe": "Rennes", "Buts": 69, "Tirs pm": 13.9, "Possession%": 55.8, "PassesRéussies%": 84.7, "AériensGagnés": 14.2},
    {"Équipe": "Lille", "Buts": 65, "Tirs pm": 14.7, "Possession%": 60.9, "PassesRéussies%": 86.1, "AériensGagnés": 11.7},
    {"Équipe": "Nice", "Buts": 48, "Tirs pm": 13.5, "Possession%": 51.5, "PassesRéussies%": 85.4, "AériensGagnés": 10.3},
    {"Équipe": "Monaco", "Buts": 70, "Tirs pm": 12.6, "Possession%": 47.8, "PassesRéussies%": 80.5, "AériensGagnés": 13.6},
    {"Équipe": "Lorient", "Buts": 52, "Tirs pm": 10.6, "Possession%": 46.4, "PassesRéussies%": 83.5, "AériensGagnés": 10.9},
    {"Équipe": "Montpellier", "Buts": 65, "Tirs pm": 11.6, "Possession%": 45.4, "PassesRéussies%": 78.6, "AériensGagnés": 12.7},
    {"Équipe": "Reims", "Buts": 45, "Tirs pm": 13.8, "Possession%": 48.4, "PassesRéussies%": 80.0, "AériensGagnés": 12.6},
    {"Équipe": "Strasbourg", "Buts": 51, "Tirs pm": 11.8, "Possession%": 45.6, "PassesRéussies%": 79.0, "AériensGagnés": 18.0},
    {"Équipe": "Brest", "Buts": 44, "Tirs pm": 11.2, "Possession%": 44.6, "PassesRéussies%": 78.0, "AériensGagnés": 16.8},
    {"Équipe": "Toulouse", "Buts": 51, "Tirs pm": 12.0, "Possession%": 51.1, "PassesRéussies%": 82.0, "AériensGagnés": 12.9},
    {"Équipe": "Nantes", "Buts": 37, "Tirs pm": 11.8, "Possession%": 45.5, "PassesRéussies%": 80.0, "AériensGagnés": 15.7},
    {"Équipe": "Clermont F.", "Buts": 45, "Tirs pm": 10.6, "Possession%": 48.1, "PassesRéussies%": 81.1, "AériensGagnés": 10.8},
    {"Équipe": "Troyes", "Buts": 45, "Tirs pm": 10.4, "Possession%": 42.1, "PassesRéussies%": 80.2, "AériensGagnés": 12.6},
    {"Équipe": "Auxerre", "Buts": 35, "Tirs pm": 11.3, "Possession%": 43.0, "PassesRéussies%": 79.6, "AériensGagnés": 13.0},
    {"Équipe": "Angers", "Buts": 33, "Tirs pm": 9.8, "Possession%": 46.7, "PassesRéussies%": 82.8, "AériensGagnés": 11.6},
    {"Équipe": "AC Ajaccio", "Buts": 23, "Tirs pm": 8.4, "Possession%": 42.5, "PassesRéussies%": 75.5, "AériensGagnés": 16.2}
]


data_21_22 = [
    {"Équipe": "Paris SG", "Tirs pm": 14.8, "Possession%": 63.3, "PassesRéussies%": 90.6, "AériensGagnés": 7},
    {"Équipe": "Lyon", "Tirs pm": 14.6, "Possession%": 58.7, "PassesRéussies%": 86.0, "AériensGagnés": 12.5},
    {"Équipe": "Rennes", "Tirs pm": 14.5, "Possession%": 56.2, "PassesRéussies%": 84.4, "AériensGagnés": 14.7},
    {"Équipe": "Strasbourg", "Tirs pm": 12.6, "Possession%": 49.0, "PassesRéussies%": 80.5, "AériensGagnés": 17.4},
    {"Équipe": "Monaco", "Tirs pm": 11.9, "Possession%": 53.9, "PassesRéussies%": 81.2, "AériensGagnés": 14.4},
    {"Équipe": "Marseille", "Tirs pm": 13.1, "Possession%": 62.0, "PassesRéussies%": 88.2, "AériensGagnés": 9.9},
    {"Équipe": "Nice", "Tirs pm": 11.9, "Possession%": 51.3, "PassesRéussies%": 83.6, "AériensGagnés": 14.2},
    {"Équipe": "Lens", "Tirs pm": 13.6, "Possession%": 50.9, "PassesRéussies%": 84.2, "AériensGagnés": 11.5},
    {"Équipe": "Nantes", "Tirs pm": 11.2, "Possession%": 43.2, "PassesRéussies%": 78.6, "AériensGagnés": 15.9},
    {"Équipe": "Brest", "Tirs pm": 10.6, "Possession%": 43.1, "PassesRéussies%": 78.6, "AériensGagnés": 16.5},
    {"Équipe": "Lille", "Tirs pm": 12.3, "Possession%": 50.7, "PassesRéussies%": 81.5, "AériensGagnés": 14.1},
    {"Équipe": "Reims", "Tirs pm": 10.4, "Possession%": 41.7, "PassesRéussies%": 78.4, "AériensGagnés": 11.9},
    {"Équipe": "Montpellier", "Tirs pm": 11.5, "Possession%": 47.9, "PassesRéussies%": 80.4, "AériensGagnés": 11.8},
    {"Équipe": "Angers", "Tirs pm": 10.3, "Possession%": 48.4, "PassesRéussies%": 83.2, "AériensGagnés": 11.1},
    {"Équipe": "Metz", "Tirs pm": 9.8, "Possession%": 42.7, "PassesRéussies%": 78.7, "AériensGagnés": 14.2},
    {"Équipe": "Lorient", "Tirs pm": 11.7, "Possession%": 44.1, "PassesRéussies%": 79.8, "AériensGagnés": 11.6},
    {"Équipe": "Troyes", "Tirs pm": 10.2, "Possession%": 44.0, "PassesRéussies%": 79.9, "AériensGagnés": 11.2},
    {"Équipe": "Saint-Etienne", "Tirs pm": 11.6, "Possession%": 48.4, "PassesRéussies%": 79.6, "AériensGagnés": 12.5},
    {"Équipe": "Clermont F.", "Tirs pm": 11.5, "Possession%": 49.8, "PassesRéussies%": 82.2, "AériensGagnés": 11.7},
    {"Équipe": "Bordeaux", "Tirs pm": 11.8, "Possession%": 47.1, "PassesRéussies%": 79.8, "AériensGagnés": 13.2}
]


data_20_21 = [
    {"Équipe": "Paris SG", "Tirs pm": 15, "Possession%": 60.1, "PassesRéussies%": 89.5, "AériensGagnés": 9.5},
    {"Équipe": "Lille", "Tirs pm": 12.8, "Possession%": 52.6, "PassesRéussies%": 83.5, "AériensGagnés": 15.8},
    {"Équipe": "Lyon", "Tirs pm": 16.1, "Possession%": 53.6, "PassesRéussies%": 84.7, "AériensGagnés": 14.3},
    {"Équipe": "Monaco", "Tirs pm": 12.8, "Possession%": 54.2, "PassesRéussies%": 82.7, "AériensGagnés": 16.5},
    {"Équipe": "Rennes", "Tirs pm": 13.5, "Possession%": 56.8, "PassesRéussies%": 85.6, "AériensGagnés": 16.9},
    {"Équipe": "Metz", "Tirs pm": 11.5, "Possession%": 46.9, "PassesRéussies%": 79.8, "AériensGagnés": 16.1},
    {"Équipe": "Lens", "Tirs pm": 11.7, "Possession%": 51.1, "PassesRéussies%": 81.8, "AériensGagnés": 17.4},
    {"Équipe": "Brest", "Tirs pm": 11.8, "Possession%": 49.4, "PassesRéussies%": 81.3, "AériensGagnés": 18.6},
    {"Équipe": "Marseille", "Tirs pm": 10, "Possession%": 19.6, "PassesRéussies%": 82.0, "AériensGagnés": 14.9},
    {"Équipe": "Montpellier", "Tirs pm": 12.2, "Possession%": 46.4, "PassesRéussies%": 78.8, "AériensGagnés": 17.9},
    {"Équipe": "Nice", "Tirs pm": 10.8, "Possession%": 53.4, "PassesRéussies%": 85.6, "AériensGagnés": 10.5},
    {"Équipe": "Nantes", "Tirs pm": 10.8, "Possession%": 21.9, "PassesRéussies%": 77.0, "AériensGagnés": 18.1},
    {"Équipe": "Strasbourg", "Tirs pm": 11.3, "Possession%": 46.5, "PassesRéussies%": 78.3, "AériensGagnés": 18.3},
    {"Équipe": "Reims", "Tirs pm": 9.6, "Possession%": 22.6, "PassesRéussies%": 80.7, "AériensGagnés": 13.4},
    {"Équipe": "Saint-Etienne", "Tirs pm": 11.6, "Possession%": 49.0, "PassesRéussies%": 79.3, "AériensGagnés": 16.4},
    {"Équipe": "Bordeaux", "Tirs pm": 11.1, "Possession%": 25.2, "PassesRéussies%": 83.3, "AériensGagnés": 15.4},
    {"Équipe": "Lorient", "Tirs pm": 11.2, "Possession%": 45.9, "PassesRéussies%": 78.8, "AériensGagnés": 13.7},
    {"Équipe": "Angers", "Tirs pm": 10.7, "Possession%": 47.1, "PassesRéussies%": 81.3, "AériensGagnés": 13.2},
    {"Équipe": "Nimes", "Tirs pm": 10.3, "Possession%": 45.8, "PassesRéussies%": 77.5, "AériensGagnés": 14.0},
    {"Équipe": "Dijon", "Tirs pm": 9.2, "Possession%": 46.9, "PassesRéussies%": 80.0, "AériensGagnés": 14.3}
]


data_23_24 = [
    {"Équipe": "Paris SG", "Tirs pm": 15, "Possession%": 65.6, "PassesRéussies%": 89.8, "AériensGagnés": 8},
    {"Équipe": "Monaco", "Tirs pm": 15.1, "Possession%": 53.7, "PassesRéussies%": 82.4, "AériensGagnés": 12.3},
    {"Équipe": "Lille", "Tirs pm": 13.4, "Possession%": 57.3, "PassesRéussies%": 86.0, "AériensGagnés": 10.6},
    {"Équipe": "Brest", "Tirs pm": 14.2, "Possession%": 53.3, "PassesRéussies%": 81.3, "AériensGagnés": 17.6},
    {"Équipe": "Marseille", "Tirs pm": 13.7, "Possession%": 54.1, "PassesRéussies%": 84.7, "AériensGagnés": 11.3},
    {"Équipe": "Rennes", "Tirs pm": 13.7, "Possession%": 50.8, "PassesRéussies%": 83.9, "AériensGagnés": 11.3},
    {"Équipe": "Nice", "Tirs pm": 13.7, "Possession%": 53.4, "PassesRéussies%": 86.5, "AériensGagnés": 11.6},
    {"Équipe": "Reims", "Tirs pm": 12.5, "Possession%": 51.9, "PassesRéussies%": 81.7, "AériensGagnés": 12.2},
    {"Équipe": "Lens", "Tirs pm": 13.9, "Possession%": 52.1, "PassesRéussies%": 83.7, "AériensGagnés": 11.5},
    {"Équipe": "Montpellier", "Tirs pm": 13.6, "Possession%": 43.7, "PassesRéussies%": 78.1, "AériensGagnés": 13.1},
    {"Équipe": "Toulouse", "Tirs pm": 11.8, "Possession%": 48.4, "PassesRéussies%": 81.3, "AériensGagnés": 15},
    {"Équipe": "Lyon", "Tirs pm": 12.7, "Possession%": 51.3, "PassesRéussies%": 83.6, "AériensGagnés": 11.5},
    {"Équipe": "Lorient", "Tirs pm": 9.5, "Possession%": 45.5, "PassesRéussies%": 83.7, "AériensGagnés": 12},
    {"Équipe": "Strasbourg", "Tirs pm": 11, "Possession%": 42.3, "PassesRéussies%": 81.1, "AériensGagnés": 11.5},
    {"Équipe": "Le Havre", "Tirs pm": 11.2, "Possession%": 44.3, "PassesRéussies%": 80.0, "AériensGagnés": 14.7},
    {"Équipe": "Metz", "Tirs pm": 9.9, "Possession%": 35.9, "PassesRéussies%": 76.8, "AériensGagnés": 11.7},
    {"Équipe": "Nantes", "Tirs pm": 11.4, "Possession%": 45.5, "PassesRéussies%": 82.1, "AériensGagnés": 11.6},
    {"Équipe": "Clermont F.", "Tirs pm": 11.7, "Possession%": 49.3, "PassesRéussies%": 81.9, "AériensGagnés": 12.1}
]

# Fusionner les données des trois collections
all_data = data_23_24 + data_22_23 + data_21_22 + data_20_21

# Initialiser un dictionnaire pour stocker les résultats
team_results = {}

# Calculer les moyennes pour chaque équipe
for team_data in all_data:
    team_name = team_data["Équipe"]
    if team_name not in team_results:
        team_results[team_name] = {
            "Tirs pm": [], "Possession%": [], "PassesRéussies%": [], "AériensGagnés": []
        }
    for key in team_results[team_name]:
        if key in team_data and isinstance(team_data[key], (int, float)):
            team_results[team_name][key].append(team_data[key])

# Calculer les moyennes pour chaque équipe
for team_name, stats in team_results.items():
    for key, values in stats.items():
        if values:
            # Calculer la moyenne et arrondir à deux chiffres après la virgule
            team_results[team_name][key] = round(sum(values) / len(values), 2)
        else:
            team_results[team_name][key] = 0

# Trier les équipes en fonction de leur moyenne de Tirs pm
sorted_teams = sorted(team_results.items(), key=lambda x: x[1]["Possession%"], reverse=True)

# Assigner la position en fonction du classement basé sur la moyenne de Tirs pm
position = 1
for team, stats in sorted_teams:
    team_results[team]["Position"] = position
    position += 1

for team, stats in sorted_teams:
    team_data = {
        "Équipe": team,
        **stats
    }
    filter_query = {"Équipe": team}  # Utilisez un champ unique pour identifier le document
    update_query = {"$set": team_data}  # Utilisez $set pour mettre à jour ou insérer le document
    collection.update_one(filter_query, update_query, upsert=True)
    print(f"Données insérées pour l'équipe : {team}")