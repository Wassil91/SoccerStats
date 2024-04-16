import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]



# Supprimer la collection si elle existe déjà
if "Stats_gen_prevision_bundes" in db.list_collection_names():
    db["Stats_gen_prevision_bundes"].drop()

# Recréer la collection
collection = db["Stats_gen_prevision_bundes"]

# Données pour les trois collections
data_22_23 = [
    {"Équipe": "Bayern Munich", "Buts": 92, "Tirs pm": 18.5, "Possession%": 64.3, "PassesRéussies%": 87.1, "AériensGagnés": 13.9},
    {"Équipe": "Bor. Dortmund", "Buts": 83, "Tirs pm": 16.6, "Possession%": 58.3, "PassesRéussies%": 84.4, "AériensGagnés": 12.6},
    {"Équipe": "RB Leipzig", "Buts": 64, "Tirs pm": 14.8, "Possession%": 58.4, "PassesRéussies%": 83.6, "AériensGagnés": 13},
    {"Équipe": "Fribourg", "Buts": 51, "Tirs pm": 12.4, "Possession%": 48, "PassesRéussies%": 76.9, "AériensGagnés": 22},
    {"Équipe": "Wolfsbourg", "Buts": 57, "Tirs pm": 12, "Possession%": 50.5, "PassesRéussies%": 79.4, "AériensGagnés": 15.6},
    {"Équipe": "B. Leverkusen", "Buts": 57, "Tirs pm": 12.9, "Possession%": 51.8, "PassesRéussies%": 81.9, "AériensGagnés": 12.7},
    {"Équipe": "B. M'Gladbach", "Buts": 52, "Tirs pm": 12, "Possession%": 54, "PassesRéussies%": 83.4, "AériensGagnés": 12.6},
    {"Équipe": "Union Berlin", "Buts": 51, "Tirs pm": 11.2, "Possession%": 42.9, "PassesRéussies%": 73.6, "AériensGagnés": 22.7},
    {"Équipe": "Mayence", "Buts": 54, "Tirs pm": 12.4, "Possession%": 43.9, "PassesRéussies%": 71.3, "AériensGagnés": 21.9},
    {"Équipe": "Eintr. Francfort", "Buts": 58, "Tirs pm": 11.9, "Possession%": 52.5, "PassesRéussies%": 79.5, "AériensGagnés": 13.8},
    {"Équipe": "Werder Breme", "Buts": 51, "Tirs pm": 10.9, "Possession%": 49.5, "PassesRéussies%": 76.3, "AériensGagnés": 18.7},
    {"Équipe": "VfB Stuttgart", "Buts": 45, "Tirs pm": 13.6, "Possession%": 50.1, "PassesRéussies%": 80.9, "AériensGagnés": 16.2},
    {"Équipe": "FC Cologne", "Buts": 49, "Tirs pm": 12.3, "Possession%": 49.4, "PassesRéussies%": 77.8, "AériensGagnés": 19.5},
    {"Équipe": "Hoffenheim", "Buts": 48, "Tirs pm": 12, "Possession%": 48, "PassesRéussies%": 77.1, "AériensGagnés": 16.3},
    {"Équipe": "Hertha Berlin", "Buts": 42, "Tirs pm": 11.3, "Possession%": 41.3, "PassesRéussies%": 71.2, "AériensGagnés": 18.8},
    {"Équipe": "VfL Bochum", "Buts": 40, "Tirs pm": 11.8, "Possession%": 45.7, "PassesRéussies%": 69.9, "AériensGagnés": 22.8},
    {"Équipe": "Schalke 04", "Buts": 35, "Tirs pm": 12.5, "Possession%": 45.2, "PassesRéussies%": 71.7, "AériensGagnés": 20.5},
    {"Équipe": "Augsbourg", "Buts": 42, "Tirs pm": 10.6, "Possession%": 40.9, "PassesRéussies%": 68.1, "AériensGagnés": 18.9}
]



data_21_22 = [
    {"Équipe": "Bayern Munich", "Buts": 97, "Tirs pm": 19.8, "Possession%": 64.8, "PassesRéussies%": 86.0, "AériensGagnés": 12.5},
    {"Équipe": "B. Leverkusen", "Buts": 80, "Tirs pm": 13.5, "Possession%": 53.7, "PassesRéussies%": 81.8, "AériensGagnés": 13.1},
    {"Équipe": "Bor. Dortmund", "Buts": 85, "Tirs pm": 13.3, "Possession%": 59.4, "PassesRéussies%": 84.0, "AériensGagnés": 12.5},
    {"Équipe": "RB Leipzig", "Buts": 72, "Tirs pm": 12.9, "Possession%": 56.5, "PassesRéussies%": 83.1, "AériensGagnés": 14.9},
    {"Équipe": "Fribourg", "Buts": 58, "Tirs pm": 13.6, "Possession%": 48.6, "PassesRéussies%": 76.2, "AériensGagnés": 19.9},
    {"Équipe": "B. M'Gladbach", "Buts": 52, "Tirs pm": 14.8, "Possession%": 54.1, "PassesRéussies%": 82.0, "AériensGagnés": 14.3},
    {"Équipe": "FC Cologne", "Buts": 52, "Tirs pm": 13.8, "Possession%": 54.8, "PassesRéussies%": 77.4, "AériensGagnés": 17.8},
    {"Équipe": "Mayence", "Buts": 50, "Tirs pm": 13.8, "Possession%": 46.0, "PassesRéussies%": 74.1, "AériensGagnés": 18},
    {"Équipe": "Wolfsbourg", "Buts": 43, "Tirs pm": 12.4, "Possession%": 50.2, "PassesRéussies%": 78.6, "AériensGagnés": 18.4},
    {"Équipe": "VfB Stuttgart", "Buts": 41, "Tirs pm": 13.3, "Possession%": 50.4, "PassesRéussies%": 80.7, "AériensGagnés": 17.3},
    {"Équipe": "Union Berlin", "Buts": 50, "Tirs pm": 12.1, "Possession%": 43.3, "PassesRéussies%": 73.6, "AériensGagnés": 17.4},
    {"Équipe": "Eintr. Francfort", "Buts": 45, "Tirs pm": 13.2, "Possession%": 49.4, "PassesRéussies%": 76.2, "AériensGagnés": 16.4},
    {"Équipe": "Hoffenheim", "Buts": 58, "Tirs pm": 13.3, "Possession%": 53.2, "PassesRéussies%": 80.7, "AériensGagnés": 14.5},
    {"Équipe": "VfL Bochum", "Buts": 38, "Tirs pm": 12.1, "Possession%": 44.5, "PassesRéussies%": 72.1, "AériensGagnés": 18.7},
    {"Équipe": "Augsbourg", "Buts": 39, "Tirs pm": 10.8, "Possession%": 40.6, "PassesRéussies%": 72.0, "AériensGagnés": 16.6},
    {"Équipe": "Arminia Bielefeld", "Buts": 27, "Tirs pm": 10.7, "Possession%": 39.9, "PassesRéussies%": 71.7, "AériensGagnés": 19.6},
    {"Équipe": "Hertha Berlin", "Buts": 37, "Tirs pm": 10.8, "Possession%": 43.2, "PassesRéussies%": 74.7, "AériensGagnés": 16.6},
    {"Équipe": "Greuther Fuerth", "Buts": 28, "Tirs pm": 9.2, "Possession%": 43.0, "PassesRéussies%": 74.8, "AériensGagnés": 14.1}
]


data_20_21 = [
    {"Équipe": "Bayern Munich", "Buts": 99, "Tirs pm": 17.1, "Possession%": 58.1, "PassesRéussies%": 85.5, "AériensGagnés": 12.9},
    {"Équipe": "Bor. Dortmund", "Buts": 75, "Tirs pm": 14.6, "Possession%": 57.5, "PassesRéussies%": 85.5, "AériensGagnés": 12.8},
    {"Équipe": "Wolfsbourg", "Buts": 61, "Tirs pm": 14.1, "Possession%": 51.0, "PassesRéussies%": 78.0, "AériensGagnés": 16.9},
    {"Équipe": "RB Leipzig", "Buts": 60, "Tirs pm": 16.0, "Possession%": 57.3, "PassesRéussies%": 83.2, "AériensGagnés": 18.6},
    {"Équipe": "B. Leverkusen", "Buts": 53, "Tirs pm": 13.0, "Possession%": 57.3, "PassesRéussies%": 84.4, "AériensGagnés": 13.1},
    {"Équipe": "Eintr. Francfort", "Buts": 69, "Tirs pm": 13.2, "Possession%": 24.8, "PassesRéussies%": 79.6, "AériensGagnés": 17.9},
    {"Équipe": "B. M'Gladbach", "Buts": 64, "Tirs pm": 13.4, "Possession%": 51.5, "PassesRéussies%": 82.0, "AériensGagnés": 15.3},
    {"Équipe": "VfB Stuttgart", "Buts": 56, "Tirs pm": 13.4, "Possession%": 27.4, "PassesRéussies%": 81.1, "AériensGagnés": 16.3},
    {"Équipe": "Hoffenheim", "Buts": 52, "Tirs pm": 12.6, "Possession%": 50.8, "PassesRéussies%": 80.7, "AériensGagnés": 15.7},
    {"Équipe": "Union Berlin", "Buts": 50, "Tirs pm": 11.7, "Possession%": 20.9, "PassesRéussies%": 76.2, "AériensGagnés": 17.6},
    {"Équipe": "Fribourg", "Buts": 52, "Tirs pm": 11.4, "Possession%": 47.5, "PassesRéussies%": 78.1, "AériensGagnés": 17.5},
    {"Équipe": "Hertha Berlin", "Buts": 41, "Tirs pm": 11.3, "Possession%": 49.8, "PassesRéussies%": 79.5, "AériensGagnés": 15.8},
    {"Équipe": "Mayence", "Buts": 39, "Tirs pm": 11.1, "Possession%": 42.7, "PassesRéussies%": 71.3, "AériensGagnés": 18.3},
    {"Équipe": "Augsbourg", "Buts": 36, "Tirs pm": 9.9, "Possession%": 44.0, "PassesRéussies%": 73.6, "AériensGagnés": 16.8},
    {"Équipe": "Arminia Bielefeld", "Buts": 26, "Tirs pm": 9.8, "Possession%": 44.1, "PassesRéussies%": 74.6, "AériensGagnés": 22.2},
    {"Équipe": "Werder Breme", "Buts": 36, "Tirs pm": 10.6, "Possession%": 18.4, "PassesRéussies%": 76.2, "AériensGagnés": 18.3},
    {"Équipe": "FC Cologne", "Buts": 34, "Tirs pm": 10.6, "Possession%": 47.1, "PassesRéussies%": 77.3, "AériensGagnés": 18.5},
    {"Équipe": "Schalke 04", "Buts": 25, "Tirs pm": 8.9, "Possession%": 46.2, "PassesRéussies%": 76.5, "AériensGagnés": 15.6}
]


data_23_24 = [
    {"Équipe": "Bayern Munich", "Buts": 78, "Tirs pm": 20.0, "Possession%": 62.0, "PassesRéussies%": 88.7, "AériensGagnés": 12.5},
    {"Équipe": "B. Leverkusen", "Buts": 68, "Tirs pm": 18.3, "Possession%": 63.3, "PassesRéussies%": 88.9, "AériensGagnés": 10.4},
    {"Équipe": "VfB Stuttgart", "Buts": 60, "Tirs pm": 15.7, "Possession%": 59.9, "PassesRéussies%": 86.3, "AériensGagnés": 14.3},
    {"Équipe": "Bor. Dortmund", "Buts": 55, "Tirs pm": 15.3, "Possession%": 57.8, "PassesRéussies%": 85.0, "AériensGagnés": 12.3},
    {"Équipe": "RB Leipzig", "Buts": 60, "Tirs pm": 16.4, "Possession%": 57.0, "PassesRéussies%": 84.6, "AériensGagnés": 13.4},
    {"Équipe": "B. M'Gladbach", "Buts": 46, "Tirs pm": 13.6, "Possession%": 47.1, "PassesRéussies%": 81.5, "AériensGagnés": 14.7},
    {"Équipe": "Eintr. Francfort", "Buts": 42, "Tirs pm": 10.9, "Possession%": 51.7, "PassesRéussies%": 80.9, "AériensGagnés": 14.3},
    {"Équipe": "Hoffenheim", "Buts": 45, "Tirs pm": 13.1, "Possession%": 48.0, "PassesRéussies%": 79.9, "AériensGagnés": 15.3},
    {"Équipe": "Fribourg", "Buts": 39, "Tirs pm": 11.9, "Possession%": 45.4, "PassesRéussies%": 78.8, "AériensGagnés": 18.6},
    {"Équipe": "Werder Breme", "Buts": 35, "Tirs pm": 11.7, "Possession%": 47.3, "PassesRéussies%": 79.3, "AériensGagnés": 16.7},
    {"Équipe": "Augsbourg", "Buts": 43, "Tirs pm": 12.5, "Possession%": 43.5, "PassesRéussies%": 76.4, "AériensGagnés": 17.9},
    {"Équipe": "FC Heidenheim", "Buts": 35, "Tirs pm": 11.7, "Possession%": 42.2, "PassesRéussies%": 73.1, "AériensGagnés": 21.0},
    {"Équipe": "Wolfsbourg", "Buts": 33, "Tirs pm": 12.3, "Possession%": 46.6, "PassesRéussies%": 79.6, "AériensGagnés": 15.4},
    {"Équipe": "Mayence", "Buts": 22, "Tirs pm": 13.7, "Possession%": 45.2, "PassesRéussies%": 74.7, "AériensGagnés": 20.1},
    {"Équipe": "VfL Bochum", "Buts": 30, "Tirs pm": 14.5, "Possession%": 44.8, "PassesRéussies%": 70.1, "AériensGagnés": 23.7},
    {"Équipe": "Union Berlin", "Buts": 25, "Tirs pm": 11.7, "Possession%": 42.3, "PassesRéussies%": 76.6, "AériensGagnés": 18.0},
    {"Équipe": "FC Cologne", "Buts": 20, "Tirs pm": 11.9, "Possession%": 43.7, "PassesRéussies%": 79.0, "AériensGagnés": 15.5},
    {"Équipe": "Darmstadt", "Buts": 26, "Tirs pm": 12.3, "Possession%": 46.3, "PassesRéussies%": 78.7, "AériensGagnés": 15.3}
]

# Werder Breme, B. Leverkusen, "B. M'Gladbach, FC Heidenheim, VfB Stuttgart, Bor. Dortmund, Eintr. Francfort
# VfL Bochum, FC Cologne

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