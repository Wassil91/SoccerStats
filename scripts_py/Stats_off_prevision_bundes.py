import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]



# Supprimer la collection si elle existe déjà
if "Stats_off_prevision_bundes" in db.list_collection_names():
    db["Stats_off_prevision_bundes"].drop()

# Recréer la collection
collection = db["Stats_off_prevision_bundes"]

# Données pour les trois collections
data_22_23 = [
    {"Équipe": "Bayern Munich", "Tirs pm": 18.5, "Tirs CA pm": 7.6, "Dribbles pm": 13.7, "Fautes subies pm": 9.9},
    {"Équipe": "Bor. Dortmund", "Tirs pm": 16.6, "Tirs CA pm": 6.6, "Dribbles pm": 12.2, "Fautes subies pm": 12},
    {"Équipe": "RB Leipzig", "Tirs pm": 14.8, "Tirs CA pm": 5.7, "Dribbles pm": 10.1, "Fautes subies pm": 11.8},
    {"Équipe": "VfB Stuttgart", "Tirs pm": 13.6, "Tirs CA pm": 4.4, "Dribbles pm": 9.9, "Fautes subies pm": 12},
    {"Équipe": "B. Leverkusen", "Tirs pm": 12.9, "Tirs CA pm": 4.9, "Dribbles pm": 10.2, "Fautes subies pm": 12},
    {"Équipe": "Schalke 04", "Tirs pm": 12.5, "Tirs CA pm": 3.8, "Dribbles pm": 5.8, "Fautes subies pm": 11.5},
    {"Équipe": "Fribourg", "Tirs pm": 12.4, "Tirs CA pm": 5.3, "Dribbles pm": 6.4, "Fautes subies pm": 12.5},
    {"Équipe": "Mayence", "Tirs pm": 12.4, "Tirs CA pm": 4.7, "Dribbles pm": 6.6, "Fautes subies pm": 9.8},
    {"Équipe": "FC Cologne", "Tirs pm": 12.3, "Tirs CA pm": 4.6, "Dribbles pm": 6.6, "Fautes subies pm": 10.4},
    {"Équipe": "B. M'Gladbach", "Tirs pm": 12, "Tirs CA pm": 5.1, "Dribbles pm": 7.1, "Fautes subies pm": 13.1},
    {"Équipe": "Wolfsbourg", "Tirs pm": 12, "Tirs CA pm": 4.3, "Dribbles pm": 7.5, "Fautes subies pm": 13.3},
    {"Équipe": "Hoffenheim", "Tirs pm": 12, "Tirs CA pm": 4.1, "Dribbles pm": 7.3, "Fautes subies pm": 10.9},
    {"Équipe": "Eintr. Francfort", "Tirs pm": 11.9, "Tirs CA pm": 4.2, "Dribbles pm": 8.6, "Fautes subies pm": 11.1},
    {"Équipe": "VfL Bochum", "Tirs pm": 11.8, "Tirs CA pm": 3.8, "Dribbles pm": 5.1, "Fautes subies pm": 10.4},
    {"Équipe": "Hertha Berlin", "Tirs pm": 11.3, "Tirs CA pm": 3.9, "Dribbles pm": 9.5, "Fautes subies pm": 11.6},
    {"Équipe": "Union Berlin", "Tirs pm": 11.2, "Tirs CA pm": 3.6, "Dribbles pm": 5.4, "Fautes subies pm": 10.1},
    {"Équipe": "Werder Breme", "Tirs pm": 10.9, "Tirs CA pm": 4.2, "Dribbles pm": 6.9, "Fautes subies pm": 10.9},
    {"Équipe": "Augsbourg", "Tirs pm": 10.6, "Tirs CA pm": 3.1, "Dribbles pm": 5.9, "Fautes subies pm": 10.5}
]



data_21_22 = [
    {"Équipe": "Bayern Munich", "Tirs pm": 19.8, "Tirs CA pm": 7.8, "Dribbles pm": 14.5, "Fautes subies pm": 10.4},
    {"Équipe": "B. M'Gladbach", "Tirs pm": 14.8, "Tirs CA pm": 5.4, "Dribbles pm": 10.9, "Fautes subies pm": 13.9},
    {"Équipe": "Mayence", "Tirs pm": 13.8, "Tirs CA pm": 4.6, "Dribbles pm": 7.9, "Fautes subies pm": 11.4},
    {"Équipe": "FC Cologne", "Tirs pm": 13.8, "Tirs CA pm": 4.4, "Dribbles pm": 7.4, "Fautes subies pm": 13.5},
    {"Équipe": "Fribourg", "Tirs pm": 13.6, "Tirs CA pm": 4.8, "Dribbles pm": 6.6, "Fautes subies pm": 11},
    {"Équipe": "B. Leverkusen", "Tirs pm": 13.5, "Tirs CA pm": 5.6, "Dribbles pm": 11.8, "Fautes subies pm": 9.7},
    {"Équipe": "Bor. Dortmund", "Tirs pm": 13.3, "Tirs CA pm": 5.2, "Dribbles pm": 10.4, "Fautes subies pm": 11.7},
    {"Équipe": "VfB Stuttgart", "Tirs pm": 13.3, "Tirs CA pm": 4.6, "Dribbles pm": 11, "Fautes subies pm": 12.3},
    {"Équipe": "Hoffenheim", "Tirs pm": 13.3, "Tirs CA pm": 4.4, "Dribbles pm": 7.4, "Fautes subies pm": 10.2},
    {"Équipe": "Eintr. Francfort", "Tirs pm": 13.2, "Tirs CA pm": 4.1, "Dribbles pm": 8.4, "Fautes subies pm": 8.8},
    {"Équipe": "RB Leipzig", "Tirs pm": 12.9, "Tirs CA pm": 5.4, "Dribbles pm": 10.2, "Fautes subies pm": 12.5},
    {"Équipe": "Wolfsbourg", "Tirs pm": 12.4, "Tirs CA pm": 4.2, "Dribbles pm": 9.4, "Fautes subies pm": 12.2},
    {"Équipe": "Union Berlin", "Tirs pm": 12.1, "Tirs CA pm": 4.6, "Dribbles pm": 7.2, "Fautes subies pm": 9.8},
    {"Équipe": "VfL Bochum", "Tirs pm": 12.1, "Tirs CA pm": 4.1, "Dribbles pm": 6.8, "Fautes subies pm": 11.5},
    {"Équipe": "Hertha Berlin", "Tirs pm": 10.8, "Tirs CA pm": 3.6, "Dribbles pm": 8.3, "Fautes subies pm": 12.6},
    {"Équipe": "Augsbourg", "Tirs pm": 10.8, "Tirs CA pm": 3.7, "Dribbles pm": 7.7, "Fautes subies pm": 10.5},
    {"Équipe": "Arminia Bielefeld", "Tirs pm": 10.7, "Tirs CA pm": 3.3, "Dribbles pm": 8.4, "Fautes subies pm": 9},
    {"Équipe": "Greuther Fuerth", "Tirs pm": 9.2, "Tirs CA pm": 3.1, "Dribbles pm": 7.8, "Fautes subies pm": 12.8}
]

data_20_21 = [
    {"Équipe": "Bayern Munich", "Tirs pm": 17.1, "Tirs CA pm": 6.9, "Dribbles pm": 12.9, "Fautes subies pm": 11},
    {"Équipe": "RB Leipzig", "Tirs pm": 16, "Tirs CA pm": 6, "Dribbles pm": 9.3, "Fautes subies pm": 11.9},
    {"Équipe": "Bor. Dortmund", "Tirs pm": 14.6, "Tirs CA pm": 5.7, "Dribbles pm": 11.8, "Fautes subies pm": 11},
    {"Équipe": "Wolfsbourg", "Tirs pm": 14.1, "Tirs CA pm": 4.9, "Dribbles pm": 8.7, "Fautes subies pm": 11.1},
    {"Équipe": "VfB Stuttgart", "Tirs pm": 13.4, "Tirs CA pm": 4.7, "Dribbles pm": 12.1, "Fautes subies pm": 12.8},
    {"Équipe": "B. M'Gladbach", "Tirs pm": 13.4, "Tirs CA pm": 5.2, "Dribbles pm": 9.4, "Fautes subies pm": 12.1},
    {"Équipe": "Eintr. Francfort", "Tirs pm": 13.2, "Tirs CA pm": 5.1, "Dribbles pm": 8.1, "Fautes subies pm": 10.8},
    {"Équipe": "B. Leverkusen", "Tirs pm": 13, "Tirs CA pm": 4.9, "Dribbles pm": 12.5, "Fautes subies pm": 9.9},
    {"Équipe": "Hoffenheim", "Tirs pm": 12.6, "Tirs CA pm": 4.6, "Dribbles pm": 8.2, "Fautes subies pm": 12},
    {"Équipe": "Union Berlin", "Tirs pm": 11.7, "Tirs CA pm": 4.2, "Dribbles pm": 6.1, "Fautes subies pm": 11},
    {"Équipe": "Fribourg", "Tirs pm": 11.4, "Tirs CA pm": 4.4, "Dribbles pm": 6.8, "Fautes subies pm": 11.8},
    {"Équipe": "Hertha Berlin", "Tirs pm": 11.3, "Tirs CA pm": 4.4, "Dribbles pm": 9.7, "Fautes subies pm": 12.6},
    {"Équipe": "Mayence", "Tirs pm": 11.1, "Tirs CA pm": 4.1, "Dribbles pm": 7.9, "Fautes subies pm": 11.1},
    {"Équipe": "Werder Breme", "Tirs pm": 10.6, "Tirs CA pm": 3.7, "Dribbles pm": 5.5, "Fautes subies pm": 13.7},
    {"Équipe": "FC Cologne", "Tirs pm": 10.6, "Tirs CA pm": 3.2, "Dribbles pm": 7, "Fautes subies pm": 12.6},
    {"Équipe": "Augsbourg", "Tirs pm": 9.9, "Tirs CA pm": 3.3, "Dribbles pm": 8.3, "Fautes subies pm": 11.9},
    {"Équipe": "Arminia Bielefeld", "Tirs pm": 9.8, "Tirs CA pm": 3, "Dribbles pm": 6.9, "Fautes subies pm": 12.3},
    {"Équipe": "Schalke 04", "Tirs pm": 8.9, "Tirs CA pm": 2.5, "Dribbles pm": 9.8, "Fautes subies pm": 11.9}
]


data_23_24 = [
    {"Équipe": "Bayern Munich", "Tirs pm": 20, "Tirs CA pm": 7.4, "Dribbles pm": 14.7, "Fautes subies pm": 9.2},
    {"Équipe": "B. Leverkusen", "Tirs pm": 18.3, "Tirs CA pm": 7, "Dribbles pm": 10.7, "Fautes subies pm": 10.1},
    {"Équipe": "RB Leipzig", "Tirs pm": 16.4, "Tirs CA pm": 6.9, "Dribbles pm": 9.1, "Fautes subies pm": 12.3},
    {"Équipe": "VfB Stuttgart", "Tirs pm": 15.7, "Tirs CA pm": 6.5, "Dribbles pm": 9.9, "Fautes subies pm": 11.2},
    {"Équipe": "Bor. Dortmund", "Tirs pm": 15.3, "Tirs CA pm": 5.6, "Dribbles pm": 9.1, "Fautes subies pm": 11.7},
    {"Équipe": "VfL Bochum", "Tirs pm": 14.5, "Tirs CA pm": 4.3, "Dribbles pm": 5.2, "Fautes subies pm": 11.6},
    {"Équipe": "Mayence", "Tirs pm": 13.7, "Tirs CA pm": 3.9, "Dribbles pm": 7.4, "Fautes subies pm": 12.1},
    {"Équipe": "B. M'Gladbach", "Tirs pm": 13.6, "Tirs CA pm": 4.4, "Dribbles pm": 7.1, "Fautes subies pm": 11.7},
    {"Équipe": "Hoffenheim", "Tirs pm": 13.1, "Tirs CA pm": 4.5, "Dribbles pm": 6.1, "Fautes subies pm": 11},
    {"Équipe": "Augsbourg", "Tirs pm": 12.5, "Tirs CA pm": 4.1, "Dribbles pm": 7.2, "Fautes subies pm": 10.7},
    {"Équipe": "Darmstadt", "Tirs pm": 12.3, "Tirs CA pm": 3.7, "Dribbles pm": 5.5, "Fautes subies pm": 10.7},
    {"Équipe": "Wolfsbourg", "Tirs pm": 12.3, "Tirs CA pm": 4.6, "Dribbles pm": 6.4, "Fautes subies pm": 12.1},
    {"Équipe": "Fribourg", "Tirs pm": 11.9, "Tirs CA pm": 4.6, "Dribbles pm": 6.2, "Fautes subies pm": 11.2},
    {"Équipe": "FC Cologne", "Tirs pm": 11.9, "Tirs CA pm": 4.1, "Dribbles pm": 7.8, "Fautes subies pm": 10.7},
    {"Équipe": "Werder Breme", "Tirs pm": 11.7, "Tirs CA pm": 3.5, "Dribbles pm": 7.4, "Fautes subies pm": 11.8},
    {"Équipe": "Union Berlin", "Tirs pm": 11.7, "Tirs CA pm": 3.9, "Dribbles pm": 5.9, "Fautes subies pm": 9.9},
    {"Équipe": "FC Heidenheim", "Tirs pm": 11.7, "Tirs CA pm": 3.2, "Dribbles pm": 6.5, "Fautes subies pm": 9.3},
    {"Équipe": "Eintr. Francfort", "Tirs pm": 10.9, "Tirs CA pm": 4.2, "Dribbles pm": 8.3, "Fautes subies pm": 9.2}
]

# Werder Breme, B. Leverkusen, "B. M'Gladbach, FC Heidenheim, VfB Stuttgart, Bor. Dortmund,
# Eintr. Francfort, VfL Bochum, FC Cologne

# Fusionner les données des trois collections
all_data = data_23_24 + data_22_23 + data_21_22 + data_20_21

# Initialiser un dictionnaire pour stocker les résultats
team_results = {}

# Calculer les moyennes pour chaque équipe
for team_data in all_data:
    team_name = team_data["Équipe"]
    if team_name not in team_results:
        team_results[team_name] = {
            "Tirs pm": [], "Tirs CA pm": [], "Dribbles pm": [], "Fautes subies pm": []
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
sorted_teams = sorted(team_results.items(), key=lambda x: x[1]["Tirs pm"], reverse=True)

# Assigner la position en fonction du classement basé sur la moyenne de Tirs pm
position = 1
for team, stats in sorted_teams:
    team_results[team]["Position"] = position
    position += 1

# Stocker les résultats dans la collection MongoDB
for team, stats in sorted_teams:
    team_data = {
        "Équipe": team,
        **stats
    }
    collection.insert_one(team_data)
    print(f"Données insérées pour l'équipe : {team}")

# Afficher les résultats (facultatif)
for team, stats in sorted_teams:
    print(f"Équipe: {team}")
    for key, value in stats.items():
        print(f"{key}: {value}")
    print()