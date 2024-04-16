import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]



# Supprimer la collection si elle existe déjà
if "Stats_def_prevision_bundes" in db.list_collection_names():
    db["Stats_def_prevision_bundes"].drop()

# Recréer la collection
collection = db["Stats_def_prevision_bundes"]

# Données pour les trois collections
data_22_23 = [
    {"Équipe": "VfL Bochum", "Tirs pm": 15.7, "Tacles pm": 18.4, "Interceptions pm": 9.9, "Fautes pm": 12.6, "Hors-jeux pm": 1.9},
    {"Équipe": "Bayern Munich", "Tirs pm": 9.2, "Tacles pm": 18.3, "Interceptions pm": 8.4, "Fautes pm": 9.0, "Hors-jeux pm": 2.5},
    {"Équipe": "FC Cologne", "Tirs pm": 11.1, "Tacles pm": 18.1, "Interceptions pm": 10.9, "Fautes pm": 11.8, "Hors-jeux pm": 2.0},
    {"Équipe": "Werder Breme", "Tirs pm": 14.0, "Tacles pm": 17.5, "Interceptions pm": 9.4, "Fautes pm": 12.5, "Hors-jeux pm": 1.9},
    {"Équipe": "Bor. Dortmund", "Tirs pm": 12.1, "Tacles pm": 17.1, "Interceptions pm": 8.8, "Fautes pm": 9.5, "Hors-jeux pm": 1.2},
    {"Équipe": "Hertha Berlin", "Tirs pm": 14.0, "Tacles pm": 16.9, "Interceptions pm": 10.8, "Fautes pm": 12.4, "Hors-jeux pm": 1.7},
    {"Équipe": "Eintr. Francfort", "Tirs pm": 10.9, "Tacles pm": 16.5, "Interceptions pm": 8.4, "Fautes pm": 12.6, "Hors-jeux pm": 2.1},
    {"Équipe": "Mayence", "Tirs pm": 12.9, "Tacles pm": 16.1, "Interceptions pm": 12.7, "Fautes pm": 14.0, "Hors-jeux pm": 2.2},
    {"Équipe": "Wolfsbourg", "Tirs pm": 13.8, "Tacles pm": 16.0, "Interceptions pm": 8.6, "Fautes pm": 11.5, "Hors-jeux pm": 1.5},
    {"Équipe": "Hoffenheim", "Tirs pm": 14.1, "Tacles pm": 16.0, "Interceptions pm": 8.4, "Fautes pm": 13.9, "Hors-jeux pm": 2.3},
    {"Équipe": "Schalke 04", "Tirs pm": 14.5, "Tacles pm": 15.9, "Interceptions pm": 9.0, "Fautes pm": 12.6, "Hors-jeux pm": 2.1},
    {"Équipe": "RB Leipzig", "Tirs pm": 9.2, "Tacles pm": 15.7, "Interceptions pm": 9.1, "Fautes pm": 11.6, "Hors-jeux pm": 2.4},
    {"Équipe": "VfB Stuttgart", "Tirs pm": 11.8, "Tacles pm": 15.5, "Interceptions pm": 9.1, "Fautes pm": 10.2, "Hors-jeux pm": 1.6},
    {"Équipe": "Augsbourg", "Tirs pm": 15.9, "Tacles pm": 15.4, "Interceptions pm": 8.4, "Fautes pm": 12.7, "Hors-jeux pm": 1.7},
    {"Équipe": "Fribourg", "Tirs pm": 13.1, "Tacles pm": 14.6, "Interceptions pm": 9.6, "Fautes pm": 11.9, "Hors-jeux pm": 1.7},
    {"Équipe": "B. M'Gladbach", "Tirs pm": 14.2, "Tacles pm": 14.6, "Interceptions pm": 7.9, "Fautes pm": 9.9, "Hors-jeux pm": 2.1},
    {"Équipe": "B. Leverkusen", "Tirs pm": 11.6, "Tacles pm": 14.5, "Interceptions pm": 8.6, "Fautes pm": 11.2, "Hors-jeux pm": 1.5},
    {"Équipe": "Union Berlin", "Tirs pm": 11.5, "Tacles pm": 14.5, "Interceptions pm": 7.7, "Fautes pm": 13.1, "Hors-jeux pm": 1.8}
]


data_21_22 = [
    {"Équipe": "FC Cologne", "Tirs pm": 12.6, "Tacles pm": 17.6, "Interceptions pm": 12.5, "Fautes pm": 12.3, "Hors-jeux pm": 2.0},
    {"Équipe": "Eintr. Francfort", "Tirs pm": 12.7, "Tacles pm": 17.5, "Interceptions pm": 11.1, "Fautes pm": 12.4, "Hors-jeux pm": 1.6},
    {"Équipe": "B. M'Gladbach", "Tirs pm": 14.0, "Tacles pm": 17.3, "Interceptions pm": 9.8, "Fautes pm": 10.6, "Hors-jeux pm": 2.2},
    {"Équipe": "VfL Bochum", "Tirs pm": 14.2, "Tacles pm": 17.2, "Interceptions pm": 12.7, "Fautes pm": 12.5, "Hors-jeux pm": 2.2},
    {"Équipe": "Mayence", "Tirs pm": 11.3, "Tacles pm": 17.0, "Interceptions pm": 12.9, "Fautes pm": 14.6, "Hors-jeux pm": 1.9},
    {"Équipe": "Greuther Fuerth", "Tirs pm": 15.0, "Tacles pm": 16.7, "Interceptions pm": 11.0, "Fautes pm": 12.9, "Hors-jeux pm": 2.0},
    {"Équipe": "Bayern Munich", "Tirs pm": 9.3, "Tacles pm": 16.6, "Interceptions pm": 10.3, "Fautes pm": 9.0, "Hors-jeux pm": 2.2},
    {"Équipe": "Augsbourg", "Tirs pm": 14.3, "Tacles pm": 16.5, "Interceptions pm": 10.7, "Fautes pm": 13.2, "Hors-jeux pm": 2.1},
    {"Équipe": "Arminia Bielefeld", "Tirs pm": 15.5, "Tacles pm": 16.1, "Interceptions pm": 12.4, "Fautes pm": 12.7, "Hors-jeux pm": 1.6},
    {"Équipe": "Hertha Berlin", "Tirs pm": 13.6, "Tacles pm": 16.1, "Interceptions pm": 11.9, "Fautes pm": 12.4, "Hors-jeux pm": 2.1},
    {"Équipe": "Bor. Dortmund", "Tirs pm": 10.9, "Tacles pm": 15.9, "Interceptions pm": 10.2, "Fautes pm": 10.6, "Hors-jeux pm": 2.1},
    {"Équipe": "Wolfsbourg", "Tirs pm": 13.1, "Tacles pm": 15.9, "Interceptions pm": 11.4, "Fautes pm": 12.1, "Hors-jeux pm": 2.2},
    {"Équipe": "Union Berlin", "Tirs pm": 11.6, "Tacles pm": 15.8, "Interceptions pm": 11.1, "Fautes pm": 12.4, "Hors-jeux pm": 1.9},
    {"Équipe": "VfB Stuttgart", "Tirs pm": 14.8, "Tacles pm": 15.6, "Interceptions pm": 10.1, "Fautes pm": 10.9, "Hors-jeux pm": 1.6},
    {"Équipe": "Fribourg", "Tirs pm": 14.2, "Tacles pm": 15.2, "Interceptions pm": 10.4, "Fautes pm": 11.5, "Hors-jeux pm": 1.7},
    {"Équipe": "RB Leipzig", "Tirs pm": 10.4, "Tacles pm": 14.7, "Interceptions pm": 10.3, "Fautes pm": 10.6, "Hors-jeux pm": 1.9},
    {"Équipe": "B. Leverkusen", "Tirs pm": 13.4, "Tacles pm": 14.2, "Interceptions pm": 10.6, "Fautes pm": 10.7, "Hors-jeux pm": 1.9},
    {"Équipe": "Hoffenheim", "Tirs pm": 12.5, "Tacles pm": 14.2, "Interceptions pm": 8.4, "Fautes pm": 12.6, "Hors-jeux pm": 2.1}
]


data_20_21 = [
    {"Équipe": "Wolfsbourg", "Tirs pm": 10.5, "Tacles pm": 17.5, "Interceptions pm": 14.0, "Fautes pm": 13.0, "Hors-jeux pm": 2.3},
    {"Équipe": "Eintr. Francfort", "Tirs pm": 12.8, "Tacles pm": 17.5, "Interceptions pm": 12.6, "Fautes pm": 13.5, "Hors-jeux pm": 2.0},
    {"Équipe": "Mayence", "Tirs pm": 13.3, "Tacles pm": 17.4, "Interceptions pm": 14.7, "Fautes pm": 14.7, "Hors-jeux pm": 1.7},
    {"Équipe": "FC Cologne", "Tirs pm": 12.3, "Tacles pm": 17.2, "Interceptions pm": 12.6, "Fautes pm": 12.4, "Hors-jeux pm": 1.7},
    {"Équipe": "Arminia Bielefeld", "Tirs pm": 13.2, "Tacles pm": 16.2, "Interceptions pm": 12.3, "Fautes pm": 11.5, "Hors-jeux pm": 1.4},
    {"Équipe": "Schalke 04", "Tirs pm": 17.3, "Tacles pm": 16.2, "Interceptions pm": 12.1, "Fautes pm": 12.2, "Hors-jeux pm": 2.0},
    {"Équipe": "Werder Breme", "Tirs pm": 13.5, "Tacles pm": 15.9, "Interceptions pm": 12.0, "Fautes pm": 11.6, "Hors-jeux pm": 2.0},
    {"Équipe": "Bor. Dortmund", "Tirs pm": 11.1, "Tacles pm": 15.2, "Interceptions pm": 12.1, "Fautes pm": 10.6, "Hors-jeux pm": 2.0},
    {"Équipe": "Bayern Munich", "Tirs pm": 10.1, "Tacles pm": 15.0, "Interceptions pm": 11.7, "Fautes pm": 9.3, "Hors-jeux pm": 2.2},
    {"Équipe": "Augsbourg", "Tirs pm": 14.4, "Tacles pm": 15.0, "Interceptions pm": 13.4, "Fautes pm": 11.6, "Hors-jeux pm": 2.0},
    {"Équipe": "Hoffenheim", "Tirs pm": 11.2, "Tacles pm": 14.9, "Interceptions pm": 12.7, "Fautes pm": 13.0, "Hors-jeux pm": 1.4},
    {"Équipe": "B. M'Gladbach", "Tirs pm": 13.4, "Tacles pm": 14.8, "Interceptions pm": 12.8, "Fautes pm": 11.6, "Hors-jeux pm": 2.4},
    {"Équipe": "Union Berlin", "Tirs pm": 10.8, "Tacles pm": 14.8, "Interceptions pm": 11.9, "Fautes pm": 12.8, "Hors-jeux pm": 2.5},
    {"Équipe": "B. Leverkusen", "Tirs pm": 10.2, "Tacles pm": 14.7, "Interceptions pm": 9.5, "Fautes pm": 12.7, "Hors-jeux pm": 2.1},
    {"Équipe": "VfB Stuttgart", "Tirs pm": 13.4, "Tacles pm": 14.6, "Interceptions pm": 12.5, "Fautes pm": 12.4, "Hors-jeux pm": 1.6},
    {"Équipe": "Hertha Berlin", "Tirs pm": 12.6, "Tacles pm": 14.3, "Interceptions pm": 12.3, "Fautes pm": 13.8, "Hors-jeux pm": 2.3},
    {"Équipe": "RB Leipzig", "Tirs pm": 7.8, "Tacles pm": 13.5, "Interceptions pm": 10.4, "Fautes pm": 12.8, "Hors-jeux pm": 2.4},
    {"Équipe": "Fribourg", "Tirs pm": 14.8, "Tacles pm": 12.7, "Interceptions pm": 13.3, "Fautes pm": 12.9, "Hors-jeux pm": 1.6}
]



data_23_24 = [
    {"Équipe": "Eintr. Francfort", "Tirs pm": 12.9, "Tacles pm": 18.3, "Interceptions pm": 8.9, "Fautes pm": 10.8, "Hors-jeux pm": 1.6},
    {"Équipe": "FC Cologne", "Tirs pm": 15.2, "Tacles pm": 18.0, "Interceptions pm": 9.5, "Fautes pm": 11.8, "Hors-jeux pm": 1.6},
    {"Équipe": "VfL Bochum", "Tirs pm": 15.6, "Tacles pm": 17.9, "Interceptions pm": 9.2, "Fautes pm": 13.5, "Hors-jeux pm": 2.6},
    {"Équipe": "Werder Breme", "Tirs pm": 15.0, "Tacles pm": 17.6, "Interceptions pm": 8.0, "Fautes pm": 12.4, "Hors-jeux pm": 2.2},
    {"Équipe": "FC Heidenheim", "Tirs pm": 15.1, "Tacles pm": 17.6, "Interceptions pm": 9.2, "Fautes pm": 13.5, "Hors-jeux pm": 1.2},
    {"Équipe": "B. M'Gladbach", "Tirs pm": 17.0, "Tacles pm": 17.0, "Interceptions pm": 9.5, "Fautes pm": 10.1, "Hors-jeux pm": 1.7},
    {"Équipe": "Mayence", "Tirs pm": 12.3, "Tacles pm": 16.9, "Interceptions pm": 10.9, "Fautes pm": 13.9, "Hors-jeux pm": 1.7},
    {"Équipe": "Darmstadt", "Tirs pm": 16.9, "Tacles pm": 16.7, "Interceptions pm": 9.6, "Fautes pm": 13.0, "Hors-jeux pm": 1.7},
    {"Équipe": "Bor. Dortmund", "Tirs pm": 13.8, "Tacles pm": 16.3, "Interceptions pm": 7.7, "Fautes pm": 9.4, "Hors-jeux pm": 1.4},
    {"Équipe": "Union Berlin", "Tirs pm": 13.8, "Tacles pm": 16.1, "Interceptions pm": 9.1, "Fautes pm": 11.8, "Hors-jeux pm": 2.1},
    {"Équipe": "Fribourg", "Tirs pm": 15.2, "Tacles pm": 16.0, "Interceptions pm": 8.1, "Fautes pm": 10.7, "Hors-jeux pm": 1.7},
    {"Équipe": "Wolfsbourg", "Tirs pm": 13.1, "Tacles pm": 15.9, "Interceptions pm": 8.4, "Fautes pm": 12.9, "Hors-jeux pm": 1.3},
    {"Équipe": "VfB Stuttgart", "Tirs pm": 11.2, "Tacles pm": 15.9, "Interceptions pm": 8.3, "Fautes pm": 10.0, "Hors-jeux pm": 1.5},
    {"Équipe": "RB Leipzig", "Tirs pm": 11.3, "Tacles pm": 15.8, "Interceptions pm": 7.1, "Fautes pm": 10.4, "Hors-jeux pm": 2.0},
    {"Équipe": "Hoffenheim", "Tirs pm": 16.8, "Tacles pm": 15.4, "Interceptions pm": 10.3, "Fautes pm": 10.5, "Hors-jeux pm": 1.5},
    {"Équipe": "Bayern Munich", "Tirs pm": 9.7, "Tacles pm": 14.9, "Interceptions pm": 9.0, "Fautes pm": 9.4, "Hors-jeux pm": 1.7},
    {"Équipe": "Augsbourg", "Tirs pm": 14.3, "Tacles pm": 14.7, "Interceptions pm": 8.6, "Fautes pm": 13.3, "Hors-jeux pm": 2.5},
    {"Équipe": "B. Leverkusen", "Tirs pm": 8.4, "Tacles pm": 13.4, "Interceptions pm": 6.8, "Fautes pm": 8.9, "Hors-jeux pm": 2.0}
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
            "Tirs pm": [], "Tacles pm": [], "Interceptions pm": [], "Fautes pm": [], "Hors-jeux pm" : []
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
sorted_teams = sorted(team_results.items(), key=lambda x: x[1]["Tacles pm"], reverse=True)

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