import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]



# Supprimer la collection si elle existe déjà
if "Stats_gen_prevision_PL" in db.list_collection_names():
    db["Stats_gen_prevision_PL"].drop()

# Recréer la collection
collection = db["Stats_gen_prevision_PL"]

# Données pour les trois collections
data_23_24 = [
    {"Équipe": "Manchest. City", "Buts": 63, "Tirs pm": 17.8, "Possession%": 65.5, "PassesRéussies%": 90, "AériensGagnés": 9.4},
    {"Équipe": "Liverpool", "Buts": 67, "Tirs pm": 19.7, "Possession%": 59.9, "PassesRéussies%": 85.2, "AériensGagnés": 15.9},
    {"Équipe": "Arsenal", "Buts": 70, "Tirs pm": 16.7, "Possession%": 60.7, "PassesRéussies%": 87, "AériensGagnés": 13.6},
    {"Équipe": "Tottenham", "Buts": 61, "Tirs pm": 15.5, "Possession%": 61.5, "PassesRéussies%": 87.2, "AériensGagnés": 10},
    {"Équipe": "Newcastle", "Buts": 63, "Tirs pm": 13.9, "Possession%": 53, "PassesRéussies%": 83.2, "AériensGagnés": 11.9},
    {"Équipe": "West Ham", "Buts": 49, "Tirs pm": 11.9, "Possession%": 40.9, "PassesRéussies%": 78.6, "AériensGagnés": 15.9},
    {"Équipe": "Aston Villa", "Buts": 62, "Tirs pm": 14.4, "Possession%": 54.7, "PassesRéussies%": 85.8, "AériensGagnés": 9.3},
    {"Équipe": "Chelsea", "Buts": 49, "Tirs pm": 13.9, "Possession%": 58.9, "PassesRéussies%": 87.2, "AériensGagnés": 11.8},
    {"Équipe": "Manchest. Utd", "Buts": 40, "Tirs pm": 14.2, "Possession%": 50.1, "PassesRéussies%": 82.6, "AériensGagnés": 11.7},
    {"Équipe": "Everton", "Buts": 30, "Tirs pm": 13.9, "Possession%": 39.9, "PassesRéussies%": 75.5, "AériensGagnés": 18.3},
    {"Équipe": "Wolverhampton", "Buts": 42, "Tirs pm": 11.8, "Possession%": 47.7, "PassesRéussies%": 81.8, "AériensGagnés": 12.1},
    {"Équipe": "Crystal Palace", "Buts": 34, "Tirs pm": 11.7, "Possession%": 41.1, "PassesRéussies%": 78.9, "AériensGagnés": 15.2},
    {"Équipe": "Fulham", "Buts": 46, "Tirs pm": 13.3, "Possession%": 50.3, "PassesRéussies%": 82.7, "AériensGagnés": 13.3},
    {"Équipe": "Bournemouth", "Buts": 43, "Tirs pm": 14.2, "Possession%": 44.5, "PassesRéussies%": 77.1, "AériensGagnés": 16},
    {"Équipe": "Brighton", "Buts": 51, "Tirs pm": 14.8, "Possession%": 62.3, "PassesRéussies%": 89.1, "AériensGagnés": 11.4},
    {"Équipe": "Brentford", "Buts": 42, "Tirs pm": 13.1, "Possession%": 44.4, "PassesRéussies%": 76.4, "AériensGagnés": 17.3},
    {"Équipe": "Luton Town", "Buts": 43, "Tirs pm": 11.7, "Possession%": 41.3, "PassesRéussies%": 75.1, "AériensGagnés": 17.2},
    {"Équipe": "Nottingham F.", "Buts": 36, "Tirs pm": 11.2, "Possession%": 40.4, "PassesRéussies%": 78.2, "AériensGagnés": 16},
    {"Équipe": "Burnley", "Buts": 31, "Tirs pm": 10.9, "Possession%": 45.6, "PassesRéussies%": 79, "AériensGagnés": 15.8},
    {"Équipe": "Sheffield Utd", "Buts": 27, "Tirs pm": 9, "Possession%": 34.5, "PassesRéussies%": 71.3, "AériensGagnés": 16.7}
]



data_21_22 = [
    {"Équipe": "Manchest. City", "Buts": 99, "Tirs pm": 18.8, "Possession%": 68.2, "PassesRéussies%": 89.7, "AériensGagnés": 12.7},
    {"Équipe": "Liverpool", "Buts": 94, "Tirs pm": 19.2, "Possession%": 63.1, "PassesRéussies%": 84.9, "AériensGagnés": 15.1},
    {"Équipe": "Chelsea", "Buts": 76, "Tirs pm": 15.6, "Possession%": 62.2, "PassesRéussies%": 87.1, "AériensGagnés": 14.3},
    {"Équipe": "Tottenham", "Buts": 69, "Tirs pm": 12.8, "Possession%": 51.6, "PassesRéussies%": 84.9, "AériensGagnés": 14.7},
    {"Équipe": "West Ham", "Buts": 60, "Tirs pm": 11.8, "Possession%": 47.4, "PassesRéussies%": 80.6, "AériensGagnés": 16.9},
    {"Équipe": "Arsenal", "Buts": 61, "Tirs pm": 15.5, "Possession%": 52.6, "PassesRéussies%": 83.4, "AériensGagnés": 12.4},
    {"Équipe": "Manchest. Utd", "Buts": 57, "Tirs pm": 13.4, "Possession%": 52.1, "PassesRéussies%": 82.8, "AériensGagnés": 14.7},
    {"Équipe": "Crystal Palace", "Buts": 50, "Tirs pm": 10.8, "Possession%": 50.8, "PassesRéussies%": 80.3, "AériensGagnés": 16.1},
    {"Équipe": "Leicester", "Buts": 62, "Tirs pm": 11.4, "Possession%": 51.8, "PassesRéussies%": 81.8, "AériensGagnés": 14},
    {"Équipe": "Brighton", "Buts": 42, "Tirs pm": 12.9, "Possession%": 54.3, "PassesRéussies%": 81.7, "AériensGagnés": 15.1},
    {"Équipe": "Wolverhampton", "Buts": 38, "Tirs pm": 10.6, "Possession%": 49.3, "PassesRéussies%": 81.3, "AériensGagnés": 12.2},
    {"Équipe": "Aston Villa", "Buts": 52, "Tirs pm": 12.2, "Possession%": 46.3, "PassesRéussies%": 79.7, "AériensGagnés": 14.3},
    {"Équipe": "Burnley", "Buts": 34, "Tirs pm": 10.7, "Possession%": 39.1, "PassesRéussies%": 69.2, "AériensGagnés": 21.9},
    {"Équipe": "Brentford", "Buts": 48, "Tirs pm": 11.6, "Possession%": 44.3, "PassesRéussies%": 73.7, "AériensGagnés": 19},
    {"Équipe": "Southampton", "Buts": 43, "Tirs pm": 12.7, "Possession%": 47.4, "PassesRéussies%": 76.6, "AériensGagnés": 17.8},
    {"Équipe": "Newcastle", "Buts": 43, "Tirs pm": 11.7, "Possession%": 39.4, "PassesRéussies%": 74.5, "AériensGagnés": 17.5},
    {"Équipe": "Everton", "Buts": 43, "Tirs pm": 11.5, "Possession%": 39.1, "PassesRéussies%": 73.3, "AériensGagnés": 16.8},
    {"Équipe": "Leeds", "Buts": 42, "Tirs pm": 12.8, "Possession%": 51.9, "PassesRéussies%": 78, "AériensGagnés": 12.6},
    {"Équipe": "Watford", "Buts": 34, "Tirs pm": 10.5, "Possession%": 39.7, "PassesRéussies%": 72.9, "AériensGagnés": 18.9},
    {"Équipe": "Norwich", "Buts": 23, "Tirs pm": 9.8, "Possession%": 42.3, "PassesRéussies%": 77.5, "AériensGagnés": 13}
]




data_20_21 = [
    {"Équipe": "Manchest. City", "Buts": 80, "Tirs pm": 15.4, "Possession%": 63.7, "PassesRéussies%": 89.3, "AériensGagnés": 12.6},
    {"Équipe": "Manchest. Utd", "Buts": 73, "Tirs pm": 13.8, "Possession%": 55.6, "PassesRéussies%": 84.8, "AériensGagnés": 14.5},
    {"Équipe": "Aston Villa", "Buts": 55, "Tirs pm": 13.7, "Possession%": 47.8, "PassesRéussies%": 78.6, "AériensGagnés": 19.4},
    {"Équipe": "Chelsea", "Buts": 58, "Tirs pm": 14.6, "Possession%": 61.2, "PassesRéussies%": 87, "AériensGagnés": 15.2},
    {"Équipe": "Liverpool", "Buts": 68, "Tirs pm": 16, "Possession%": 62.4, "PassesRéussies%": 85.7, "AériensGagnés": 14.3},
    {"Équipe": "Tottenham", "Buts": 68, "Tirs pm": 11.7, "Possession%": 51.4, "PassesRéussies%": 81.8, "AériensGagnés": 16.4},
    {"Équipe": "Leicester", "Buts": 68, "Tirs pm": 12.8, "Possession%": 54.4, "PassesRéussies%": 82.1, "AériensGagnés": 16.2},
    {"Équipe": "Leeds", "Buts": 62, "Tirs pm": 13.7, "Possession%": 57.8, "PassesRéussies%": 80.8, "AériensGagnés": 14.5},
    {"Équipe": "West Ham", "Buts": 62, "Tirs pm": 12.3, "Possession%": 42.5, "PassesRéussies%": 77.8, "AériensGagnés": 19.9},
    {"Équipe": "Everton", "Buts": 47, "Tirs pm": 10.5, "Possession%": 46.5, "PassesRéussies%": 81.4, "AériensGagnés": 17.7},
    {"Équipe": "Arsenal", "Buts": 55, "Tirs pm": 12.1, "Possession%": 53.4, "PassesRéussies%": 85, "AériensGagnés": 13.5},
    {"Équipe": "Wolverhampton", "Buts": 35, "Tirs pm": 12, "Possession%": 49.2, "PassesRéussies%": 83.1, "AériensGagnés": 15.2},
    {"Équipe": "Brighton", "Buts": 40, "Tirs pm": 12.8, "Possession%": 50.8, "PassesRéussies%": 81.3, "AériensGagnés": 14.2},
    {"Équipe": "Burnley", "Buts": 33, "Tirs pm": 10.1, "Possession%": 41.5, "PassesRéussies%": 71.6, "AériensGagnés": 23.4},
    {"Équipe": "Fulham", "Buts": 27, "Tirs pm": 11.6, "Possession%": 49.5, "PassesRéussies%": 81.2, "AériensGagnés": 17.2},
    {"Équipe": "Southampton", "Buts": 47, "Tirs pm": 11.2, "Possession%": 51.7, "PassesRéussies%": 79.3, "AériensGagnés": 14.1},
    {"Équipe": "Newcastle", "Buts": 46, "Tirs pm": 10.4, "Possession%": 38, "PassesRéussies%": 76, "AériensGagnés": 17.1},
    {"Équipe": "Crystal Palace", "Buts": 41, "Tirs pm": 9.2, "Possession%": 39.9, "PassesRéussies%": 76.1, "AériensGagnés": 18.3},
    {"Équipe": "West Bromwich Albion", "Buts": 35, "Tirs pm": 8.9, "Possession%": 37, "PassesRéussies%": 72.2, "AériensGagnés": 19.1},
    {"Équipe": "Sheffield Utd", "Buts": 20, "Tirs pm": 8.5, "Possession%": 41, "PassesRéussies%": 76.9, "AériensGagnés": 19.1}
]




data_22_23 = [
    {"Équipe": "Manchest. City", "Buts": 94, "Tirs pm": 15.8, "Possession%": 65.2, "PassesRéussies%": 89.2, "AériensGagnés": 11.6},
    {"Équipe": "Arsenal", "Buts": 88, "Tirs pm": 15.6, "Possession%": 59.7, "PassesRéussies%": 85.4, "AériensGagnés": 12.9},
    {"Équipe": "Newcastle", "Buts": 68, "Tirs pm": 15, "Possession%": 52.2, "PassesRéussies%": 79.8, "AériensGagnés": 14.7},
    {"Équipe": "Manchest. Utd", "Buts": 58, "Tirs pm": 15.6, "Possession%": 53.8, "PassesRéussies%": 82.3, "AériensGagnés": 12.3},
    {"Équipe": "Liverpool", "Buts": 75, "Tirs pm": 15.9, "Possession%": 60.6, "PassesRéussies%": 84.2, "AériensGagnés": 12.7},
    {"Équipe": "Brighton", "Buts": 72, "Tirs pm": 16.1, "Possession%": 60.5, "PassesRéussies%": 85.9, "AériensGagnés": 11.7},
    {"Équipe": "Tottenham", "Buts": 70, "Tirs pm": 13.6, "Possession%": 49.8, "PassesRéussies%": 83.4, "AériensGagnés": 14.6},
    {"Équipe": "Brentford", "Buts": 58, "Tirs pm": 10.7, "Possession%": 43.3, "PassesRéussies%": 74.8, "AériensGagnés": 17.6},
    {"Équipe": "Aston Villa", "Buts": 51, "Tirs pm": 11.3, "Possession%": 49.2, "PassesRéussies%": 81.1, "AériensGagnés": 11.7},
    {"Équipe": "Chelsea", "Buts": 38, "Tirs pm": 12.7, "Possession%": 58.8, "PassesRéussies%": 85.8, "AériensGagnés": 13.1},
    {"Équipe": "Everton", "Buts": 34, "Tirs pm": 11.3, "Possession%": 42.5, "PassesRéussies%": 77, "AériensGagnés": 15.3},
    {"Équipe": "Crystal Palace", "Buts": 40, "Tirs pm": 11.2, "Possession%": 45.8, "PassesRéussies%": 79.3, "AériensGagnés": 13.1},
    {"Équipe": "Fulham", "Buts": 55, "Tirs pm": 11.3, "Possession%": 48.6, "PassesRéussies%": 80, "AériensGagnés": 13.5},
    {"Équipe": "West Ham", "Buts": 42, "Tirs pm": 12.5, "Possession%": 41.4, "PassesRéussies%": 78.2, "AériensGagnés": 16.4},
    {"Équipe": "Leicester", "Buts": 51, "Tirs pm": 11, "Possession%": 47.7, "PassesRéussies%": 80.3, "AériensGagnés": 13.5},
    {"Équipe": "Bournemouth", "Buts": 37, "Tirs pm": 9.4, "Possession%": 40, "PassesRéussies%": 77.5, "AériensGagnés": 13.8},
    {"Équipe": "Wolverhampton", "Buts": 31, "Tirs pm": 10.8, "Possession%": 50.1, "PassesRéussies%": 81.3, "AériensGagnés": 12.3},
    {"Équipe": "Southampton", "Buts": 36, "Tirs pm": 11, "Possession%": 44.1, "PassesRéussies%": 77.3, "AériensGagnés": 14.6},
    {"Équipe": "Leeds", "Buts": 48, "Tirs pm": 12.2, "Possession%": 46.3, "PassesRéussies%": 74.9, "AériensGagnés": 15.5},
    {"Équipe": "Nottingham F.", "Buts": 38, "Tirs pm": 9.7, "Possession%": 37.2, "PassesRéussies%": 72.3, "AériensGagnés": 14.8}
]

# Manchest. City, Nottingham F., Sheffield Utd, Luton Town, Manchest. Utd, Wolverhampton

#FC Barcelone, Gérone FC, Athletic Bilbao, Atl. Madrid, Betis Séville, FC Valence, UD Las Palmas, Alaves, Real Majorque, FC Seville, Cadix, Grenade

# Fusionner les données des trois collections
all_data = data_23_24 + data_22_23 + data_21_22 + data_20_21

# Initialiser un dictionnaire pour stocker les résultats
team_results = {}

# Calculer les moyennes pour chaque équipe
for team_data in all_data:
    team_name = team_data["Équipe"]
    if team_name not in team_results:
        team_results[team_name] = {
            "Buts": [], "Tirs pm": [], "Possession%": [], "PassesRéussies%": [], "AériensGagnés": []
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
sorted_teams = sorted(team_results.items(), key=lambda x: x[1]["Buts"], reverse=True)

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