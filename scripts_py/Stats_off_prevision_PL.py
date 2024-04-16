import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]



# Supprimer la collection si elle existe déjà
if "Stats_off_prevision_PL" in db.list_collection_names():
    db["Stats_off_prevision_PL"].drop()

# Recréer la collection
collection = db["Stats_off_prevision_PL"]

# Données pour les trois collections
data_22_23 = [
    {"Équipe": "Brighton", "Tirs pm": 16.1, "Tirs CA pm": 6.1, "Dribbles pm": 9.1, "Fautes subies pm": 10.7},
    {"Équipe": "Liverpool", "Tirs pm": 15.9, "Tirs CA pm": 5.6, "Dribbles pm": 8.5, "Fautes subies pm": 8.4},
    {"Équipe": "Manchest. City", "Tirs pm": 15.8, "Tirs CA pm": 5.8, "Dribbles pm": 8.8, "Fautes subies pm": 10.1},
    {"Équipe": "Arsenal", "Tirs pm": 15.6, "Tirs CA pm": 5.4, "Dribbles pm": 9.4, "Fautes subies pm": 11.4},
    {"Équipe": "Manchest. Utd", "Tirs pm": 15.6, "Tirs CA pm": 5.7, "Dribbles pm": 8.4, "Fautes subies pm": 7.8},
    {"Équipe": "Newcastle", "Tirs pm": 15.0, "Tirs CA pm": 5.2, "Dribbles pm": 9.2, "Fautes subies pm": 10.5},
    {"Équipe": "Tottenham", "Tirs pm": 13.6, "Tirs CA pm": 5.2, "Dribbles pm": 8.1, "Fautes subies pm": 9.3},
    {"Équipe": "Chelsea", "Tirs pm": 12.7, "Tirs CA pm": 4.2, "Dribbles pm": 10.0, "Fautes subies pm": 12.2},
    {"Équipe": "West Ham", "Tirs pm": 12.5, "Tirs CA pm": 3.8, "Dribbles pm": 6.6, "Fautes subies pm": 8.3},
    {"Équipe": "Leeds", "Tirs pm": 12.2, "Tirs CA pm": 3.8, "Dribbles pm": 7.1, "Fautes subies pm": 10.7},
    {"Équipe": "Aston Villa", "Tirs pm": 11.3, "Tirs CA pm": 4.0, "Dribbles pm": 7.7, "Fautes subies pm": 13.1},
    {"Équipe": "Everton", "Tirs pm": 11.3, "Tirs CA pm": 4.0, "Dribbles pm": 8.0, "Fautes subies pm": 10.1},
    {"Équipe": "Fulham", "Tirs pm": 11.3, "Tirs CA pm": 3.9, "Dribbles pm": 7.2, "Fautes subies pm": 10.7},
    {"Équipe": "Crystal Palace", "Tirs pm": 11.2, "Tirs CA pm": 3.6, "Dribbles pm": 9.7, "Fautes subies pm": 12.6},
    {"Équipe": "Leicester", "Tirs pm": 11.0, "Tirs CA pm": 4.0, "Dribbles pm": 7.7, "Fautes subies pm": 10.4},
    {"Équipe": "Southampton", "Tirs pm": 11.0, "Tirs CA pm": 3.7, "Dribbles pm": 9.5, "Fautes subies pm": 9.9},
    {"Équipe": "Wolverhampton", "Tirs pm": 10.8, "Tirs CA pm": 3.3, "Dribbles pm": 9.9, "Fautes subies pm": 10.2},
    {"Équipe": "Brentford", "Tirs pm": 10.7, "Tirs CA pm": 4.3, "Dribbles pm": 6.9, "Fautes subies pm": 10.5},
    {"Équipe": "Nottingham F.", "Tirs pm": 9.7, "Tirs CA pm": 3.1, "Dribbles pm": 6.7, "Fautes subies pm": 10.3},
    {"Équipe": "Bournemouth", "Tirs pm": 9.4, "Tirs CA pm": 3.5, "Dribbles pm": 8.4, "Fautes subies pm": 9.6}
]




data_21_22 = [
    {"Équipe": "Liverpool", "Tirs pm": 19.2, "Tirs CA pm": 6.7, "Dribbles pm": 9.0, "Fautes subies pm": 7.5},
    {"Équipe": "Manchest. City", "Tirs pm": 18.8, "Tirs CA pm": 6.7, "Dribbles pm": 11.2, "Fautes subies pm": 8.7},
    {"Équipe": "Chelsea", "Tirs pm": 15.6, "Tirs CA pm": 5.6, "Dribbles pm": 9.6, "Fautes subies pm": 10.3},
    {"Équipe": "Arsenal", "Tirs pm": 15.5, "Tirs CA pm": 5.2, "Dribbles pm": 8.4, "Fautes subies pm": 9.4},
    {"Équipe": "Manchest. Utd", "Tirs pm": 13.4, "Tirs CA pm": 4.9, "Dribbles pm": 9.7, "Fautes subies pm": 8.4},
    {"Équipe": "Brighton", "Tirs pm": 12.9, "Tirs CA pm": 4.0, "Dribbles pm": 8.3, "Fautes subies pm": 9.3},
    {"Équipe": "Leeds", "Tirs pm": 12.8, "Tirs CA pm": 4.1, "Dribbles pm": 8.9, "Fautes subies pm": 9.7},
    {"Équipe": "Tottenham", "Tirs pm": 12.8, "Tirs CA pm": 5.2, "Dribbles pm": 9.7, "Fautes subies pm": 10.5},
    {"Équipe": "Southampton", "Tirs pm": 12.7, "Tirs CA pm": 4.5, "Dribbles pm": 9.2, "Fautes subies pm": 9.6},
    {"Équipe": "Aston Villa", "Tirs pm": 12.2, "Tirs CA pm": 4.3, "Dribbles pm": 8.8, "Fautes subies pm": 13.3},
    {"Équipe": "West Ham", "Tirs pm": 11.8, "Tirs CA pm": 4.1, "Dribbles pm": 8.1, "Fautes subies pm": 7.3},
    {"Équipe": "Newcastle", "Tirs pm": 11.7, "Tirs CA pm": 3.9, "Dribbles pm": 9.4, "Fautes subies pm": 9.6},
    {"Équipe": "Brentford", "Tirs pm": 11.6, "Tirs CA pm": 4.1, "Dribbles pm": 6.9, "Fautes subies pm": 9.6},
    {"Équipe": "Everton", "Tirs pm": 11.5, "Tirs CA pm": 3.6, "Dribbles pm": 8.9, "Fautes subies pm": 10.2},
    {"Équipe": "Leicester", "Tirs pm": 11.4, "Tirs CA pm": 4.4, "Dribbles pm": 7.8, "Fautes subies pm": 10.4},
    {"Équipe": "Crystal Palace", "Tirs pm": 10.8, "Tirs CA pm": 3.9, "Dribbles pm": 10.1, "Fautes subies pm": 12.7},
    {"Équipe": "Burnley", "Tirs pm": 10.7, "Tirs CA pm": 3.3, "Dribbles pm": 6.8, "Fautes subies pm": 8.7},
    {"Équipe": "Wolverhampton", "Tirs pm": 10.6, "Tirs CA pm": 3.6, "Dribbles pm": 11.8, "Fautes subies pm": 8.8},
    {"Équipe": "Watford", "Tirs pm": 10.5, "Tirs CA pm": 3.3, "Dribbles pm": 10.4, "Fautes subies pm": 9.3},
    {"Équipe": "Norwich", "Tirs pm": 9.8, "Tirs CA pm": 2.9, "Dribbles pm": 7.8, "Fautes subies pm": 11.3}
]


data_20_21 = [
    {"Équipe": "Liverpool", "Tirs pm": 16.0, "Tirs CA pm": 5.6, "Dribbles pm": 10.7, "Fautes subies pm": 8.3},
    {"Équipe": "Manchest. City", "Tirs pm": 15.4, "Tirs CA pm": 5.5, "Dribbles pm": 12.5, "Fautes subies pm": 9.7},
    {"Équipe": "Chelsea", "Tirs pm": 14.6, "Tirs CA pm": 5.5, "Dribbles pm": 9.4, "Fautes subies pm": 9.6},
    {"Équipe": "Manchest. Utd", "Tirs pm": 13.8, "Tirs CA pm": 5.6, "Dribbles pm": 10.9, "Fautes subies pm": 10.5},
    {"Équipe": "Leeds", "Tirs pm": 13.7, "Tirs CA pm": 5.2, "Dribbles pm": 8.4, "Fautes subies pm": 10.8},
    {"Équipe": "Aston Villa", "Tirs pm": 13.7, "Tirs CA pm": 4.9, "Dribbles pm": 9.7, "Fautes subies pm": 14.6},
    {"Équipe": "Brighton", "Tirs pm": 12.8, "Tirs CA pm": 3.8, "Dribbles pm": 9.2, "Fautes subies pm": 9.4},
    {"Équipe": "Leicester", "Tirs pm": 12.8, "Tirs CA pm": 4.9, "Dribbles pm": 9.4, "Fautes subies pm": 11.1},
    {"Équipe": "West Ham", "Tirs pm": 12.3, "Tirs CA pm": 4.3, "Dribbles pm": 7.8, "Fautes subies pm": 9.3},
    {"Équipe": "Arsenal", "Tirs pm": 12.1, "Tirs CA pm": 4.0, "Dribbles pm": 7.8, "Fautes subies pm": 10.5},
    {"Équipe": "Wolverhampton", "Tirs pm": 12.0, "Tirs CA pm": 4.0, "Dribbles pm": 12.2, "Fautes subies pm": 9.9},
    {"Équipe": "Tottenham", "Tirs pm": 11.7, "Tirs CA pm": 4.6, "Dribbles pm": 10.6, "Fautes subies pm": 12.3},
    {"Équipe": "Fulham", "Tirs pm": 11.6, "Tirs CA pm": 3.6, "Dribbles pm": 13.3, "Fautes subies pm": 9.8},
    {"Équipe": "Southampton", "Tirs pm": 11.2, "Tirs CA pm": 4.4, "Dribbles pm": 9.0, "Fautes subies pm": 11.1},
    {"Équipe": "Everton", "Tirs pm": 10.5, "Tirs CA pm": 3.9, "Dribbles pm": 9.5, "Fautes subies pm": 10.3},
    {"Équipe": "Newcastle", "Tirs pm": 10.4, "Tirs CA pm": 3.7, "Dribbles pm": 9.2, "Fautes subies pm": 11.1},
    {"Équipe": "Burnley", "Tirs pm": 10.1, "Tirs CA pm": 3.4, "Dribbles pm": 6.2, "Fautes subies pm": 10.2},
    {"Équipe": "Crystal Palace", "Tirs pm": 9.2, "Tirs CA pm": 3.5, "Dribbles pm": 9.6, "Fautes subies pm": 11.5},
    {"Équipe": "West Bromwich Albion", "Tirs pm": 8.9, "Tirs CA pm": 2.9, "Dribbles pm": 6.9, "Fautes subies pm": 10.8},
    {"Équipe": "Sheffield Utd", "Tirs pm": 8.5, "Tirs CA pm": 2.6, "Dribbles pm": 7.1, "Fautes subies pm": 7.7}
]



data_23_24 = [
    {"Équipe": "Liverpool", "Tirs pm": 19.7, "Tirs CA pm": 6.9, "Dribbles pm": 8.7, "Fautes subies pm": 10.3},
    {"Équipe": "Manchest. City", "Tirs pm": 17.8, "Tirs CA pm": 6.8, "Dribbles pm": 10.6, "Fautes subies pm": 10.8},
    {"Équipe": "Arsenal", "Tirs pm": 16.7, "Tirs CA pm": 5.9, "Dribbles pm": 7.5, "Fautes subies pm": 10.5},
    {"Équipe": "Tottenham", "Tirs pm": 15.5, "Tirs CA pm": 5.8, "Dribbles pm": 10.0, "Fautes subies pm": 13.7},
    {"Équipe": "Brighton", "Tirs pm": 14.8, "Tirs CA pm": 5.9, "Dribbles pm": 7.8, "Fautes subies pm": 12.4},
    {"Équipe": "Aston Villa", "Tirs pm": 14.4, "Tirs CA pm": 5.5, "Dribbles pm": 9.5, "Fautes subies pm": 12.6},
    {"Équipe": "Manchest. Utd", "Tirs pm": 14.2, "Tirs CA pm": 4.9, "Dribbles pm": 8.1, "Fautes subies pm": 9.0},
    {"Équipe": "Bournemouth", "Tirs pm": 14.2, "Tirs CA pm": 5.0, "Dribbles pm": 10.2, "Fautes subies pm": 9.9},
    {"Équipe": "Everton", "Tirs pm": 13.9, "Tirs CA pm": 4.6, "Dribbles pm": 6.7, "Fautes subies pm": 9.2},
    {"Équipe": "Chelsea", "Tirs pm": 13.9, "Tirs CA pm": 5.4, "Dribbles pm": 10.1, "Fautes subies pm": 11.5},
    {"Équipe": "Newcastle", "Tirs pm": 13.9, "Tirs CA pm": 5.3, "Dribbles pm": 8.9, "Fautes subies pm": 12.6},
    {"Équipe": "Fulham", "Tirs pm": 13.3, "Tirs CA pm": 4.8, "Dribbles pm": 7.2, "Fautes subies pm": 9.8},
    {"Équipe": "Brentford", "Tirs pm": 13.1, "Tirs CA pm": 4.5, "Dribbles pm": 6.0, "Fautes subies pm": 11.0},
    {"Équipe": "West Ham", "Tirs pm": 11.9, "Tirs CA pm": 4.0, "Dribbles pm": 8.9, "Fautes subies pm": 10.3},
    {"Équipe": "Wolverhampton", "Tirs pm": 11.8, "Tirs CA pm": 4.2, "Dribbles pm": 11.4, "Fautes subies pm": 10.5},
    {"Équipe": "Luton Town", "Tirs pm": 11.7, "Tirs CA pm": 3.6, "Dribbles pm": 9.6, "Fautes subies pm": 11.8},
    {"Équipe": "Crystal Palace", "Tirs pm": 11.7, "Tirs CA pm": 4.1, "Dribbles pm": 8.7, "Fautes subies pm": 11.8},
    {"Équipe": "Nottingham F.", "Tirs pm": 11.2, "Tirs CA pm": 3.7, "Dribbles pm": 7.3, "Fautes subies pm": 9.8},
    {"Équipe": "Burnley", "Tirs pm": 10.9, "Tirs CA pm": 3.5, "Dribbles pm": 8.2, "Fautes subies pm": 9.6},
    {"Équipe": "Sheffield Utd", "Tirs pm": 9.0, "Tirs CA pm": 3.1, "Dribbles pm": 5.9, "Fautes subies pm": 8.6}
]

# Manchest. City, Nottingham F., Sheffield Utd, Luton Town, Manchest. Utd, Wolverhampton

#FC Barcelone, Gérone FC, Athletic Bilbao, Atl. Madrid, Betis Séville, FC Valence,
# UD Las Palmas, Alaves, Real Majorque, FC Seville, Cadix, Grenade

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