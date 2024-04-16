import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]



# Supprimer la collection si elle existe déjà
if "Stats_def_prevision_PL" in db.list_collection_names():
    db["Stats_def_prevision_PL"].drop()

# Recréer la collection
collection = db["Stats_def_prevision_PL"]

# Données pour les trois collections
data_22_23 = [
    {"Équipe": "Leeds", "Tirs pm": 13.9, "Tacles pm": 22.1, "Interceptions pm": 10.3, "Fautes pm": 12.3, "Hors-jeux pm": 1.6},
    {"Équipe": "Chelsea", "Tirs pm": 11.5, "Tacles pm": 19.5, "Interceptions pm": 9, "Fautes pm": 10.4, "Hors-jeux pm": 1.9},
    {"Équipe": "Southampton", "Tirs pm": 13.8, "Tacles pm": 18.8, "Interceptions pm": 11.3, "Fautes pm": 11.3, "Hors-jeux pm": 1.5},
    {"Équipe": "Everton", "Tirs pm": 15.2, "Tacles pm": 18.6, "Interceptions pm": 10.3, "Fautes pm": 10.4, "Hors-jeux pm": 1.7},
    {"Équipe": "Leicester", "Tirs pm": 15.1, "Tacles pm": 18.4, "Interceptions pm": 9.7, "Fautes pm": 10.8, "Hors-jeux pm": 2.2},
    {"Équipe": "Crystal Palace", "Tirs pm": 12, "Tacles pm": 18.2, "Interceptions pm": 9.4, "Fautes pm": 11.7, "Hors-jeux pm": 1.5},
    {"Équipe": "Wolverhampton", "Tirs pm": 14.6, "Tacles pm": 17.4, "Interceptions pm": 7.3, "Fautes pm": 12.2, "Hors-jeux pm": 1.6},
    {"Équipe": "Nottingham F.", "Tirs pm": 14.7, "Tacles pm": 17.3, "Interceptions pm": 9.2, "Fautes pm": 11.7, "Hors-jeux pm": 1.8},
    {"Équipe": "Manchest. Utd", "Tirs pm": 12.7, "Tacles pm": 17.3, "Interceptions pm": 9.3, "Fautes pm": 11.2, "Hors-jeux pm": 2.1},
    {"Équipe": "Aston Villa", "Tirs pm": 11.3, "Tacles pm": 16.7, "Interceptions pm": 8.5, "Fautes pm": 11, "Hors-jeux pm": 1.5},
    {"Équipe": "Fulham", "Tirs pm": 13.3, "Tacles pm": 16.5, "Interceptions pm": 8.9, "Fautes pm": 10.8, "Hors-jeux pm": 1.6},
    {"Équipe": "Bournemouth", "Tirs pm": 16.5, "Tacles pm": 16.3, "Interceptions pm": 9.3, "Fautes pm": 10.3, "Hors-jeux pm": 1},
    {"Équipe": "Tottenham", "Tirs pm": 13.7, "Tacles pm": 16.2, "Interceptions pm": 8.9, "Fautes pm": 11.2, "Hors-jeux pm": 1.8},
    {"Équipe": "Brighton", "Tirs pm": 10.2, "Tacles pm": 16.2, "Interceptions pm": 8, "Fautes pm": 11.2, "Hors-jeux pm": 1.8},
    {"Équipe": "West Ham", "Tirs pm": 13.1, "Tacles pm": 16, "Interceptions pm": 10.7, "Fautes pm": 9.5, "Hors-jeux pm": 1.6},
    {"Équipe": "Newcastle", "Tirs pm": 10.2, "Tacles pm": 16, "Interceptions pm": 8.8, "Fautes pm": 10.7, "Hors-jeux pm": 1.9},
    {"Équipe": "Liverpool", "Tirs pm": 9.7, "Tacles pm": 15.5, "Interceptions pm": 8.8, "Fautes pm": 10.7, "Hors-jeux pm": 2.2},
    {"Équipe": "Brentford", "Tirs pm": 14.7, "Tacles pm": 15.4, "Interceptions pm": 9, "Fautes pm": 9.3, "Hors-jeux pm": 1.9},
    {"Équipe": "Arsenal", "Tirs pm": 9, "Tacles pm": 14.9, "Interceptions pm": 6.2, "Fautes pm": 9.8, "Hors-jeux pm": 1.5},
    {"Équipe": "Manchest. City", "Tirs pm": 7.7, "Tacles pm": 12.4, "Interceptions pm": 5.9, "Fautes pm": 9.1, "Hors-jeux pm": 1.3}
]



data_21_22 = [
    {"Équipe": "Leeds", "Tirs pm": 15.3, "Tacles pm": 20.7, "Interceptions pm": 10.4, "Fautes pm": 12.3, "Hors-jeux pm": 1.7},
    {"Équipe": "Everton", "Tirs pm": 13.7, "Tacles pm": 18.6, "Interceptions pm": 9.6, "Fautes pm": 9.7, "Hors-jeux pm": 1.4},
    {"Équipe": "Leicester", "Tirs pm": 14.9, "Tacles pm": 18.2, "Interceptions pm": 9.7, "Fautes pm": 9.4, "Hors-jeux pm": 1.6},
    {"Équipe": "Wolverhampton", "Tirs pm": 13.4, "Tacles pm": 17.7, "Interceptions pm": 10.2, "Fautes pm": 9.6, "Hors-jeux pm": 1.6},
    {"Équipe": "Brighton", "Tirs pm": 12.5, "Tacles pm": 17.6, "Interceptions pm": 9.4, "Fautes pm": 10.3, "Hors-jeux pm": 1.4},
    {"Équipe": "Newcastle", "Tirs pm": 13.8, "Tacles pm": 16.9, "Interceptions pm": 9.4, "Fautes pm": 10.1, "Hors-jeux pm": 1.9},
    {"Équipe": "Crystal Palace", "Tirs pm": 11.7, "Tacles pm": 16.7, "Interceptions pm": 8.8, "Fautes pm": 10.9, "Hors-jeux pm": 1.7},
    {"Équipe": "Aston Villa", "Tirs pm": 12.6, "Tacles pm": 16.7, "Interceptions pm": 8.9, "Fautes pm": 10.6, "Hors-jeux pm": 1.4},
    {"Équipe": "Chelsea", "Tirs pm": 9, "Tacles pm": 16.3, "Interceptions pm": 9.1, "Fautes pm": 10.8, "Hors-jeux pm": 1.8},
    {"Équipe": "Watford", "Tirs pm": 14.8, "Tacles pm": 16.2, "Interceptions pm": 11.7, "Fautes pm": 11.6, "Hors-jeux pm": 1.4},
    {"Équipe": "Norwich", "Tirs pm": 16.6, "Tacles pm": 16.2, "Interceptions pm": 10, "Fautes pm": 9.4, "Hors-jeux pm": 1.4},
    {"Équipe": "Brentford", "Tirs pm": 13.2, "Tacles pm": 16, "Interceptions pm": 10.3, "Fautes pm": 9.8, "Hors-jeux pm": 1.7},
    {"Équipe": "Southampton", "Tirs pm": 13, "Tacles pm": 15.8, "Interceptions pm": 11.7, "Fautes pm": 10.6, "Hors-jeux pm": 1.1},
    {"Équipe": "Burnley", "Tirs pm": 15.9, "Tacles pm": 15.5, "Interceptions pm": 10.8, "Fautes pm": 10.4, "Hors-jeux pm": 2.7},
    {"Équipe": "Manchest. Utd", "Tirs pm": 13.4, "Tacles pm": 15.4, "Interceptions pm": 8.9, "Fautes pm": 10.4, "Hors-jeux pm": 2.3},
    {"Équipe": "Tottenham", "Tirs pm": 12.9, "Tacles pm": 15.3, "Interceptions pm": 9.3, "Fautes pm": 10.1, "Hors-jeux pm": 1.6},
    {"Équipe": "Liverpool", "Tirs pm": 7.8, "Tacles pm": 14.6, "Interceptions pm": 9, "Fautes pm": 9.6, "Hors-jeux pm": 1.7},
    {"Équipe": "Arsenal", "Tirs pm": 11.2, "Tacles pm": 14.2, "Interceptions pm": 7.8, "Fautes pm": 9.6, "Hors-jeux pm": 1.7},
    {"Équipe": "West Ham", "Tirs pm": 14.4, "Tacles pm": 14.1, "Interceptions pm": 9.8, "Fautes pm": 8.5, "Hors-jeux pm": 1.6},
    {"Équipe": "Manchest. City", "Tirs pm": 6.2, "Tacles pm": 13.1, "Interceptions pm": 7.3, "Fautes pm": 8.4, "Hors-jeux pm": 1.7}
]



data_20_21 = [
    {"Équipe": "Leeds", "Tirs pm": 14.7, "Tacles pm": 19.5, "Interceptions pm": 10.4, "Fautes pm": 11.4, "Hors-jeux pm": 2},
    {"Équipe": "Southampton", "Tirs pm": 11.3, "Tacles pm": 19.1, "Interceptions pm": 10.6, "Fautes pm": 11.3, "Hors-jeux pm": 2},
    {"Équipe": "Leicester", "Tirs pm": 9.7, "Tacles pm": 17.9, "Interceptions pm": 11.5, "Fautes pm": 10.9, "Hors-jeux pm": 1.9},
    {"Équipe": "Everton", "Tirs pm": 13.3, "Tacles pm": 16.8, "Interceptions pm": 10.3, "Fautes pm": 10.2, "Hors-jeux pm": 1.7},
    {"Équipe": "Crystal Palace", "Tirs pm": 14.2, "Tacles pm": 16.8, "Interceptions pm": 10.4, "Fautes pm": 10.8, "Hors-jeux pm": 1.8},
    {"Équipe": "Tottenham", "Tirs pm": 12.8, "Tacles pm": 16.7, "Interceptions pm": 9, "Fautes pm": 11.6, "Hors-jeux pm": 1.2},
    {"Équipe": "Sheffield Utd", "Tirs pm": 14.3, "Tacles pm": 16.7, "Interceptions pm": 9.2, "Fautes pm": 12.1, "Hors-jeux pm": 1.9},
    {"Équipe": "Brighton", "Tirs pm": 9.4, "Tacles pm": 16.5, "Interceptions pm": 11.2, "Fautes pm": 11.3, "Hors-jeux pm": 1.4},
    {"Équipe": "Chelsea", "Tirs pm": 8.8, "Tacles pm": 16.1, "Interceptions pm": 10.1, "Fautes pm": 11.4, "Hors-jeux pm": 2.1},
    {"Équipe": "Fulham", "Tirs pm": 11.3, "Tacles pm": 15.8, "Interceptions pm": 11.5, "Fautes pm": 12.7, "Hors-jeux pm": 1.1},
    {"Équipe": "West Bromwich Albion", "Tirs pm": 15.8, "Tacles pm": 15.3, "Interceptions pm": 11.4, "Fautes pm": 10.6, "Hors-jeux pm": 2},
    {"Équipe": "Wolverhampton", "Tirs pm": 11.3, "Tacles pm": 15.1, "Interceptions pm": 11.3, "Fautes pm": 11.1, "Hors-jeux pm": 1},
    {"Équipe": "Manchest. Utd", "Tirs pm": 11.3, "Tacles pm": 14.5, "Interceptions pm": 10.2, "Fautes pm": 11.9, "Hors-jeux pm": 2},
    {"Équipe": "Aston Villa", "Tirs pm": 14.2, "Tacles pm": 13.9, "Interceptions pm": 10.2, "Fautes pm": 11.6, "Hors-jeux pm": 1.9},
    {"Équipe": "Liverpool", "Tirs pm": 8.7, "Tacles pm": 13.8, "Interceptions pm": 9.1, "Fautes pm": 10.4, "Hors-jeux pm": 1.7},
    {"Équipe": "West Ham", "Tirs pm": 12.2, "Tacles pm": 13.5, "Interceptions pm": 11.5, "Fautes pm": 9.9, "Hors-jeux pm": 1.9},
    {"Équipe": "Newcastle", "Tirs pm": 15.1, "Tacles pm": 13.5, "Interceptions pm": 9.5, "Fautes pm": 10.2, "Hors-jeux pm": 1.5},
    {"Équipe": "Burnley", "Tirs pm": 15, "Tacles pm": 13.3, "Interceptions pm": 10.9, "Fautes pm": 10.1, "Hors-jeux pm": 2.1},
    {"Équipe": "Manchest. City", "Tirs pm": 7.3, "Tacles pm": 12.9, "Interceptions pm": 8.3, "Fautes pm": 9.3, "Hors-jeux pm": 1.5},
    {"Équipe": "Arsenal", "Tirs pm": 10.9, "Tacles pm": 12, "Interceptions pm": 9.2, "Fautes pm": 9.1, "Hors-jeux pm": 1.6}
]




data_23_24 = [
    {"Équipe": "Nottingham F.", "Tirs pm": 13.2, "Tacles pm": 19.8, "Interceptions pm": 8.4, "Fautes pm": 11.7, "Hors-jeux pm": 2.1},
    {"Équipe": "Tottenham", "Tirs pm": 12.8, "Tacles pm": 19.7, "Interceptions pm": 9.5, "Fautes pm": 11, "Hors-jeux pm": 2.3},
    {"Équipe": "Crystal Palace", "Tirs pm": 12.4, "Tacles pm": 19.6, "Interceptions pm": 8.4, "Fautes pm": 12.1, "Hors-jeux pm": 1.6},
    {"Équipe": "Everton", "Tirs pm": 13.9, "Tacles pm": 19.6, "Interceptions pm": 10, "Fautes pm": 12.1, "Hors-jeux pm": 2.1},
    {"Équipe": "Sheffield Utd", "Tirs pm": 18.1, "Tacles pm": 19.5, "Interceptions pm": 9.2, "Fautes pm": 11.4, "Hors-jeux pm": 1.8},
    {"Équipe": "Wolverhampton", "Tirs pm": 14.2, "Tacles pm": 18.8, "Interceptions pm": 8, "Fautes pm": 12.4, "Hors-jeux pm": 2},
    {"Équipe": "Bournemouth", "Tirs pm": 14.1, "Tacles pm": 18.6, "Interceptions pm": 8.9, "Fautes pm": 13.3, "Hors-jeux pm": 1.7},
    {"Équipe": "Brentford", "Tirs pm": 14.7, "Tacles pm": 18.5, "Interceptions pm": 10, "Fautes pm": 10.2, "Hors-jeux pm": 1.8},
    {"Équipe": "Chelsea", "Tirs pm": 14.1, "Tacles pm": 18.2, "Interceptions pm": 7.9, "Fautes pm": 12.4, "Hors-jeux pm": 2.6},
    {"Équipe": "West Ham", "Tirs pm": 16.8, "Tacles pm": 17.9, "Interceptions pm": 9.7, "Fautes pm": 10.5, "Hors-jeux pm": 2},
    {"Équipe": "Liverpool", "Tirs pm": 11, "Tacles pm": 17.5, "Interceptions pm": 7.9, "Fautes pm": 12.4, "Hors-jeux pm": 2.5},
    {"Équipe": "Newcastle", "Tirs pm": 14, "Tacles pm": 17.4, "Interceptions pm": 7.1, "Fautes pm": 10, "Hors-jeux pm": 1.5},
    {"Équipe": "Fulham", "Tirs pm": 13.8, "Tacles pm": 17.3, "Interceptions pm": 10.7, "Fautes pm": 10.1, "Hors-jeux pm": 1.6},
    {"Équipe": "Manchest. Utd", "Tirs pm": 17.2, "Tacles pm": 17.3, "Interceptions pm": 8, "Fautes pm": 10.7, "Hors-jeux pm": 2.7},
    {"Équipe": "Luton Town", "Tirs pm": 17.3, "Tacles pm": 16.9, "Interceptions pm": 8.8, "Fautes pm": 11.7, "Hors-jeux pm": 1.9},
    {"Équipe": "Brighton", "Tirs pm": 12.3, "Tacles pm": 16.4, "Interceptions pm": 7.7, "Fautes pm": 10.6, "Hors-jeux pm": 2.3},
    {"Équipe": "Burnley", "Tirs pm": 15.8, "Tacles pm": 15.9, "Interceptions pm": 7.2, "Fautes pm": 11.2, "Hors-jeux pm": 2.2},
    {"Équipe": "Arsenal", "Tirs pm": 8.4, "Tacles pm": 15.8, "Interceptions pm": 7.2, "Fautes pm": 9.9, "Hors-jeux pm": 2.2},
    {"Équipe": "Aston Villa", "Tirs pm": 10.8, "Tacles pm": 14.6, "Interceptions pm": 6.4, "Fautes pm": 10.9, "Hors-jeux pm": 1.2},
    {"Équipe": "Manchest. City", "Tirs pm": 8.2, "Tacles pm": 12.8, "Interceptions pm": 6.2, "Fautes pm": 8.3, "Hors-jeux pm": 1.1}
]


# Manchest. City, Nottingham F., Sheffield Utd, Luton Town, Manchest. Utd, Wolverhampton
#FC Barcelone, Gérone FC, Athletic Bilbao, Atl. Madrid, Betis Séville, FC Valence,
# UD UD Las Palmas, Alaves, Real Majorque, FC Seville, Cadix, Grenade

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