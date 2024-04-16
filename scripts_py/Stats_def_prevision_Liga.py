import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]



# Supprimer la collection si elle existe déjà
if "Stats_def_prevision_Liga" in db.list_collection_names():
    db["Stats_def_prevision_Liga"].drop()

# Recréer la collection
collection = db["Stats_def_prevision_Liga"]

# Données pour les trois collections
data_22_23 = [
    {"Équipe": "FC Valence", "Tirs pm": 10.5, "Tacles pm": 17.8, "Interceptions pm": 7.6, "Fautes pm": 13.3, "Hors-jeux pm": 2.1},
    {"Équipe": "Real Valladolid", "Tirs pm": 15.5, "Tacles pm": 17.7, "Interceptions pm": 9.1, "Fautes pm": 12.1, "Hors-jeux pm": 2.6},
    {"Équipe": "Atl. Madrid", "Tirs pm": 11.5, "Tacles pm": 17.6, "Interceptions pm": 8, "Fautes pm": 11.6, "Hors-jeux pm": 2.1},
    {"Équipe": "Betis Séville", "Tirs pm": 12.3, "Tacles pm": 17.5, "Interceptions pm": 9.6, "Fautes pm": 10.9, "Hors-jeux pm": 1.9},
    {"Équipe": "Celta Vigo", "Tirs pm": 10.2, "Tacles pm": 17.5, "Interceptions pm": 9.7, "Fautes pm": 12.3, "Hors-jeux pm": 2.1},
    {"Équipe": "Real Sociedad", "Tirs pm": 10.4, "Tacles pm": 17.4, "Interceptions pm": 7.1, "Fautes pm": 16.3, "Hors-jeux pm": 2.4},
    {"Équipe": "Espanyol", "Tirs pm": 14.3, "Tacles pm": 17.2, "Interceptions pm": 8.9, "Fautes pm": 12, "Hors-jeux pm": 1.6},
    {"Équipe": "Rayo Vallecano", "Tirs pm": 11.5, "Tacles pm": 16.7, "Interceptions pm": 8.1, "Fautes pm": 14.3, "Hors-jeux pm": 2.6},
    {"Équipe": "Elche", "Tirs pm": 14.8, "Tacles pm": 16.7, "Interceptions pm": 8.5, "Fautes pm": 13.8, "Hors-jeux pm": 2.1},
    {"Équipe": "Cadix", "Tirs pm": 14.5, "Tacles pm": 16.4, "Interceptions pm": 7.1, "Fautes pm": 14, "Hors-jeux pm": 2},
    {"Équipe": "Athletic Bilbao", "Tirs pm": 10.3, "Tacles pm": 16, "Interceptions pm": 8.2, "Fautes pm": 13.5, "Hors-jeux pm": 2.5},
    {"Équipe": "FC Seville", "Tirs pm": 13.4, "Tacles pm": 15.4, "Interceptions pm": 8.9, "Fautes pm": 12.5, "Hors-jeux pm": 2.8},
    {"Équipe": "Mallorca", "Tirs pm": 13.1, "Tacles pm": 15.3, "Interceptions pm": 8.4, "Fautes pm": 16.1, "Hors-jeux pm": 2},
    {"Équipe": "Villarreal", "Tirs pm": 12.5, "Tacles pm": 15.2, "Interceptions pm": 6.9, "Fautes pm": 11.8, "Hors-jeux pm": 2.3},
    {"Équipe": "Almeria", "Tirs pm": 14.3, "Tacles pm": 14.7, "Interceptions pm": 8.5, "Fautes pm": 11.7, "Hors-jeux pm": 2.2},
    {"Équipe": "Gérone FC", "Tirs pm": 13, "Tacles pm": 14.5, "Interceptions pm": 7, "Fautes pm": 11.6, "Hors-jeux pm": 2.3},
    {"Équipe": "Real Madrid", "Tirs pm": 11.1, "Tacles pm": 14.3, "Interceptions pm": 7.8, "Fautes pm": 9.7, "Hors-jeux pm": 2.3},
    {"Équipe": "Getafe", "Tirs pm": 12.8, "Tacles pm": 14.2, "Interceptions pm": 8.4, "Fautes pm": 15.8, "Hors-jeux pm": 2.1},
    {"Équipe": "Osasuna", "Tirs pm": 11.7, "Tacles pm": 14.1, "Interceptions pm": 8.3, "Fautes pm": 14.3, "Hors-jeux pm": 1.6},
    {"Équipe": "FC Barcelone", "Tirs pm": 8.7, "Tacles pm": 13.7, "Interceptions pm": 6.7, "Fautes pm": 11.4, "Hors-jeux pm": 2.6}
]


data_21_22 = [
    {"Équipe": "Celta Vigo", "Tirs pm": 10.7, "Tacles pm": 18.3, "Interceptions pm": 9.9, "Fautes pm": 13.3, "Hors-jeux pm": 2.2},
    {"Équipe": "Betis Séville", "Tirs pm": 11.4, "Tacles pm": 17.4, "Interceptions pm": 9.5, "Fautes pm": 12.8, "Hors-jeux pm": 1.9},
    {"Équipe": "Cadix", "Tirs pm": 12.5, "Tacles pm": 17, "Interceptions pm": 10.9, "Fautes pm": 12.8, "Hors-jeux pm": 1.8},
    {"Équipe": "Atl. Madrid", "Tirs pm": 9.4, "Tacles pm": 16.7, "Interceptions pm": 9.6, "Fautes pm": 12.4, "Hors-jeux pm": 2.2},
    {"Équipe": "Rayo Vallecano", "Tirs pm": 10.9, "Tacles pm": 16.3, "Interceptions pm": 9.6, "Fautes pm": 14.1, "Hors-jeux pm": 2.2},
    {"Équipe": "Grenade", "Tirs pm": 14.8, "Tacles pm": 16.1, "Interceptions pm": 8.6, "Fautes pm": 12.6, "Hors-jeux pm": 1.6},
    {"Équipe": "Mallorca", "Tirs pm": 12.3, "Tacles pm": 16, "Interceptions pm": 9.1, "Fautes pm": 14.6, "Hors-jeux pm": 1.8},
    {"Équipe": "FC Valence", "Tirs pm": 11.7, "Tacles pm": 15.6, "Interceptions pm": 8.9, "Fautes pm": 16.9, "Hors-jeux pm": 1.4},
    {"Équipe": "Elche", "Tirs pm": 13.7, "Tacles pm": 15.5, "Interceptions pm": 8, "Fautes pm": 13.6, "Hors-jeux pm": 1.9},
    {"Équipe": "FC Barcelone", "Tirs pm": 9.3, "Tacles pm": 15.4, "Interceptions pm": 8.6, "Fautes pm": 13.1, "Hors-jeux pm": 2.6},
    {"Équipe": "Getafe", "Tirs pm": 10.8, "Tacles pm": 15.4, "Interceptions pm": 10.6, "Fautes pm": 14.7, "Hors-jeux pm": 1.4},
    {"Équipe": "Real Sociedad", "Tirs pm": 11, "Tacles pm": 15.4, "Interceptions pm": 9.2, "Fautes pm": 12.9, "Hors-jeux pm": 2},
    {"Équipe": "Osasuna", "Tirs pm": 11.7, "Tacles pm": 15.2, "Interceptions pm": 9.7, "Fautes pm": 13.1, "Hors-jeux pm": 1.7},
    {"Équipe": "Real Madrid", "Tirs pm": 11.4, "Tacles pm": 14.7, "Interceptions pm": 8.8, "Fautes pm": 10.4, "Hors-jeux pm": 2.8},
    {"Équipe": "Levante", "Tirs pm": 13.5, "Tacles pm": 14.7, "Interceptions pm": 9.9, "Fautes pm": 14.5, "Hors-jeux pm": 2},
    {"Équipe": "Espanyol", "Tirs pm": 13.2, "Tacles pm": 14.7, "Interceptions pm": 8.6, "Fautes pm": 11.6, "Hors-jeux pm": 1.8},
    {"Équipe": "Alaves", "Tirs pm": 13.6, "Tacles pm": 14.5, "Interceptions pm": 9.9, "Fautes pm": 13.9, "Hors-jeux pm": 1.9},
    {"Équipe": "Villarreal", "Tirs pm": 10.9, "Tacles pm": 14.2, "Interceptions pm": 8.7, "Fautes pm": 12.4, "Hors-jeux pm": 2.4},
    {"Équipe": "FC Seville", "Tirs pm": 11.2, "Tacles pm": 14.1, "Interceptions pm": 7.1, "Fautes pm": 12.2, "Hors-jeux pm": 2.1},
    {"Équipe": "Athletic Bilbao", "Tirs pm": 11.7, "Tacles pm": 13.4, "Interceptions pm": 8.9, "Fautes pm": 12.8, "Hors-jeux pm": 2.3}
]


data_20_21 = [
    {"Équipe": "Celta Vigo", "Tirs pm": 10.9, "Tacles pm": 17.4, "Interceptions pm": 10.7, "Fautes pm": 16.6, "Hors-jeux pm": 1.7},
    {"Équipe": "Atl. Madrid", "Tirs pm": 9.5, "Tacles pm": 16.4, "Interceptions pm": 10.2, "Fautes pm": 12.9, "Hors-jeux pm": 2.5},
    {"Équipe": "Getafe", "Tirs pm": 8.4, "Tacles pm": 15.2, "Interceptions pm": 10.4, "Fautes pm": 16.6, "Hors-jeux pm": 1.9},
    {"Équipe": "Cadix", "Tirs pm": 11.8, "Tacles pm": 15.1, "Interceptions pm": 11.5, "Fautes pm": 11.5, "Hors-jeux pm": 1.5},
    {"Équipe": "Real Valladolid", "Tirs pm": 12.4, "Tacles pm": 14.7, "Interceptions pm": 11.3, "Fautes pm": 13.4, "Hors-jeux pm": 2},
    {"Équipe": "Betis Séville", "Tirs pm": 9.6, "Tacles pm": 14.6, "Interceptions pm": 11.9, "Fautes pm": 13.7, "Hors-jeux pm": 1.7},
    {"Équipe": "SD Huesca", "Tirs pm": 11.1, "Tacles pm": 14.6, "Interceptions pm": 11.5, "Fautes pm": 13.9, "Hors-jeux pm": 2.7},
    {"Équipe": "Elche", "Tirs pm": 12.5, "Tacles pm": 14.5, "Interceptions pm": 10.2, "Fautes pm": 13.9, "Hors-jeux pm": 1.3},
    {"Équipe": "Real Sociedad", "Tirs pm": 8.9, "Tacles pm": 14.4, "Interceptions pm": 8.3, "Fautes pm": 14.9, "Hors-jeux pm": 2.3},
    {"Équipe": "Villarreal", "Tirs pm": 10.8, "Tacles pm": 14.2, "Interceptions pm": 9.8, "Fautes pm": 11.8, "Hors-jeux pm": 2.3},
    {"Équipe": "Real Madrid", "Tirs pm": 9.4, "Tacles pm": 14, "Interceptions pm": 10.5, "Fautes pm": 10.7, "Hors-jeux pm": 2.3},
    {"Équipe": "Osasuna", "Tirs pm": 11.3, "Tacles pm": 13.8, "Interceptions pm": 10.3, "Fautes pm": 14.1, "Hors-jeux pm": 1.8},
    {"Équipe": "Eibar", "Tirs pm": 9.2, "Tacles pm": 13.5, "Interceptions pm": 11.4, "Fautes pm": 13, "Hors-jeux pm": 1.8},
    {"Équipe": "Levante", "Tirs pm": 12.2, "Tacles pm": 13.4, "Interceptions pm": 11.4, "Fautes pm": 13.4, "Hors-jeux pm": 1.8},
    {"Équipe": "Alaves", "Tirs pm": 10.5, "Tacles pm": 13.1, "Interceptions pm": 11.2, "Fautes pm": 13.5, "Hors-jeux pm": 1.9},
    {"Équipe": "FC Barcelone", "Tirs pm": 8.6, "Tacles pm": 13.1, "Interceptions pm": 9.5, "Fautes pm": 9.4, "Hors-jeux pm": 2.4},
    {"Équipe": "Grenade", "Tirs pm": 12.6, "Tacles pm": 13.1, "Interceptions pm": 10.3, "Fautes pm": 14, "Hors-jeux pm": 2.2},
    {"Équipe": "FC Seville", "Tirs pm": 9.2, "Tacles pm": 12.9, "Interceptions pm": 9.5, "Fautes pm": 12.9, "Hors-jeux pm": 1.8},
    {"Équipe": "FC Valence", "Tirs pm": 14.4, "Tacles pm": 12.2, "Interceptions pm": 10.3, "Fautes pm": 12.1, "Hors-jeux pm": 2.4},
    {"Équipe": "Athletic Bilbao", "Tirs pm": 9.9, "Tacles pm": 12.2, "Interceptions pm": 9.1, "Fautes pm": 12.7, "Hors-jeux pm": 2}
]



data_23_24 = [
    {"Équipe": "Mallorca", "Tirs pm": 13.1, "Tacles pm": 18.4, "Interceptions pm": 9.4, "Fautes pm": 15.5, "Hors-jeux pm": 2.9},
    {"Équipe": "FC Seville", "Tirs pm": 13, "Tacles pm": 18.3, "Interceptions pm": 8.3, "Fautes pm": 12.4, "Hors-jeux pm": 2.3},
    {"Équipe": "FC Valence", "Tirs pm": 9.8, "Tacles pm": 18, "Interceptions pm": 8.4, "Fautes pm": 12.9, "Hors-jeux pm": 1.8},
    {"Équipe": "Betis Séville", "Tirs pm": 12.1, "Tacles pm": 17.6, "Interceptions pm": 9, "Fautes pm": 11.4, "Hors-jeux pm": 1.7},
    {"Équipe": "UD Las Palmas", "Tirs pm": 11.9, "Tacles pm": 17.5, "Interceptions pm": 7.5, "Fautes pm": 10.9, "Hors-jeux pm": 2.2},
    {"Équipe": "Atl. Madrid", "Tirs pm": 12.6, "Tacles pm": 17.3, "Interceptions pm": 7.2, "Fautes pm": 11.1, "Hors-jeux pm": 3.4},
    {"Équipe": "Cadix", "Tirs pm": 13.3, "Tacles pm": 17.3, "Interceptions pm": 7.8, "Fautes pm": 15.4, "Hors-jeux pm": 2.5},
    {"Équipe": "Real Sociedad", "Tirs pm": 11, "Tacles pm": 17, "Interceptions pm": 6.9, "Fautes pm": 16.1, "Hors-jeux pm": 2.7},
    {"Équipe": "Almeria", "Tirs pm": 15.6, "Tacles pm": 16.9, "Interceptions pm": 7.9, "Fautes pm": 13.2, "Hors-jeux pm": 2.1},
    {"Équipe": "Grenade", "Tirs pm": 13.3, "Tacles pm": 16.5, "Interceptions pm": 6.6, "Fautes pm": 13, "Hors-jeux pm": 1.9},
    {"Équipe": "Celta Vigo", "Tirs pm": 12.7, "Tacles pm": 16.3, "Interceptions pm": 8.3, "Fautes pm": 12.3, "Hors-jeux pm": 1.7},
    {"Équipe": "Rayo Vallecano", "Tirs pm": 12.1, "Tacles pm": 16.2, "Interceptions pm": 8.4, "Fautes pm": 15.5, "Hors-jeux pm": 2.5},
    {"Équipe": "Athletic Bilbao", "Tirs pm": 10.2, "Tacles pm": 15.7, "Interceptions pm": 7.3, "Fautes pm": 13.9, "Hors-jeux pm": 2.3},
    {"Équipe": "FC Barcelone", "Tirs pm": 10.7, "Tacles pm": 15.6, "Interceptions pm": 6.8, "Fautes pm": 10.8, "Hors-jeux pm": 2.6},
    {"Équipe": "Getafe", "Tirs pm": 12.4, "Tacles pm": 15.2, "Interceptions pm": 8.7, "Fautes pm": 17.1, "Hors-jeux pm": 2},
    {"Équipe": "Villarreal", "Tirs pm": 15.1, "Tacles pm": 15.2, "Interceptions pm": 8.1, "Fautes pm": 13.1, "Hors-jeux pm": 2.6},
    {"Équipe": "Real Madrid", "Tirs pm": 9.9, "Tacles pm": 14.8, "Interceptions pm": 8.1, "Fautes pm": 9.9, "Hors-jeux pm": 2.2},
    {"Équipe": "Alaves", "Tirs pm": 11.8, "Tacles pm": 14.7, "Interceptions pm": 7, "Fautes pm": 12.6, "Hors-jeux pm": 1.9},
    {"Équipe": "Gérone FC", "Tirs pm": 13.6, "Tacles pm": 14.7, "Interceptions pm": 7, "Fautes pm": 11.2, "Hors-jeux pm": 2.4},
    {"Équipe": "Osasuna", "Tirs pm": 11.5, "Tacles pm": 14, "Interceptions pm": 6.9, "Fautes pm": 14.2, "Hors-jeux pm": 1.7}
]

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