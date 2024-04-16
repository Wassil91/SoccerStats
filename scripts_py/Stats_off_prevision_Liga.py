import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]



# Supprimer la collection si elle existe déjà
if "Stats_off_prevision_Liga" in db.list_collection_names():
    db["Stats_off_prevision_Liga"].drop()

# Recréer la collection
collection = db["Stats_off_prevision_Liga"]

# Données pour les trois collections
data_22_23 = [
    {"Équipe": "Real Madrid", "Tirs pm": 17, "Tirs CA pm": 5.8, "Dribbles pm": 12.5, "Fautes subies pm": 13.9},
    {"Équipe": "FC Barcelone", "Tirs pm": 15.1, "Tirs CA pm": 5.1, "Dribbles pm": 10.4, "Fautes subies pm": 11.9},
    {"Équipe": "Athletic Bilbao", "Tirs pm": 14.4, "Tirs CA pm": 4.5, "Dribbles pm": 9.2, "Fautes subies pm": 11.1},
    {"Équipe": "Atl. Madrid", "Tirs pm": 14.2, "Tirs CA pm": 5.3, "Dribbles pm": 8.5, "Fautes subies pm": 11},
    {"Équipe": "Rayo Vallecano", "Tirs pm": 13.6, "Tirs CA pm": 4.1, "Dribbles pm": 7.8, "Fautes subies pm": 12.9},
    {"Équipe": "Villarreal", "Tirs pm": 13.3, "Tirs CA pm": 5.2, "Dribbles pm": 11.4, "Fautes subies pm": 12.7},
    {"Équipe": "FC Valence", "Tirs pm": 12.8, "Tirs CA pm": 3.9, "Dribbles pm": 9.3, "Fautes subies pm": 13.4},
    {"Équipe": "Celta Vigo", "Tirs pm": 12.4, "Tirs CA pm": 4.3, "Dribbles pm": 8.5, "Fautes subies pm": 13},
    {"Équipe": "Real Sociedad", "Tirs pm": 12.3, "Tirs CA pm": 4.7, "Dribbles pm": 8.1, "Fautes subies pm": 11.9},
    {"Équipe": "FC Seville", "Tirs pm": 12.3, "Tirs CA pm": 3.9, "Dribbles pm": 7.1, "Fautes subies pm": 12.1},
    {"Équipe": "Gérone FC", "Tirs pm": 12, "Tirs CA pm": 4.5, "Dribbles pm": 9, "Fautes subies pm": 13.3},
    {"Équipe": "Real Valladolid", "Tirs pm": 11.8, "Tirs CA pm": 3.8, "Dribbles pm": 8.1, "Fautes subies pm": 13.6},
    {"Équipe": "Osasuna", "Tirs pm": 11.7, "Tirs CA pm": 3.6, "Dribbles pm": 8.3, "Fautes subies pm": 10.8},
    {"Équipe": "Almeria", "Tirs pm": 11.6, "Tirs CA pm": 4.2, "Dribbles pm": 7.8, "Fautes subies pm": 10.6},
    {"Équipe": "Espanyol", "Tirs pm": 11.1, "Tirs CA pm": 4.1, "Dribbles pm": 6.8, "Fautes subies pm": 13},
    {"Équipe": "Betis Séville", "Tirs pm": 11.1, "Tirs CA pm": 4, "Dribbles pm": 9.6, "Fautes subies pm": 14.1},
    {"Équipe": "Elche", "Tirs pm": 10.7, "Tirs CA pm": 3.4, "Dribbles pm": 9, "Fautes subies pm": 10.4},
    {"Équipe": "Cadix", "Tirs pm": 10.4, "Tirs CA pm": 3.1, "Dribbles pm": 6.5, "Fautes subies pm": 12.7},
    {"Équipe": "Getafe", "Tirs pm": 9.8, "Tirs CA pm": 3.3, "Dribbles pm": 5.7, "Fautes subies pm": 11.5},
    {"Équipe": "Real Majorque", "Tirs pm": 8.7, "Tirs CA pm": 3, "Dribbles pm": 7.7, "Fautes subies pm": 12}
]



data_21_22 = [
    {"Équipe": "Real Madrid", "Tirs pm": 17.3, "Tirs CA pm": 6.8, "Dribbles pm": 12, "Fautes subies pm": 12.5},
    {"Équipe": "FC Barcelone", "Tirs pm": 13.6, "Tirs CA pm": 4.9, "Dribbles pm": 12.5, "Fautes subies pm": 12.9},
    {"Équipe": "Betis Séville", "Tirs pm": 13.6, "Tirs CA pm": 5, "Dribbles pm": 9.8, "Fautes subies pm": 12.4},
    {"Équipe": "Rayo Vallecano", "Tirs pm": 13.3, "Tirs CA pm": 3.7, "Dribbles pm": 9.4, "Fautes subies pm": 13.4},
    {"Équipe": "Athletic Bilbao", "Tirs pm": 12.3, "Tirs CA pm": 4.3, "Dribbles pm": 7.8, "Fautes subies pm": 12.7},
    {"Équipe": "Atl. Madrid", "Tirs pm": 12.2, "Tirs CA pm": 4.2, "Dribbles pm": 10.1, "Fautes subies pm": 13.2},
    {"Équipe": "Villarreal", "Tirs pm": 12, "Tirs CA pm": 4.6, "Dribbles pm": 11.9, "Fautes subies pm": 12.1},
    {"Équipe": "Real Majorque", "Tirs pm": 11.8, "Tirs CA pm": 3.6, "Dribbles pm": 9.7, "Fautes subies pm": 14.1},
    {"Équipe": "Levante", "Tirs pm": 11.7, "Tirs CA pm": 4.2, "Dribbles pm": 9.2, "Fautes subies pm": 10.6},
    {"Équipe": "Real Sociedad", "Tirs pm": 11.6, "Tirs CA pm": 4.1, "Dribbles pm": 9.5, "Fautes subies pm": 12.2},
    {"Équipe": "FC Seville", "Tirs pm": 11.4, "Tirs CA pm": 3.8, "Dribbles pm": 8, "Fautes subies pm": 12.2},
    {"Équipe": "Osasuna", "Tirs pm": 11.4, "Tirs CA pm": 3.3, "Dribbles pm": 6.9, "Fautes subies pm": 10.3},
    {"Équipe": "Grenade", "Tirs pm": 11.2, "Tirs CA pm": 3.6, "Dribbles pm": 7.1, "Fautes subies pm": 12.3},
    {"Équipe": "Cadix", "Tirs pm": 10.8, "Tirs CA pm": 3.4, "Dribbles pm": 7.4, "Fautes subies pm": 11.8},
    {"Équipe": "Celta Vigo", "Tirs pm": 10.8, "Tirs CA pm": 3.8, "Dribbles pm": 9.1, "Fautes subies pm": 12.2},
    {"Équipe": "Getafe", "Tirs pm": 10.8, "Tirs CA pm": 3.1, "Dribbles pm": 6.4, "Fautes subies pm": 12.7},
    {"Équipe": "Espanyol", "Tirs pm": 10.7, "Tirs CA pm": 3.5, "Dribbles pm": 9.8, "Fautes subies pm": 13.6},
    {"Équipe": "FC Valence", "Tirs pm": 10.6, "Tirs CA pm": 3.8, "Dribbles pm": 9.5, "Fautes subies pm": 15.4},
    {"Équipe": "Alaves", "Tirs pm": 10, "Tirs CA pm": 3.1, "Dribbles pm": 7.1, "Fautes subies pm": 12},
    {"Équipe": "Elche", "Tirs pm": 8.8, "Tirs CA pm": 2.8, "Dribbles pm": 8.9, "Fautes subies pm": 12.4}
]

data_20_21 = [
    {"Équipe": "FC Barcelone", "Tirs pm": 15.3, "Tirs CA pm": 6.4, "Dribbles pm": 13.5, "Fautes subies pm": 13.8},
    {"Équipe": "Real Madrid", "Tirs pm": 14.4, "Tirs CA pm": 4.6, "Dribbles pm": 11.3, "Fautes subies pm": 12.5},
    {"Équipe": "FC Seville", "Tirs pm": 12.1, "Tirs CA pm": 3.8, "Dribbles pm": 10.3, "Fautes subies pm": 12.8},
    {"Équipe": "Atl. Madrid", "Tirs pm": 12.1, "Tirs CA pm": 4.9, "Dribbles pm": 9.8, "Fautes subies pm": 11.7},
    {"Équipe": "Eibar", "Tirs pm": 11.9, "Tirs CA pm": 3.6, "Dribbles pm": 8.4, "Fautes subies pm": 10.2},
    {"Équipe": "Betis Séville", "Tirs pm": 11.7, "Tirs CA pm": 4.2, "Dribbles pm": 11, "Fautes subies pm": 12.7},
    {"Équipe": "Real Sociedad", "Tirs pm": 11.3, "Tirs CA pm": 4.1, "Dribbles pm": 8.7, "Fautes subies pm": 12.1},
    {"Équipe": "SD Huesca", "Tirs pm": 10.7, "Tirs CA pm": 3.6, "Dribbles pm": 11.2, "Fautes subies pm": 13.6},
    {"Équipe": "Villarreal", "Tirs pm": 10.7, "Tirs CA pm": 4.3, "Dribbles pm": 11.5, "Fautes subies pm": 12.2},
    {"Équipe": "Athletic Bilbao", "Tirs pm": 10.6, "Tirs CA pm": 3.6, "Dribbles pm": 8, "Fautes subies pm": 12.6},
    {"Équipe": "FC Valence", "Tirs pm": 10.3, "Tirs CA pm": 3.7, "Dribbles pm": 9, "Fautes subies pm": 13.8},
    {"Équipe": "Levante", "Tirs pm": 10.1, "Tirs CA pm": 3.4, "Dribbles pm": 9.6, "Fautes subies pm": 10.4},
    {"Équipe": "Osasuna", "Tirs pm": 9.8, "Tirs CA pm": 3.2, "Dribbles pm": 6.4, "Fautes subies pm": 10.7},
    {"Équipe": "Real Valladolid", "Tirs pm": 9.7, "Tirs CA pm": 3.1, "Dribbles pm": 7.7, "Fautes subies pm": 13.4},
    {"Équipe": "Getafe", "Tirs pm": 9.5, "Tirs CA pm": 2.8, "Dribbles pm": 6.9, "Fautes subies pm": 13.7},
    {"Équipe": "Grenade", "Tirs pm": 9.4, "Tirs CA pm": 3.3, "Dribbles pm": 8.2, "Fautes subies pm": 12.3},
    {"Équipe": "Celta Vigo", "Tirs pm": 9.4, "Tirs CA pm": 3.7, "Dribbles pm": 7.3, "Fautes subies pm": 12.9},
    {"Équipe": "Alaves", "Tirs pm": 9.1, "Tirs CA pm": 2.7, "Dribbles pm": 6.7, "Fautes subies pm": 12.8},
    {"Équipe": "Cadix", "Tirs pm": 8, "Tirs CA pm": 2.7, "Dribbles pm": 8.4, "Fautes subies pm": 12.7},
    {"Équipe": "Elche", "Tirs pm": 7.1, "Tirs CA pm": 2.4, "Dribbles pm": 9.6, "Fautes subies pm": 12}
]


data_23_24 = [
    {"Équipe": "Real Madrid", "Tirs pm": 16.4, "Tirs CA pm": 6.4, "Dribbles pm": 10.6, "Fautes subies pm": 14.1},
    {"Équipe": "FC Barcelone", "Tirs pm": 15.7, "Tirs CA pm": 6.1, "Dribbles pm": 10.4, "Fautes subies pm": 13},
    {"Équipe": "FC Seville", "Tirs pm": 13.5, "Tirs CA pm": 4.5, "Dribbles pm": 8.1, "Fautes subies pm": 12.2},
    {"Équipe": "Betis Séville", "Tirs pm": 13.2, "Tirs CA pm": 3.9, "Dribbles pm": 10.4, "Fautes subies pm": 13.3},
    {"Équipe": "Athletic Bilbao", "Tirs pm": 12.7, "Tirs CA pm": 4.8, "Dribbles pm": 9.1, "Fautes subies pm": 11.6},
    {"Équipe": "Real Sociedad", "Tirs pm": 12.7, "Tirs CA pm": 4.3, "Dribbles pm": 8.8, "Fautes subies pm": 11.9},
    {"Équipe": "Gérone FC", "Tirs pm": 12.7, "Tirs CA pm": 4.9, "Dribbles pm": 9.2, "Fautes subies pm": 12.5},
    {"Équipe": "Celta Vigo", "Tirs pm": 12.6, "Tirs CA pm": 4.1, "Dribbles pm": 7.9, "Fautes subies pm": 13.3},
    {"Équipe": "Atl. Madrid", "Tirs pm": 12.6, "Tirs CA pm": 5.7, "Dribbles pm": 8.7, "Fautes subies pm": 11.3},
    {"Équipe": "Alaves", "Tirs pm": 12.4, "Tirs CA pm": 3.6, "Dribbles pm": 8.6, "Fautes subies pm": 11.6},
    {"Équipe": "Almeria", "Tirs pm": 12, "Tirs CA pm": 4, "Dribbles pm": 7.7, "Fautes subies pm": 12.5},
    {"Équipe": "Rayo Vallecano", "Tirs pm": 12, "Tirs CA pm": 4.3, "Dribbles pm": 7.3, "Fautes subies pm": 13.7},
    {"Équipe": "Villarreal", "Tirs pm": 11.8, "Tirs CA pm": 4.6, "Dribbles pm": 9.2, "Fautes subies pm": 12.4},
    {"Équipe": "Getafe", "Tirs pm": 11.6, "Tirs CA pm": 4.1, "Dribbles pm": 5.8, "Fautes subies pm": 10.9},
    {"Équipe": "Osasuna", "Tirs pm": 11.3, "Tirs CA pm": 3.4, "Dribbles pm": 6, "Fautes subies pm": 10.4},
    {"Équipe": "Real Majorque", "Tirs pm": 11.1, "Tirs CA pm": 3.6, "Dribbles pm": 6.9, "Fautes subies pm": 11.2},
    {"Équipe": "Grenade", "Tirs pm": 10.8, "Tirs CA pm": 3.8, "Dribbles pm": 8.6, "Fautes subies pm": 14.2},
    {"Équipe": "UD Las Palmas", "Tirs pm": 10.7, "Tirs CA pm": 3.4, "Dribbles pm": 8.2, "Fautes subies pm": 13.6},
    {"Équipe": "Cadix", "Tirs pm": 10, "Tirs CA pm": 3, "Dribbles pm": 5.8, "Fautes subies pm": 13.6},
    {"Équipe": "FC Valence", "Tirs pm": 9.8, "Tirs CA pm": 3.5, "Dribbles pm": 7.4, "Fautes subies pm": 12.2}
]

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