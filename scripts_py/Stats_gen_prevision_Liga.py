import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]



# Supprimer la collection si elle existe déjà
if "Stats_gen_prevision_Liga" in db.list_collection_names():
    db["Stats_gen_prevision_Liga"].drop()

# Recréer la collection
collection = db["Stats_gen_prevision_Liga"]

# Données pour les trois collections
data_22_23 = [
    {"Équipe": "Real Madrid", "Buts": 75, "Tirs pm": 17, "Possession%": 61.2, "PassesRéussies%": 90, "AériensGagnés": 8.9},
    {"Équipe": "FC Barcelone", "Buts": 70, "Tirs pm": 15.1, "Possession%": 64.8, "PassesRéussies%": 88.1, "AériensGagnés": 12.3},
    {"Équipe": "Atl. Madrid", "Buts": 70, "Tirs pm": 14.2, "Possession%": 50.4, "PassesRéussies%": 84.2, "AériensGagnés": 11.9},
    {"Équipe": "Villarreal", "Buts": 59, "Tirs pm": 13.3, "Possession%": 56.7, "PassesRéussies%": 85.4, "AériensGagnés": 9.5},
    {"Équipe": "Real Sociedad", "Buts": 51, "Tirs pm": 12.3, "Possession%": 54.6, "PassesRéussies%": 81.9, "AériensGagnés": 17.6},
    {"Équipe": "Betis Séville", "Buts": 46, "Tirs pm": 11.1, "Possession%": 50.5, "PassesRéussies%": 82.4, "AériensGagnés": 12.1},
    {"Équipe": "Athletic Bilbao", "Buts": 47, "Tirs pm": 14.4, "Possession%": 51.5, "PassesRéussies%": 80.1, "AériensGagnés": 14.2},
    {"Équipe": "Real Majorque", "Buts": 37, "Tirs pm": 8.7, "Possession%": 40.4, "PassesRéussies%": 75.5, "AériensGagnés": 17.8},
    {"Équipe": "Celta Vigo", "Buts": 43, "Tirs pm": 12.4, "Possession%": 50.1, "PassesRéussies%": 81.8, "AériensGagnés": 12.1},
    {"Équipe": "Gérone FC", "Buts": 58, "Tirs pm": 12, "Possession%": 50.9, "PassesRéussies%": 83.4, "AériensGagnés": 11.4},
    {"Équipe": "FC Valence", "Buts": 42, "Tirs pm": 12.8, "Possession%": 51.8, "PassesRéussies%": 80.6, "AériensGagnés": 13},
    {"Équipe": "Osasuna", "Buts": 37, "Tirs pm": 11.7, "Possession%": 48.1, "PassesRéussies%": 78, "AériensGagnés": 16.2},
    {"Équipe": "FC Seville", "Buts": 47, "Tirs pm": 12.3, "Possession%": 52.7, "PassesRéussies%": 83, "AériensGagnés": 13.1},
    {"Équipe": "Espanyol", "Buts": 52, "Tirs pm": 11.1, "Possession%": 42.6, "PassesRéussies%": 76.5, "AériensGagnés": 17.1},
    {"Équipe": "Rayo Vallecano", "Buts": 45, "Tirs pm": 13.6, "Possession%": 50.8, "PassesRéussies%": 78.8, "AériensGagnés": 11.5},
    {"Équipe": "Real Valladolid", "Buts": 33, "Tirs pm": 11.8, "Possession%": 48.5, "PassesRéussies%": 79.1, "AériensGagnés": 12.5},
    {"Équipe": "Almeria", "Buts": 49, "Tirs pm": 11.6, "Possession%": 44.9, "PassesRéussies%": 78.3, "AériensGagnés": 12.8},
    {"Équipe": "Getafe", "Buts": 34, "Tirs pm": 9.8, "Possession%": 39.2, "PassesRéussies%": 70.6, "AériensGagnés": 19},
    {"Équipe": "Cadix", "Buts": 30, "Tirs pm": 10.4, "Possession%": 41.3, "PassesRéussies%": 74.1, "AériensGagnés": 16.1},
    {"Équipe": "Elche", "Buts": 30, "Tirs pm": 10.7, "Possession%": 44.9, "PassesRéussies%": 79.1, "AériensGagnés": 12.8}
]



data_21_22 = [
    {"Équipe": "Real Madrid", "Buts": 80, "Tirs pm": 17.3, "Possession%": 60.1, "PassesRéussies%": 89, "AériensGagnés": 10.5},
    {"Équipe": "FC Barcelone", "Buts": 68, "Tirs pm": 13.6, "Possession%": 64.7, "PassesRéussies%": 88.3, "AériensGagnés": 12.2},
    {"Équipe": "Betis Séville", "Buts": 62, "Tirs pm": 13.6, "Possession%": 53.8, "PassesRéussies%": 82.5, "AériensGagnés": 13.4},
    {"Équipe": "Villarreal", "Buts": 63, "Tirs pm": 12, "Possession%": 56.8, "PassesRéussies%": 83.6, "AériensGagnés": 13},
    {"Équipe": "Atl. Madrid", "Buts": 65, "Tirs pm": 12.2, "Possession%": 50.2, "PassesRéussies%": 81.1, "AériensGagnés": 15},
    {"Équipe": "FC Seville", "Buts": 53, "Tirs pm": 11.4, "Possession%": 59.9, "PassesRéussies%": 84.7, "AériensGagnés": 15.7},
    {"Équipe": "Real Sociedad", "Buts": 40, "Tirs pm": 11.6, "Possession%": 54.6, "PassesRéussies%": 81.8, "AériensGagnés": 18.1},
    {"Équipe": "Celta Vigo", "Buts": 43, "Tirs pm": 10.8, "Possession%": 55.4, "PassesRéussies%": 79.8, "AériensGagnés": 16},
    {"Équipe": "Athletic Bilbao", "Buts": 43, "Tirs pm": 12.3, "Possession%": 47.5, "PassesRéussies%": 77.1, "AériensGagnés": 15.7},
    {"Équipe": "Rayo Vallecano", "Buts": 39, "Tirs pm": 13.3, "Possession%": 49.4, "PassesRéussies%": 77.6, "AériensGagnés": 17.6},
    {"Équipe": "Osasuna", "Buts": 37, "Tirs pm": 11.4, "Possession%": 46.1, "PassesRéussies%": 74.4, "AériensGagnés": 21.7},
    {"Équipe": "Getafe", "Buts": 33, "Tirs pm": 10.8, "Possession%": 40.4, "PassesRéussies%": 72.3, "AériensGagnés": 19.3},
    {"Équipe": "FC Valence", "Buts": 48, "Tirs pm": 10.6, "Possession%": 43.3, "PassesRéussies%": 71.9, "AériensGagnés": 15.1},
    {"Équipe": "Cadix", "Buts": 35, "Tirs pm": 10.8, "Possession%": 40.5, "PassesRéussies%": 73.6, "AériensGagnés": 16.1},
    {"Équipe": "Espanyol", "Buts": 40, "Tirs pm": 10.7, "Possession%": 46.7, "PassesRéussies%": 81.1, "AériensGagnés": 12.1},
    {"Équipe": "Grenade", "Buts": 44, "Tirs pm": 11.2, "Possession%": 43.9, "PassesRéussies%": 72.9, "AériensGagnés": 16.2},
    {"Équipe": "Elche", "Buts": 40, "Tirs pm": 8.8, "Possession%": 48, "PassesRéussies%": 79.2, "AériensGagnés": 14.3},
    {"Équipe": "Real Majorque", "Buts": 36, "Tirs pm": 11.8, "Possession%": 44.6, "PassesRéussies%": 76, "AériensGagnés": 17.4},
    {"Équipe": "Alaves", "Buts": 31, "Tirs pm": 10, "Possession%": 41.3, "PassesRéussies%": 70.3, "AériensGagnés": 23.1},
    {"Équipe": "Levante", "Buts": 51, "Tirs pm": 11.7, "Possession%": 46.6, "PassesRéussies%": 78.3, "AériensGagnés": 11.9}
]



data_20_21 = [
    {"Équipe": "FC Barcelone", "Buts": 85, "Tirs pm": 15.3, "Possession%": 36.9, "PassesRéussies%": 89.7, "AériensGagnés": 10.6},
    {"Équipe": "Real Madrid", "Buts": 67, "Tirs pm": 14.4, "Possession%": 57.7, "PassesRéussies%": 87.7, "AériensGagnés": 11.8},
    {"Équipe": "Atl. Madrid", "Buts": 67, "Tirs pm": 12.1, "Possession%": 51.8, "PassesRéussies%": 83.1, "AériensGagnés": 14.4},
    {"Équipe": "FC Seville", "Buts": 53, "Tirs pm": 12.1, "Possession%": 58.7, "PassesRéussies%": 86.2, "AériensGagnés": 16.6},
    {"Équipe": "Villarreal", "Buts": 60, "Tirs pm": 10.7, "Possession%": 54.3, "PassesRéussies%": 84.4, "AériensGagnés": 13},
    {"Équipe": "Real Sociedad", "Buts": 59, "Tirs pm": 11.3, "Possession%": 53.7, "PassesRéussies%": 80.8, "AériensGagnés": 17.9},
    {"Équipe": "Betis Séville", "Buts": 50, "Tirs pm": 11.7, "Possession%": 52.9, "PassesRéussies%": 82, "AériensGagnés": 16.4},
    {"Équipe": "FC Valence", "Buts": 50, "Tirs pm": 10.3, "Possession%": 47.9, "PassesRéussies%": 79.4, "AériensGagnés": 16.3},
    {"Équipe": "Celta Vigo", "Buts": 55, "Tirs pm": 9.4, "Possession%": 52, "PassesRéussies%": 79.9, "AériensGagnés": 16.5},
    {"Équipe": "SD Huesca", "Buts": 34, "Tirs pm": 10.7, "Possession%": 48.7, "PassesRéussies%": 79.8, "AériensGagnés": 15.7},
    {"Équipe": "Athletic Bilbao", "Buts": 46, "Tirs pm": 10.6, "Possession%": 49.4, "PassesRéussies%": 78.5, "AériensGagnés": 17.9},
    {"Équipe": "Osasuna", "Buts": 37, "Tirs pm": 9.8, "Possession%": 44.6, "PassesRéussies%": 70.2, "AériensGagnés": 26.8},
    {"Équipe": "Eibar", "Buts": 29, "Tirs pm": 11.9, "Possession%": 49.2, "PassesRéussies%": 72.6, "AériensGagnés": 24.4},
    {"Équipe": "Alaves", "Buts": 36, "Tirs pm": 9.1, "Possession%": 44.6, "PassesRéussies%": 72.9, "AériensGagnés": 22.6},
    {"Équipe": "Levante", "Buts": 46, "Tirs pm": 10.1, "Possession%": 51.5, "PassesRéussies%": 80.1, "AériensGagnés": 12.2},
    {"Équipe": "Getafe", "Buts": 28, "Tirs pm": 9.5, "Possession%": 8.9, "PassesRéussies%": 66.5, "AériensGagnés": 22.8},
    {"Équipe": "Elche", "Buts": 34, "Tirs pm": 7.1, "Possession%": 48.1, "PassesRéussies%": 81.5, "AériensGagnés": 13.2},
    {"Équipe": "Grenade", "Buts": 47, "Tirs pm": 9.4, "Possession%": 43.4, "PassesRéussies%": 70, "AériensGagnés": 18.3},
    {"Équipe": "Cadix", "Buts": 36, "Tirs pm": 8, "Possession%": 38.5, "PassesRéussies%": 68.8, "AériensGagnés": 18.5},
    {"Équipe": "Real Valladolid", "Buts": 34, "Tirs pm": 9.7, "Possession%": 46.2, "PassesRéussies%": 74.8, "AériensGagnés": 17.1}
]



data_23_24 = [
    {"Équipe": "Real Madrid", "Buts": 64, "Tirs pm": 16.4, "Possession%": 59.6, "PassesRéussies%": 90.1, "AériensGagnés": 8.7},
    {"Équipe": "FC Barcelone", "Buts": 60, "Tirs pm": 15.7, "Possession%": 64.8, "PassesRéussies%": 88.3, "AériensGagnés": 13},
    {"Équipe": "Gérone FC", "Buts": 59, "Tirs pm": 12.7, "Possession%": 57, "PassesRéussies%": 87.4, "AériensGagnés": 10.6},
    {"Équipe": "Athletic Bilbao", "Buts": 50, "Tirs pm": 12.7, "Possession%": 48.9, "PassesRéussies%": 78.8, "AériensGagnés": 15.3},
    {"Équipe": "Atl. Madrid", "Buts": 54, "Tirs pm": 12.6, "Possession%": 51.2, "PassesRéussies%": 84.6, "AériensGagnés": 11.8},
    {"Équipe": "Betis Séville", "Buts": 34, "Tirs pm": 13.2, "Possession%": 50.5, "PassesRéussies%": 82.9, "AériensGagnés": 13},
    {"Équipe": "Real Sociedad", "Buts": 42, "Tirs pm": 12.7, "Possession%": 56.7, "PassesRéussies%": 81.8, "AériensGagnés": 21.7},
    {"Équipe": "Villarreal", "Buts": 47, "Tirs pm": 11.8, "Possession%": 50.1, "PassesRéussies%": 83.7, "AériensGagnés": 14.1},
    {"Équipe": "FC Seville", "Buts": 36, "Tirs pm": 13.5, "Possession%": 51.2, "PassesRéussies%": 81.3, "AériensGagnés": 17.1},
    {"Équipe": "Celta Vigo", "Buts": 32, "Tirs pm": 12.6, "Possession%": 44, "PassesRéussies%": 78.2, "AériensGagnés": 15.3},
    {"Équipe": "UD Las Palmas", "Buts": 29, "Tirs pm": 10.7, "Possession%": 61, "PassesRéussies%": 85.5, "AériensGagnés": 11.3},
    {"Équipe": "FC Valence", "Buts": 32, "Tirs pm": 9.8, "Possession%": 43.8, "PassesRéussies%": 77.2, "AériensGagnés": 12.8},
    {"Équipe": "Real Majorque", "Buts": 25, "Tirs pm": 11.1, "Possession%": 43.4, "PassesRéussies%": 74.4, "AériensGagnés": 21.6},
    {"Équipe": "Getafe", "Buts": 37, "Tirs pm": 11.6, "Possession%": 42.5, "PassesRéussies%": 71, "AériensGagnés": 19},
    {"Équipe": "Alaves", "Buts": 26, "Tirs pm": 12.4, "Possession%": 41.2, "PassesRéussies%": 74.3, "AériensGagnés": 15},
    {"Équipe": "Osasuna", "Buts": 33, "Tirs pm": 11.3, "Possession%": 47.3, "PassesRéussies%": 76.8, "AériensGagnés": 20.6},
    {"Équipe": "Rayo Vallecano", "Buts": 25, "Tirs pm": 12, "Possession%": 48.8, "PassesRéussies%": 77.6, "AériensGagnés": 12.9},
    {"Équipe": "Cadix", "Buts": 21, "Tirs pm": 10, "Possession%": 42.1, "PassesRéussies%": 72.9, "AériensGagnés": 17.4},
    {"Équipe": "Almeria", "Buts": 28, "Tirs pm": 12, "Possession%": 45.5, "PassesRéussies%": 78.2, "AériensGagnés": 15.8},
    {"Équipe": "Grenade", "Buts": 30, "Tirs pm": 10.8, "Possession%": 45.6, "PassesRéussies%": 77.9, "AériensGagnés": 12.5}
]

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