import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]



# Supprimer la collection si elle existe déjà
if "Stats_gen_prevision_serieA" in db.list_collection_names():
    db["Stats_gen_prevision_serieA"].drop()

# Recréer la collection
collection = db["Stats_gen_prevision_serieA"]

# Données pour les trois collections
data_22_23 = [
    {"Équipe": "Naples", "Buts": 77, "Tirs pm": 16.2, "Possession%": 62.1, "PassesRéussies%": 87.8, "AériensGagnés": 12.8},
    {"Équipe": "Lazio Rome", "Buts": 60, "Tirs pm": 11.5, "Possession%": 52.2, "PassesRéussies%": 85.4, "AériensGagnés": 10.7},
    {"Équipe": "Inter Milan", "Buts": 71, "Tirs pm": 16.6, "Possession%": 55.7, "PassesRéussies%": 85.6, "AériensGagnés": 13.3},
    {"Équipe": "Juventus Turin", "Buts": 56, "Tirs pm": 14.1, "Possession%": 49.4, "PassesRéussies%": 83.6, "AériensGagnés": 12.8},
    {"Équipe": "Milan AC", "Buts": 64, "Tirs pm": 14.5, "Possession%": 55.6, "PassesRéussies%": 82.6, "AériensGagnés": 14.8},
    {"Équipe": "Atalanta Berga.", "Buts": 66, "Tirs pm": 13.5, "Possession%": 49.9, "PassesRéussies%": 80.6, "AériensGagnés": 15.2},
    {"Équipe": "AS Rome", "Buts": 50, "Tirs pm": 13.1, "Possession%": 48.1, "PassesRéussies%": 81.3, "AériensGagnés": 14.4},
    {"Équipe": "Bologne", "Buts": 53, "Tirs pm": 12.8, "Possession%": 55, "PassesRéussies%": 83, "AériensGagnés": 9.8},
    {"Équipe": "Fiorentina", "Buts": 53, "Tirs pm": 15.9, "Possession%": 56.4, "PassesRéussies%": 82.3, "AériensGagnés": 15.5},
    {"Équipe": "Udinese", "Buts": 47, "Tirs pm": 13.3, "Possession%": 47.5, "PassesRéussies%": 81.8, "AériensGagnés": 12.7},
    {"Équipe": "Torino", "Buts": 42, "Tirs pm": 11.7, "Possession%": 53.3, "PassesRéussies%": 81.1, "AériensGagnés": 15.6},
    {"Équipe": "Monza", "Buts": 48, "Tirs pm": 10.8, "Possession%": 55.6, "PassesRéussies%": 84.6, "AériensGagnés": 11.8},
    {"Équipe": "Empoli", "Buts": 37, "Tirs pm": 11.8, "Possession%": 47.1, "PassesRéussies%": 80.3, "AériensGagnés": 11.5},
    {"Équipe": "Salernitana", "Buts": 48, "Tirs pm": 10.1, "Possession%": 44.6, "PassesRéussies%": 78.7, "AériensGagnés": 14},
    {"Équipe": "Lecce", "Buts": 33, "Tirs pm": 11, "Possession%": 40.8, "PassesRéussies%": 72.4, "AériensGagnés": 14.9},
    {"Équipe": "Sassuolo", "Buts": 47, "Tirs pm": 12.8, "Possession%": 49.1, "PassesRéussies%": 82.5, "AériensGagnés": 9.9},
    {"Équipe": "Hellas Vérone", "Buts": 31, "Tirs pm": 10.9, "Possession%": 40.5, "PassesRéussies%": 70.7, "AériensGagnés": 21.4},
    {"Équipe": "Cremonese", "Buts": 36, "Tirs pm": 12.2, "Possession%": 41.4, "PassesRéussies%": 75, "AériensGagnés": 14.8},
    {"Équipe": "Spezia", "Buts": 31, "Tirs pm": 11.1, "Possession%": 46.9, "PassesRéussies%": 77.1, "AériensGagnés": 14.1},
    {"Équipe": "Sampdoria", "Buts": 24, "Tirs pm": 9.9, "Possession%": 45.6, "PassesRéussies%": 78.2, "AériensGagnés": 16.4}
]




data_21_22 = [
    {"Équipe": "Inter Milan", "Buts": 84, "Tirs pm": 17.8, "Possession%": 56.8, "PassesRéussies%": 86.3, "AériensGagnés": 14.6},
    {"Équipe": "Milan AC", "Buts": 69, "Tirs pm": 15.8, "Possession%": 54.2, "PassesRéussies%": 83.3, "AériensGagnés": 13.7},
    {"Équipe": "Naples", "Buts": 74, "Tirs pm": 15.2, "Possession%": 58.8, "PassesRéussies%": 86.9, "AériensGagnés": 10.7},
    {"Équipe": "Juventus Turin", "Buts": 57, "Tirs pm": 13.8, "Possession%": 51.7, "PassesRéussies%": 84.7, "AériensGagnés": 14.7},
    {"Équipe": "Atalanta Berga.", "Buts": 65, "Tirs pm": 15.9, "Possession%": 55, "PassesRéussies%": 81.9, "AériensGagnés": 18.2},
    {"Équipe": "AS Rome", "Buts": 59, "Tirs pm": 15.8, "Possession%": 51.2, "PassesRéussies%": 82.9, "AériensGagnés": 13.9},
    {"Équipe": "Lazio Rome", "Buts": 77, "Tirs pm": 11.9, "Possession%": 55.4, "PassesRéussies%": 87, "AériensGagnés": 11},
    {"Équipe": "Torino", "Buts": 46, "Tirs pm": 12.5, "Possession%": 53.7, "PassesRéussies%": 79, "AériensGagnés": 20.6},
    {"Équipe": "Hellas Vérone", "Buts": 65, "Tirs pm": 12.2, "Possession%": 50.5, "PassesRéussies%": 76.7, "AériensGagnés": 18.2},
    {"Équipe": "Udinese", "Buts": 61, "Tirs pm": 13.4, "Possession%": 42.1, "PassesRéussies%": 78.7, "AériensGagnés": 12.1},
    {"Équipe": "Sassuolo", "Buts": 64, "Tirs pm": 15.2, "Possession%": 55.2, "PassesRéussies%": 85.3, "AériensGagnés": 9.2},
    {"Équipe": "Bologne", "Buts": 44, "Tirs pm": 11.6, "Possession%": 50.3, "PassesRéussies%": 81, "AériensGagnés": 13.3},
    {"Équipe": "Fiorentina", "Buts": 59, "Tirs pm": 13.5, "Possession%": 58, "PassesRéussies%": 85.4, "AériensGagnés": 12.5},
    {"Équipe": "Sampdoria", "Buts": 46, "Tirs pm": 10.3, "Possession%": 45.7, "PassesRéussies%": 77.8, "AériensGagnés": 16.3},
    {"Équipe": "Empoli", "Buts": 50, "Tirs pm": 13.1, "Possession%": 46.9, "PassesRéussies%": 78.9, "AériensGagnés": 11.4},
    {"Équipe": "Spezia", "Buts": 41, "Tirs pm": 10.2, "Possession%": 42.4, "PassesRéussies%": 76.7, "AériensGagnés": 14.2},
    {"Équipe": "Cagliari", "Buts": 34, "Tirs pm": 11.2, "Possession%": 43.9, "PassesRéussies%": 75.2, "AériensGagnés": 18.6},
    {"Équipe": "Genoa", "Buts": 27, "Tirs pm": 10.6, "Possession%": 43.3, "PassesRéussies%": 73.7, "AériensGagnés": 16.8},
    {"Équipe": "Venezia", "Buts": 34, "Tirs pm": 9.3, "Possession%": 41.8, "PassesRéussies%": 77.5, "AériensGagnés": 15},
    {"Équipe": "Salernitana", "Buts": 33, "Tirs pm": 11.3, "Possession%": 40.2, "PassesRéussies%": 76.5, "AériensGagnés": 17.5}
]




data_20_21 = [
    {"Équipe": "Juventus Turin", "Buts": 77, "Tirs pm": 15.7, "Possession%": 26.5, "PassesRéussies%": 88.3, "AériensGagnés": 11.4},
    {"Équipe": "Atalanta Berga.", "Buts": 90, "Tirs pm": 16.3, "Possession%": 29.6, "PassesRéussies%": 83.5, "AériensGagnés": 16.8},
    {"Équipe": "Milan AC", "Buts": 74, "Tirs pm": 14.7, "Possession%": 51.4, "PassesRéussies%": 84, "AériensGagnés": 15.2},
    {"Équipe": "Inter Milan", "Buts": 89, "Tirs pm": 14.5, "Possession%": 52, "PassesRéussies%": 87, "AériensGagnés": 11.8},
    {"Équipe": "Naples", "Buts": 86, "Tirs pm": 17, "Possession%": 54.1, "PassesRéussies%": 87, "AériensGagnés": 11.1},
    {"Équipe": "AS Rome", "Buts": 68, "Tirs pm": 14.3, "Possession%": 51.5, "PassesRéussies%": 84.5, "AériensGagnés": 12.1},
    {"Équipe": "Sassuolo", "Buts": 64, "Tirs pm": 13.9, "Possession%": 31.3, "PassesRéussies%": 87.8, "AériensGagnés": 10.9},
    {"Équipe": "Sampdoria", "Buts": 52, "Tirs pm": 11.3, "Possession%": 20.6, "PassesRéussies%": 78.5, "AériensGagnés": 16.8},
    {"Équipe": "Lazio Rome", "Buts": 61, "Tirs pm": 13.8, "Possession%": 52.2, "PassesRéussies%": 83.8, "AériensGagnés": 14.6},
    {"Équipe": "Torino", "Buts": 50, "Tirs pm": 12.2, "Possession%": 23.5, "PassesRéussies%": 80.5, "AériensGagnés": 16.1},
    {"Équipe": "Hellas Vérone", "Buts": 43, "Tirs pm": 10.6, "Possession%": 49.6, "PassesRéussies%": 76.3, "AériensGagnés": 20.6},
    {"Équipe": "Cagliari", "Buts": 43, "Tirs pm": 11.4, "Possession%": 45.8, "PassesRéussies%": 79.8, "AériensGagnés": 17.2},
    {"Équipe": "Bologne", "Buts": 51, "Tirs pm": 13.1, "Possession%": 50.7, "PassesRéussies%": 81.5, "AériensGagnés": 15.1},
    {"Équipe": "Udinese", "Buts": 42, "Tirs pm": 10.9, "Possession%": 47.2, "PassesRéussies%": 82.5, "AériensGagnés": 13.1},
    {"Équipe": "Genoa", "Buts": 47, "Tirs pm": 9, "Possession%": 18, "PassesRéussies%": 79.7, "AériensGagnés": 13.7},
    {"Équipe": "Fiorentina", "Buts": 47, "Tirs pm": 9.8, "Possession%": 46.8, "PassesRéussies%": 81, "AériensGagnés": 14.2},
    {"Équipe": "Spezia", "Buts": 52, "Tirs pm": 10.2, "Possession%": 30.5, "PassesRéussies%": 81.1, "AériensGagnés": 14.4},
    {"Équipe": "Parma Calcio 1913", "Buts": 39, "Tirs pm": 10.4, "Possession%": 24.9, "PassesRéussies%": 82.5, "AériensGagnés": 16.9},
    {"Équipe": "Crotone", "Buts": 45, "Tirs pm": 9.5, "Possession%": 15.6, "PassesRéussies%": 80.4, "AériensGagnés": 12.7},
    {"Équipe": "Benevento", "Buts": 40, "Tirs pm": 11, "Possession%": 35.5, "PassesRéussies%": 77.7, "AériensGagnés": 13.4}
]



data_23_24 = [
    {"Équipe": "Inter Milan", "Buts": 71, "Tirs pm": 15.4, "Possession%": 55.5, "PassesRéussies%": 87.1, "AériensGagnés": 14.5},
    {"Équipe": "Atalanta Berga.", "Buts": 54, "Tirs pm": 13.9, "Possession%": 49.9, "PassesRéussies%": 82.1, "AériensGagnés": 16.3},
    {"Équipe": "Milan AC", "Buts": 55, "Tirs pm": 13.9, "Possession%": 56, "PassesRéussies%": 86.8, "AériensGagnés": 10.9},
    {"Équipe": "Juventus Turin", "Buts": 44, "Tirs pm": 14.3, "Possession%": 47.9, "PassesRéussies%": 83.7, "AériensGagnés": 13.5},
    {"Équipe": "Bologne", "Buts": 42, "Tirs pm": 12.8, "Possession%": 57.8, "PassesRéussies%": 86.1, "AériensGagnés": 11.7},
    {"Équipe": "AS Rome", "Buts": 55, "Tirs pm": 11.9, "Possession%": 54.3, "PassesRéussies%": 85.1, "AériensGagnés": 13.3},
    {"Équipe": "Naples", "Buts": 44, "Tirs pm": 16.9, "Possession%": 61.4, "PassesRéussies%": 87.2, "AériensGagnés": 13},
    {"Équipe": "Fiorentina", "Buts": 41, "Tirs pm": 13.5, "Possession%": 56.5, "PassesRéussies%": 83.1, "AériensGagnés": 16.2},
    {"Équipe": "Torino", "Buts": 29, "Tirs pm": 11.5, "Possession%": 51.9, "PassesRéussies%": 81.8, "AériensGagnés": 16.1},
    {"Équipe": "Lazio Rome", "Buts": 37, "Tirs pm": 11.5, "Possession%": 52.2, "PassesRéussies%": 84.8, "AériensGagnés": 10.4},
    {"Équipe": "Monza", "Buts": 32, "Tirs pm": 12.3, "Possession%": 53.7, "PassesRéussies%": 85.7, "AériensGagnés": 11.2},
    {"Équipe": "Genoa", "Buts": 32, "Tirs pm": 10.8, "Possession%": 43.8, "PassesRéussies%": 77.9, "AériensGagnés": 16.4},
    {"Équipe": "Udinese", "Buts": 28, "Tirs pm": 12.6, "Possession%": 39.9, "PassesRéussies%": 78.1, "AériensGagnés": 14.9},
    {"Équipe": "Hellas Vérone", "Buts": 26, "Tirs pm": 11.3, "Possession%": 44.3, "PassesRéussies%": 75.4, "AériensGagnés": 19.8},
    {"Équipe": "Lecce", "Buts": 26, "Tirs pm": 13.2, "Possession%": 43.5, "PassesRéussies%": 78.1, "AériensGagnés": 14},
    {"Équipe": "Empoli", "Buts": 22, "Tirs pm": 11.6, "Possession%": 44.4, "PassesRéussies%": 79, "AériensGagnés": 12.9},
    {"Équipe": "Frosinone", "Buts": 38, "Tirs pm": 11.7, "Possession%": 51.4, "PassesRéussies%": 80.6, "AériensGagnés": 14.1},
    {"Équipe": "Cagliari", "Buts": 29, "Tirs pm": 11.9, "Possession%": 42.3, "PassesRéussies%": 76.6, "AériensGagnés": 15},
    {"Équipe": "Sassuolo", "Buts": 33, "Tirs pm": 12.8, "Possession%": 43.9, "PassesRéussies%": 79.4, "AériensGagnés": 11.2},
    {"Équipe": "Salernitana", "Buts": 23, "Tirs pm": 10.8, "Possession%": 45.6, "PassesRéussies%": 79.3, "AériensGagnés": 13.9}
]


#Inter Milan Milan, Milan AC, Juventus Turin, Bologne, AS Rome, Atalanta Berga., Naples, Lazio Rome, Hellas Vérone

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