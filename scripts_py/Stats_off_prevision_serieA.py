import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]



# Supprimer la collection si elle existe déjà
if "Stats_off_prevision_serieA" in db.list_collection_names():
    db["Stats_off_prevision_serieA"].drop()

# Recréer la collection
collection = db["Stats_off_prevision_serieA"]

# Données pour les trois collections
data_22_23 = [
    {"Équipe": "Inter Milan", "Tirs pm": 16.6, "Tirs CA pm": 5.4, "Dribbles pm": 5.8, "Fautes subies pm": 10.7},
    {"Équipe": "Naples", "Tirs pm": 16.2, "Tirs CA pm": 5.9, "Dribbles pm": 8.9, "Fautes subies pm": 12.3},
    {"Équipe": "Fiorentina", "Tirs pm": 15.9, "Tirs CA pm": 4.9, "Dribbles pm": 8.5, "Fautes subies pm": 13.4},
    {"Équipe": "Milan AC", "Tirs pm": 14.5, "Tirs CA pm": 4.9, "Dribbles pm": 9.7, "Fautes subies pm": 11.5},
    {"Équipe": "Juventus Turin", "Tirs pm": 14.1, "Tirs CA pm": 4.7, "Dribbles pm": 7.7, "Fautes subies pm": 10.8},
    {"Équipe": "Atalanta Berga.", "Tirs pm": 13.5, "Tirs CA pm": 4.7, "Dribbles pm": 9, "Fautes subies pm": 10.1},
    {"Équipe": "Udinese", "Tirs pm": 13.3, "Tirs CA pm": 4, "Dribbles pm": 8.7, "Fautes subies pm": 12.5},
    {"Équipe": "AS Rome", "Tirs pm": 13.1, "Tirs CA pm": 4.4, "Dribbles pm": 7.3, "Fautes subies pm": 13.3},
    {"Équipe": "Sassuolo", "Tirs pm": 12.8, "Tirs CA pm": 4.5, "Dribbles pm": 7.9, "Fautes subies pm": 11.7},
    {"Équipe": "Bologne", "Tirs pm": 12.8, "Tirs CA pm": 4.2, "Dribbles pm": 7.2, "Fautes subies pm": 11.2},
    {"Équipe": "Cremonese", "Tirs pm": 12.2, "Tirs CA pm": 3.5, "Dribbles pm": 6.3, "Fautes subies pm": 10.7},
    {"Équipe": "Empoli", "Tirs pm": 11.8, "Tirs CA pm": 3.4, "Dribbles pm": 8.2, "Fautes subies pm": 12.1},
    {"Équipe": "Torino", "Tirs pm": 11.7, "Tirs CA pm": 3.9, "Dribbles pm": 8.1, "Fautes subies pm": 10.1},
    {"Équipe": "Lazio Rome", "Tirs pm": 11.5, "Tirs CA pm": 4.5, "Dribbles pm": 7.5, "Fautes subies pm": 12.4},
    {"Équipe": "Spezia", "Tirs pm": 11.1, "Tirs CA pm": 3.3, "Dribbles pm": 7.3, "Fautes subies pm": 10},
    {"Équipe": "Lecce", "Tirs pm": 11, "Tirs CA pm": 3, "Dribbles pm": 6.3, "Fautes subies pm": 11.6},
    {"Équipe": "Hellas Vérone", "Tirs pm": 10.9, "Tirs CA pm": 3.2, "Dribbles pm": 7.2, "Fautes subies pm": 10.1},
    {"Équipe": "Monza", "Tirs pm": 10.8, "Tirs CA pm": 3.8, "Dribbles pm": 7.4, "Fautes subies pm": 13},
    {"Équipe": "Salernitana", "Tirs pm": 10.1, "Tirs CA pm": 3.1, "Dribbles pm": 6.9, "Fautes subies pm": 11.6},
    {"Équipe": "Sampdoria", "Tirs pm": 9.9, "Tirs CA pm": 3.1, "Dribbles pm": 6.8, "Fautes subies pm": 13.5}
]




data_21_22 = [
    {"Équipe": "Inter Milan", "Tirs pm": 17.8, "Tirs CA pm": 6.7, "Dribbles pm": 6.7, "Fautes subies pm": 10.8},
    {"Équipe": "Atalanta Berga.", "Tirs pm": 15.9, "Tirs CA pm": 4.7, "Dribbles pm": 9.8, "Fautes subies pm": 11.9},
    {"Équipe": "AS Rome", "Tirs pm": 15.8, "Tirs CA pm": 5.2, "Dribbles pm": 8.7, "Fautes subies pm": 12.5},
    {"Équipe": "Milan AC", "Tirs pm": 15.8, "Tirs CA pm": 5.2, "Dribbles pm": 12, "Fautes subies pm": 13.5},
    {"Équipe": "Naples", "Tirs pm": 15.2, "Tirs CA pm": 5.3, "Dribbles pm": 9.4, "Fautes subies pm": 12.7},
    {"Équipe": "Sassuolo", "Tirs pm": 15.2, "Tirs CA pm": 5.6, "Dribbles pm": 10.6, "Fautes subies pm": 12.6},
    {"Équipe": "Juventus Turin", "Tirs pm": 13.8, "Tirs CA pm": 4.5, "Dribbles pm": 9.7, "Fautes subies pm": 13.2},
    {"Équipe": "Fiorentina", "Tirs pm": 13.5, "Tirs CA pm": 4.9, "Dribbles pm": 7.9, "Fautes subies pm": 14.9},
    {"Équipe": "Udinese", "Tirs pm": 13.4, "Tirs CA pm": 4.9, "Dribbles pm": 9.6, "Fautes subies pm": 12.1},
    {"Équipe": "Empoli", "Tirs pm": 13.1, "Tirs CA pm": 4.3, "Dribbles pm": 7.7, "Fautes subies pm": 12.2},
    {"Équipe": "Torino", "Tirs pm": 12.5, "Tirs CA pm": 4.1, "Dribbles pm": 7.8, "Fautes subies pm": 12.1},
    {"Équipe": "Hellas Vérone", "Tirs pm": 12.2, "Tirs CA pm": 4.2, "Dribbles pm": 8.2, "Fautes subies pm": 11.3},
    {"Équipe": "Lazio Rome", "Tirs pm": 11.9, "Tirs CA pm": 5.1, "Dribbles pm": 8.8, "Fautes subies pm": 11.8},
    {"Équipe": "Bologne", "Tirs pm": 11.6, "Tirs CA pm": 3.9, "Dribbles pm": 8.6, "Fautes subies pm": 13.1},
    {"Équipe": "Salernitana", "Tirs pm": 11.3, "Tirs CA pm": 3.7, "Dribbles pm": 8.3, "Fautes subies pm": 13.7},
    {"Équipe": "Cagliari", "Tirs pm": 11.2, "Tirs CA pm": 3.3, "Dribbles pm": 6.6, "Fautes subies pm": 12.9},
    {"Équipe": "Genoa", "Tirs pm": 10.6, "Tirs CA pm": 3, "Dribbles pm": 7.9, "Fautes subies pm": 13.1},
    {"Équipe": "Sampdoria", "Tirs pm": 10.3, "Tirs CA pm": 3.3, "Dribbles pm": 6, "Fautes subies pm": 13.5},
    {"Équipe": "Spezia", "Tirs pm": 10.2, "Tirs CA pm": 3.6, "Dribbles pm": 7.7, "Fautes subies pm": 11.6},
    {"Équipe": "Venezia", "Tirs pm": 9.3, "Tirs CA pm": 3.2, "Dribbles pm": 7.9, "Fautes subies pm": 12}
]


data_20_21 = [
    {"Équipe": "Naples", "Tirs pm": 17, "Tirs CA pm": 5.9, "Dribbles pm": 10.2, "Fautes subies pm": 14.4},
    {"Équipe": "Atalanta Berga.", "Tirs pm": 16.3, "Tirs CA pm": 6.1, "Dribbles pm": 10.4, "Fautes subies pm": 12.4},
    {"Équipe": "Juventus Turin", "Tirs pm": 15.7, "Tirs CA pm": 5.7, "Dribbles pm": 11.9, "Fautes subies pm": 12.4},
    {"Équipe": "Milan AC", "Tirs pm": 14.7, "Tirs CA pm": 5.4, "Dribbles pm": 10.9, "Fautes subies pm": 12.5},
    {"Équipe": "Inter Milan", "Tirs pm": 14.5, "Tirs CA pm": 5.4, "Dribbles pm": 6.8, "Fautes subies pm": 11.2},
    {"Équipe": "AS Rome", "Tirs pm": 14.3, "Tirs CA pm": 5.3, "Dribbles pm": 10.8, "Fautes subies pm": 11.2},
    {"Équipe": "Sassuolo", "Tirs pm": 13.9, "Tirs CA pm": 4.7, "Dribbles pm": 11.3, "Fautes subies pm": 12.5},
    {"Équipe": "Lazio Rome", "Tirs pm": 13.8, "Tirs CA pm": 4.9, "Dribbles pm": 7.8, "Fautes subies pm": 11.2},
    {"Équipe": "Bologne", "Tirs pm": 13.1, "Tirs CA pm": 4.7, "Dribbles pm": 9.1, "Fautes subies pm": 12.5},
    {"Équipe": "Torino", "Tirs pm": 12.2, "Tirs CA pm": 4, "Dribbles pm": 10.2, "Fautes subies pm": 15.8},
    {"Équipe": "Cagliari", "Tirs pm": 11.4, "Tirs CA pm": 3.4, "Dribbles pm": 8.8, "Fautes subies pm": 12.3},
    {"Équipe": "Sampdoria", "Tirs pm": 11.3, "Tirs CA pm": 3.7, "Dribbles pm": 7.7, "Fautes subies pm": 12.8},
    {"Équipe": "Benevento", "Tirs pm": 11, "Tirs CA pm": 3.5, "Dribbles pm": 7.2, "Fautes subies pm": 15.6},
    {"Équipe": "Udinese", "Tirs pm": 10.9, "Tirs CA pm": 3.4, "Dribbles pm": 9.9, "Fautes subies pm": 13.7},
    {"Équipe": "Hellas Vérone", "Tirs pm": 10.6, "Tirs CA pm": 3.3, "Dribbles pm": 8, "Fautes subies pm": 13.7},
    {"Équipe": "Parma Calcio 1913", "Tirs pm": 10.4, "Tirs CA pm": 3.4, "Dribbles pm": 9.5, "Fautes subies pm": 12.7},
    {"Équipe": "Spezia", "Tirs pm": 10.2, "Tirs CA pm": 3.6, "Dribbles pm": 8.5, "Fautes subies pm": 13.9},
    {"Équipe": "Fiorentina", "Tirs pm": 9.8, "Tirs CA pm": 3.3, "Dribbles pm": 8.6, "Fautes subies pm": 13.8},
    {"Équipe": "Crotone", "Tirs pm": 9.5, "Tirs CA pm": 3.2, "Dribbles pm": 10.1, "Fautes subies pm": 12.9},
    {"Équipe": "Genoa", "Tirs pm": 9, "Tirs CA pm": 3.3, "Dribbles pm": 7.7, "Fautes subies pm": 12.3}
]


data_23_24 = [
    {"Équipe": "Naples", "Tirs pm": 16.9, "Tirs CA pm": 5.4, "Dribbles pm": 8.4, "Fautes subies pm": 11.8},
    {"Équipe": "Inter Milan", "Tirs pm": 15.4, "Tirs CA pm": 5.5, "Dribbles pm": 5.2, "Fautes subies pm": 10.4},
    {"Équipe": "Juventus Turin", "Tirs pm": 14.3, "Tirs CA pm": 4.1, "Dribbles pm": 7.1, "Fautes subies pm": 9.8},
    {"Équipe": "Milan AC", "Tirs pm": 13.9, "Tirs CA pm": 5.4, "Dribbles pm": 9.1, "Fautes subies pm": 11},
    {"Équipe": "Atalanta Berga.", "Tirs pm": 13.9, "Tirs CA pm": 5.3, "Dribbles pm": 7.4, "Fautes subies pm": 9.5},
    {"Équipe": "Fiorentina", "Tirs pm": 13.6, "Tirs CA pm": 4.6, "Dribbles pm": 7, "Fautes subies pm": 11.8},
    {"Équipe": "Lecce", "Tirs pm": 13.2, "Tirs CA pm": 3.9, "Dribbles pm": 7.8, "Fautes subies pm": 13},
    {"Équipe": "Sassuolo", "Tirs pm": 12.8, "Tirs CA pm": 4.2, "Dribbles pm": 7.3, "Fautes subies pm": 10.6},
    {"Équipe": "Bologne", "Tirs pm": 12.8, "Tirs CA pm": 4.5, "Dribbles pm": 7.9, "Fautes subies pm": 11.7},
    {"Équipe": "Udinese", "Tirs pm": 12.6, "Tirs CA pm": 4, "Dribbles pm": 9.4, "Fautes subies pm": 11.1},
    {"Équipe": "Monza", "Tirs pm": 12.3, "Tirs CA pm": 3.7, "Dribbles pm": 5.1, "Fautes subies pm": 10.5},
    {"Équipe": "Cagliari", "Tirs pm": 11.9, "Tirs CA pm": 3.5, "Dribbles pm": 6.2, "Fautes subies pm": 10.4},
    {"Équipe": "AS Rome", "Tirs pm": 11.9, "Tirs CA pm": 4, "Dribbles pm": 5.6, "Fautes subies pm": 11.1},
    {"Équipe": "Frosinone", "Tirs pm": 11.7, "Tirs CA pm": 4, "Dribbles pm": 8.1, "Fautes subies pm": 11.9},
    {"Équipe": "Empoli", "Tirs pm": 11.6, "Tirs CA pm": 3.3, "Dribbles pm": 6.1, "Fautes subies pm": 13.3},
    {"Équipe": "Lazio Rome", "Tirs pm": 11.5, "Tirs CA pm": 3.6, "Dribbles pm": 7.2, "Fautes subies pm": 11.6},
    {"Équipe": "Torino", "Tirs pm": 11.5, "Tirs CA pm": 3.4, "Dribbles pm": 6.6, "Fautes subies pm": 10.5},
    {"Équipe": "Hellas Vérone", "Tirs pm": 11.3, "Tirs CA pm": 3.8, "Dribbles pm": 7, "Fautes subies pm": 12.1},
    {"Équipe": "Genoa", "Tirs pm": 10.8, "Tirs CA pm": 3.4, "Dribbles pm": 5, "Fautes subies pm": 10.8},
    {"Équipe": "Salernitana", "Tirs pm": 10.8, "Tirs CA pm": 3.3, "Dribbles pm": 5.8, "Fautes subies pm": 12.5}
]

#Inter Milan, Milan AC, Juventus Turin, Bologne, AS Rome, Atalanta Berga., Naples, Lazio Rome, Hellas Vérone

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