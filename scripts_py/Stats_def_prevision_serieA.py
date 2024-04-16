import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]



# Supprimer la collection si elle existe déjà
if "Stats_def_prevision_serieA" in db.list_collection_names():
    db["Stats_def_prevision_serieA"].drop()

# Recréer la collection
collection = db["Stats_def_prevision_serieA"]

# Données pour les trois collections
data_22_23 = [
    {"Équipe": "Lecce", "Tirs pm": 11.8, "Tacles pm": 18.6, "Interceptions pm": 9.7, "Fautes pm": 14.6, "Hors-jeux pm": 1.8},
    {"Équipe": "Cremonese", "Tirs pm": 15.3, "Tacles pm": 18.5, "Interceptions pm": 10.3, "Fautes pm": 12.1, "Hors-jeux pm": 1.8},
    {"Équipe": "Milan AC", "Tirs pm": 10.7, "Tacles pm": 18.2, "Interceptions pm": 7.6, "Fautes pm": 11.8, "Hors-jeux pm": 1.6},
    {"Équipe": "Udinese", "Tirs pm": 12.3, "Tacles pm": 17.1, "Interceptions pm": 8.3, "Fautes pm": 12.6, "Hors-jeux pm": 1.3},
    {"Équipe": "Bologne", "Tirs pm": 12.7, "Tacles pm": 17, "Interceptions pm": 7.9, "Fautes pm": 12.6, "Hors-jeux pm": 2},
    {"Équipe": "Atalanta Berga.", "Tirs pm": 11.8, "Tacles pm": 16.7, "Interceptions pm": 11.1, "Fautes pm": 12.4, "Hors-jeux pm": 1.5},
    {"Équipe": "AS Rome", "Tirs pm": 10.4, "Tacles pm": 16.4, "Interceptions pm": 9.7, "Fautes pm": 11.7, "Hors-jeux pm": 1.5},
    {"Équipe": "Spezia", "Tirs pm": 15.8, "Tacles pm": 16.4, "Interceptions pm": 7.9, "Fautes pm": 13.7, "Hors-jeux pm": 1.4},
    {"Équipe": "Empoli", "Tirs pm": 16.1, "Tacles pm": 15.9, "Interceptions pm": 7.4, "Fautes pm": 11.6, "Hors-jeux pm": 1.4},
    {"Équipe": "Hellas Vérone", "Tirs pm": 14.2, "Tacles pm": 15.9, "Interceptions pm": 8.9, "Fautes pm": 14.2, "Hors-jeux pm": 2},
    {"Équipe": "Monza", "Tirs pm": 12.8, "Tacles pm": 15.7, "Interceptions pm": 8.8, "Fautes pm": 12.9, "Hors-jeux pm": 1.8},
    {"Équipe": "Juventus Turin", "Tirs pm": 12.3, "Tacles pm": 15.6, "Interceptions pm": 8.1, "Fautes pm": 12, "Hors-jeux pm": 1.6},
    {"Équipe": "Sampdoria", "Tirs pm": 15.8, "Tacles pm": 15.5, "Interceptions pm": 8.3, "Fautes pm": 13.3, "Hors-jeux pm": 1.4},
    {"Équipe": "Salernitana", "Tirs pm": 15.6, "Tacles pm": 15.4, "Interceptions pm": 8.7, "Fautes pm": 12.4, "Hors-jeux pm": 1.8},
    {"Équipe": "Lazio Rome", "Tirs pm": 12.2, "Tacles pm": 15.3, "Interceptions pm": 8.2, "Fautes pm": 10.5, "Hors-jeux pm": 1.4},
    {"Équipe": "Fiorentina", "Tirs pm": 9.1, "Tacles pm": 15.3, "Interceptions pm": 6.8, "Fautes pm": 12.8, "Hors-jeux pm": 1.5},
    {"Équipe": "Inter Milan", "Tirs pm": 11.2, "Tacles pm": 15.1, "Interceptions pm": 8.3, "Fautes pm": 11.7, "Hors-jeux pm": 1.8},
    {"Équipe": "Naples", "Tirs pm": 9.6, "Tacles pm": 15, "Interceptions pm": 7.6, "Fautes pm": 10.2, "Hors-jeux pm": 1.6},
    {"Équipe": "Sassuolo", "Tirs pm": 12.7, "Tacles pm": 13.2, "Interceptions pm": 7.3, "Fautes pm": 10.7, "Hors-jeux pm": 1.1},
    {"Équipe": "Torino", "Tirs pm": 11.4, "Tacles pm": 12.4, "Interceptions pm": 7.7, "Fautes pm": 13.3, "Hors-jeux pm": 2.1}
]


data_21_22 = [
    {"Équipe": "Genoa", "Tirs pm": 14.2, "Tacles pm": 19.4, "Interceptions pm": 11.8, "Fautes pm": 14.9, "Hors-jeux pm": 1.9},
    {"Équipe": "Milan AC", "Tirs pm": 10.6, "Tacles pm": 18.3, "Interceptions pm": 9.3, "Fautes pm": 12.2, "Hors-jeux pm": 2.1},
    {"Équipe": "Atalanta Berga.", "Tirs pm": 10.3, "Tacles pm": 16.6, "Interceptions pm": 11.6, "Fautes pm": 13.5, "Hors-jeux pm": 1.8},
    {"Équipe": "Udinese", "Tirs pm": 12.3, "Tacles pm": 16.4, "Interceptions pm": 9.2, "Fautes pm": 14.8, "Hors-jeux pm": 2.1},
    {"Équipe": "AS Rome", "Tirs pm": 9.9, "Tacles pm": 16.1, "Interceptions pm": 10.1, "Fautes pm": 13.2, "Hors-jeux pm": 1.6},
    {"Équipe": "Venezia", "Tirs pm": 17.8, "Tacles pm": 16.1, "Interceptions pm": 10.2, "Fautes pm": 13.7, "Hors-jeux pm": 1.6},
    {"Équipe": "Inter Milan", "Tirs pm": 10.6, "Tacles pm": 15.5, "Interceptions pm": 8.7, "Fautes pm": 12.3, "Hors-jeux pm": 1.5},
    {"Équipe": "Spezia", "Tirs pm": 16.9, "Tacles pm": 15.3, "Interceptions pm": 9.8, "Fautes pm": 13.4, "Hors-jeux pm": 2},
    {"Équipe": "Lazio Rome", "Tirs pm": 12.4, "Tacles pm": 15.2, "Interceptions pm": 8.3, "Fautes pm": 11.6, "Hors-jeux pm": 1.2},
    {"Équipe": "Empoli", "Tirs pm": 17.6, "Tacles pm": 14.9, "Interceptions pm": 9.3, "Fautes pm": 13.3, "Hors-jeux pm": 2.2},
    {"Équipe": "Sampdoria", "Tirs pm": 14.6, "Tacles pm": 14.8, "Interceptions pm": 11.7, "Fautes pm": 13, "Hors-jeux pm": 2.2},
    {"Équipe": "Cagliari", "Tirs pm": 14.6, "Tacles pm": 14.7, "Interceptions pm": 10.4, "Fautes pm": 14.4, "Hors-jeux pm": 1.8},
    {"Équipe": "Juventus Turin", "Tirs pm": 11.7, "Tacles pm": 14.6, "Interceptions pm": 8.7, "Fautes pm": 13.4, "Hors-jeux pm": 1.9},
    {"Équipe": "Naples", "Tirs pm": 11, "Tacles pm": 14.5, "Interceptions pm": 7.8, "Fautes pm": 12.1, "Hors-jeux pm": 1.8},
    {"Équipe": "Salernitana", "Tirs pm": 15.6, "Tacles pm": 14.2, "Interceptions pm": 9.3, "Fautes pm": 13, "Hors-jeux pm": 1.1},
    {"Équipe": "Hellas Vérone", "Tirs pm": 12.5, "Tacles pm": 14.1, "Interceptions pm": 10.3, "Fautes pm": 14.7, "Hors-jeux pm": 2.6},
    {"Équipe": "Torino", "Tirs pm": 10.4, "Tacles pm": 14, "Interceptions pm": 11.1, "Fautes pm": 16.7, "Hors-jeux pm": 2.2},
    {"Équipe": "Bologne", "Tirs pm": 14.2, "Tacles pm": 13.9, "Interceptions pm": 9.7, "Fautes pm": 11.7, "Hors-jeux pm": 1.5},
    {"Équipe": "Fiorentina", "Tirs pm": 9.4, "Tacles pm": 13.4, "Interceptions pm": 6.4, "Fautes pm": 12.2, "Hors-jeux pm": 1.5},
    {"Équipe": "Sassuolo", "Tirs pm": 14, "Tacles pm": 12.7, "Interceptions pm": 8.3, "Fautes pm": 12.1, "Hors-jeux pm": 1.1}
]


data_20_21 = [
    {"Équipe": "Milan AC", "Tirs pm": 12.2, "Tacles pm": 16.5, "Interceptions pm": 10.1, "Fautes pm": 13.5, "Hors-jeux pm": 2.4},
    {"Équipe": "Atalanta Berga.", "Tirs pm": 8.6, "Tacles pm": 15.9, "Interceptions pm": 13.6, "Fautes pm": 15.3, "Hors-jeux pm": 1.5},
    {"Équipe": "Lazio Rome", "Tirs pm": 10.6, "Tacles pm": 15.7, "Interceptions pm": 11.7, "Fautes pm": 13.1, "Hors-jeux pm": 2.3},
    {"Équipe": "Cagliari", "Tirs pm": 14.6, "Tacles pm": 15.5, "Interceptions pm": 10.2, "Fautes pm": 13.6, "Hors-jeux pm": 1.5},
    {"Équipe": "Parma Calcio 1913", "Tirs pm": 13.9, "Tacles pm": 15.4, "Interceptions pm": 12.2, "Fautes pm": 13.8, "Hors-jeux pm": 2.1},
    {"Équipe": "Sampdoria", "Tirs pm": 13.6, "Tacles pm": 15.2, "Interceptions pm": 11.9, "Fautes pm": 13.4, "Hors-jeux pm": 1.6},
    {"Équipe": "Naples", "Tirs pm": 10.3, "Tacles pm": 15.1, "Interceptions pm": 9.7, "Fautes pm": 10.2, "Hors-jeux pm": 1.9},
    {"Équipe": "Spezia", "Tirs pm": 11.2, "Tacles pm": 14.9, "Interceptions pm": 10.4, "Fautes pm": 15.5, "Hors-jeux pm": 2.2},
    {"Équipe": "Juventus Turin", "Tirs pm": 11.4, "Tacles pm": 14.9, "Interceptions pm": 12.1, "Fautes pm": 13.3, "Hors-jeux pm": 2.4},
    {"Équipe": "Genoa", "Tirs pm": 14.3, "Tacles pm": 14.6, "Interceptions pm": 13.7, "Fautes pm": 13.7, "Hors-jeux pm": 2.1},
    {"Équipe": "Bologne", "Tirs pm": 14.2, "Tacles pm": 14.3, "Interceptions pm": 11.2, "Fautes pm": 14.1, "Hors-jeux pm": 1.9},
    {"Équipe": "Crotone", "Tirs pm": 15.6, "Tacles pm": 14.2, "Interceptions pm": 10, "Fautes pm": 14, "Hors-jeux pm": 1.5},
    {"Équipe": "Torino", "Tirs pm": 13.3, "Tacles pm": 13.9, "Interceptions pm": 11.7, "Fautes pm": 15.2, "Hors-jeux pm": 1.6},
    {"Équipe": "Sassuolo", "Tirs pm": 13.9, "Tacles pm": 13.9, "Interceptions pm": 11.2, "Fautes pm": 13, "Hors-jeux pm": 1.5},
    {"Équipe": "Benevento", "Tirs pm": 13.2, "Tacles pm": 13.4, "Interceptions pm": 10.1, "Fautes pm": 13.1, "Hors-jeux pm": 1.6},
    {"Équipe": "Inter Milan", "Tirs pm": 10.2, "Tacles pm": 13.3, "Interceptions pm": 9.8, "Fautes pm": 12.4, "Hors-jeux pm": 1.7},
    {"Équipe": "Udinese", "Tirs pm": 12.2, "Tacles pm": 13.2, "Interceptions pm": 11.6, "Fautes pm": 13.4, "Hors-jeux pm": 2.2},
    {"Équipe": "Fiorentina", "Tirs pm": 12, "Tacles pm": 13.1, "Interceptions pm": 10.2, "Fautes pm": 13.8, "Hors-jeux pm": 1.6},
    {"Équipe": "AS Rome", "Tirs pm": 10.4, "Tacles pm": 12.9, "Interceptions pm": 12.9, "Fautes pm": 14.6, "Hors-jeux pm": 1.6},
    {"Équipe": "Hellas Vérone", "Tirs pm": 14.1, "Tacles pm": 12.5, "Interceptions pm": 12.9, "Fautes pm": 16.4, "Hors-jeux pm": 2.8}
]


data_23_24 = [
    {"Équipe": "Genoa", "Tirs pm": 12.5, "Tacles pm": 17.1, "Interceptions pm": 8.9, "Fautes pm": 11.8, "Hors-jeux pm": 1},
    {"Équipe": "Bologne", "Tirs pm": 10.7, "Tacles pm": 16.6, "Interceptions pm": 6.9, "Fautes pm": 12.2, "Hors-jeux pm": 1.3},
    {"Équipe": "Empoli", "Tirs pm": 15, "Tacles pm": 16.3, "Interceptions pm": 6.9, "Fautes pm": 13.3, "Hors-jeux pm": 2.1},
    {"Équipe": "Hellas Vérone", "Tirs pm": 13.7, "Tacles pm": 16.2, "Interceptions pm": 7.4, "Fautes pm": 13.7, "Hors-jeux pm": 1.6},
    {"Équipe": "Atalanta Berga.", "Tirs pm": 12.2, "Tacles pm": 16.1, "Interceptions pm": 9.1, "Fautes pm": 12.6, "Hors-jeux pm": 1.6},
    {"Équipe": "Lecce", "Tirs pm": 13.8, "Tacles pm": 16, "Interceptions pm": 8.8, "Fautes pm": 13.5, "Hors-jeux pm": 1.6},
    {"Équipe": "Fiorentina", "Tirs pm": 10.3, "Tacles pm": 15.6, "Interceptions pm": 6.2, "Fautes pm": 12.6, "Hors-jeux pm": 1.4},
    {"Équipe": "Monza", "Tirs pm": 14.7, "Tacles pm": 15.4, "Interceptions pm": 7.5, "Fautes pm": 11.8, "Hors-jeux pm": 1.2},
    {"Équipe": "Milan AC", "Tirs pm": 13.5, "Tacles pm": 15.3, "Interceptions pm": 6.8, "Fautes pm": 11.7, "Hors-jeux pm": 1.6},
    {"Équipe": "Udinese", "Tirs pm": 12.6, "Tacles pm": 15.3, "Interceptions pm": 9.2, "Fautes pm": 13, "Hors-jeux pm": 1.3},
    {"Équipe": "Lazio Rome", "Tirs pm": 12.9, "Tacles pm": 15.2, "Interceptions pm": 7.7, "Fautes pm": 11.9, "Hors-jeux pm": 1.6},
    {"Équipe": "Juventus Turin", "Tirs pm": 11.5, "Tacles pm": 15.2, "Interceptions pm": 7.1, "Fautes pm": 12.2, "Hors-jeux pm": 1.9},
    {"Équipe": "Naples", "Tirs pm": 10.5, "Tacles pm": 14.8, "Interceptions pm": 6.1, "Fautes pm": 9.7, "Hors-jeux pm": 2},
    {"Équipe": "AS Rome", "Tirs pm": 11.3, "Tacles pm": 14.8, "Interceptions pm": 6.1, "Fautes pm": 12.6, "Hors-jeux pm": 2},
    {"Équipe": "Inter Milan", "Tirs pm": 10.1, "Tacles pm": 14.6, "Interceptions pm": 7.5, "Fautes pm": 11.1, "Hors-jeux pm": 1.7},
    {"Équipe": "Frosinone", "Tirs pm": 15, "Tacles pm": 14.5, "Interceptions pm": 6.7, "Fautes pm": 10, "Hors-jeux pm": 1.4},
    {"Équipe": "Cagliari", "Tirs pm": 13.3, "Tacles pm": 14, "Interceptions pm": 7.2, "Fautes pm": 12.3, "Hors-jeux pm": 1.6},
    {"Équipe": "Salernitana", "Tirs pm": 15.4, "Tacles pm": 13.7, "Interceptions pm": 7.2, "Fautes pm": 12, "Hors-jeux pm": 1.8},
    {"Équipe": "Sassuolo", "Tirs pm": 14.8, "Tacles pm": 13.3, "Interceptions pm": 6.8, "Fautes pm": 10, "Hors-jeux pm": 0.7},
    {"Équipe": "Torino", "Tirs pm": 10.9, "Tacles pm": 11.9, "Interceptions pm": 8.5, "Fautes pm": 12.3, "Hors-jeux pm": 1.8}
]

#Inter Milan, Milan AC, Juventus  Turin, Bologne, AS Rome, Atalanta Berga., Naples, Lazio Rome, Hellas Vérone

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