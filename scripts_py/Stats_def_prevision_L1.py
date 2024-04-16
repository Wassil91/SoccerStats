import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]



# Supprimer la collection si elle existe déjà
if "Stats_def_prevision_L1" in db.list_collection_names():
    db["Stats_def_prevision_L1"].drop()

# Recréer la collection
collection = db["Stats_def_prevision_L1"]

# Données pour les trois collections
data_22_23 = [
    {"Équipe": "Nice", "Tirs pm": 11.7, "Tacles pm": 21.5, "Interceptions pm": 10.9, "Fautes pm": 10.1, "Hors-jeux pm": 1.3},
    {"Équipe": "Lorient", "Tirs pm": 14.9, "Tacles pm": 19.4, "Interceptions pm": 9.7, "Fautes pm": 10.4, "Hors-jeux pm": 1.3},
    {"Équipe": "Lyon", "Tirs pm": 11.3, "Tacles pm": 19, "Interceptions pm": 12.7, "Fautes pm": 13.2, "Hors-jeux pm": 1.5},
    {"Équipe": "Toulouse", "Tirs pm": 14.2, "Tacles pm": 18.8, "Interceptions pm": 10.8, "Fautes pm": 12.4, "Hors-jeux pm": 1.9},
    {"Équipe": "Nantes", "Tirs pm": 12, "Tacles pm": 18.7, "Interceptions pm": 11.3, "Fautes pm": 12.7, "Hors-jeux pm": 1.4},
    {"Équipe": "Brest", "Tirs pm": 12.9, "Tacles pm": 18.6, "Interceptions pm": 12.5, "Fautes pm": 13.1, "Hors-jeux pm": 1.6},
    {"Équipe": "Reims", "Tirs pm": 12.8, "Tacles pm": 18.5, "Interceptions pm": 10.4, "Fautes pm": 13.3, "Hors-jeux pm": 2.2},
    {"Équipe": "Monaco", "Tirs pm": 13.5, "Tacles pm": 18.3, "Interceptions pm": 11.2, "Fautes pm": 12.8, "Hors-jeux pm": 2.2},
    {"Équipe": "Strasbourg", "Tirs pm": 11.2, "Tacles pm": 18.2, "Interceptions pm": 11.5, "Fautes pm": 13.9, "Hors-jeux pm": 1.1},
    {"Équipe": "Marseille", "Tirs pm": 11.2, "Tacles pm": 18, "Interceptions pm": 11.1, "Fautes pm": 14, "Hors-jeux pm": 1.8},
    {"Équipe": "Clermont F.", "Tirs pm": 12.8, "Tacles pm": 18, "Interceptions pm": 11.6, "Fautes pm": 11.2, "Hors-jeux pm": 1.4},
    {"Équipe": "Montpellier", "Tirs pm": 13.8, "Tacles pm": 18, "Interceptions pm": 10.6, "Fautes pm": 12.8, "Hors-jeux pm": 1.9},
    {"Équipe": "Auxerre", "Tirs pm": 12.4, "Tacles pm": 17.7, "Interceptions pm": 11.9, "Fautes pm": 12.2, "Hors-jeux pm": 1.3},
    {"Équipe": "Angers", "Tirs pm": 12.2, "Tacles pm": 17.2, "Interceptions pm": 9.9, "Fautes pm": 12.9, "Hors-jeux pm": 1.2},
    {"Équipe": "AC Ajaccio", "Tirs pm": 11.1, "Tacles pm": 16.8, "Interceptions pm": 11.1, "Fautes pm": 14.2, "Hors-jeux pm": 1.7},
    {"Équipe": "Lille", "Tirs pm": 8.7, "Tacles pm": 16.7, "Interceptions pm": 7.9, "Fautes pm": 13.1, "Hors-jeux pm": 1.7},
    {"Équipe": "Paris SG", "Tirs pm": 11.8, "Tacles pm": 16.1, "Interceptions pm": 7.8, "Fautes pm": 10, "Hors-jeux pm": 2},
    {"Équipe": "Troyes", "Tirs pm": 16.5, "Tacles pm": 16, "Interceptions pm": 10.5, "Fautes pm": 10.6, "Hors-jeux pm": 1.5},
    {"Équipe": "Rennes", "Tirs pm": 9.9, "Tacles pm": 15.8, "Interceptions pm": 9, "Fautes pm": 12.2, "Hors-jeux pm": 1.4},
    {"Équipe": "Lens", "Tirs pm": 10.3, "Tacles pm": 14.2, "Interceptions pm": 8.9, "Fautes pm": 12.9, "Hors-jeux pm": 1.6}
]

data_21_22 = [
    {"Équipe": "Monaco", "Tirs pm": 11, "Tacles pm": 19.6, "Interceptions pm": 11.2, "Fautes pm": 13.1, "Hors-jeux pm": 2.5},
    {"Équipe": "Metz", "Tirs pm": 14.8, "Tacles pm": 18.7, "Interceptions pm": 13.3, "Fautes pm": 13.5, "Hors-jeux pm": 1.3},
    {"Équipe": "Lorient", "Tirs pm": 12.2, "Tacles pm": 18.3, "Interceptions pm": 10.8, "Fautes pm": 12.5, "Hors-jeux pm": 1.4},
    {"Équipe": "Saint-Etienne", "Tirs pm": 12.1, "Tacles pm": 18.3, "Interceptions pm": 11.9, "Fautes pm": 12.2, "Hors-jeux pm": 1.3},
    {"Équipe": "Nantes", "Tirs pm": 12.5, "Tacles pm": 18, "Interceptions pm": 10.9, "Fautes pm": 13.2, "Hors-jeux pm": 1.4},
    {"Équipe": "Brest", "Tirs pm": 13, "Tacles pm": 17.9, "Interceptions pm": 12.3, "Fautes pm": 11.2, "Hors-jeux pm": 1.2},
    {"Équipe": "Strasbourg", "Tirs pm": 10.9, "Tacles pm": 17.4, "Interceptions pm": 11.4, "Fautes pm": 13.1, "Hors-jeux pm": 1.6},
    {"Équipe": "Lille", "Tirs pm": 11.8, "Tacles pm": 17.2, "Interceptions pm": 9.9, "Fautes pm": 11.7, "Hors-jeux pm": 2.3},
    {"Équipe": "Bordeaux", "Tirs pm": 13.7, "Tacles pm": 16.9, "Interceptions pm": 11.9, "Fautes pm": 12.1, "Hors-jeux pm": 1.5},
    {"Équipe": "Troyes", "Tirs pm": 12.2, "Tacles pm": 16.7, "Interceptions pm": 11.3, "Fautes pm": 11.6, "Hors-jeux pm": 1.4},
    {"Équipe": "Lyon", "Tirs pm": 12.2, "Tacles pm": 16.6, "Interceptions pm": 10.4, "Fautes pm": 11.5, "Hors-jeux pm": 1.6},
    {"Équipe": "Montpellier", "Tirs pm": 15.8, "Tacles pm": 16.4, "Interceptions pm": 9.4, "Fautes pm": 10.8, "Hors-jeux pm": 1.4},
    {"Équipe": "Clermont F.", "Tirs pm": 11.6, "Tacles pm": 16.4, "Interceptions pm": 10.2, "Fautes pm": 10.2, "Hors-jeux pm": 1.4},
    {"Équipe": "Angers", "Tirs pm": 11.3, "Tacles pm": 16.3, "Interceptions pm": 11.1, "Fautes pm": 12.4, "Hors-jeux pm": 1.2},
    {"Équipe": "Reims", "Tirs pm": 13.5, "Tacles pm": 16.2, "Interceptions pm": 13, "Fautes pm": 11.8, "Hors-jeux pm": 2},
    {"Équipe": "Nice", "Tirs pm": 11.4, "Tacles pm": 16.1, "Interceptions pm": 11.6, "Fautes pm": 12, "Hors-jeux pm": 1.4},
    {"Équipe": "Paris SG", "Tirs pm": 10.8, "Tacles pm": 16, "Interceptions pm": 8, "Fautes pm": 9.4, "Hors-jeux pm": 2.4},
    {"Équipe": "Lens", "Tirs pm": 10.9, "Tacles pm": 15.6, "Interceptions pm": 12, "Fautes pm": 11.1, "Hors-jeux pm": 1.5},
    {"Équipe": "Marseille", "Tirs pm": 8.7, "Tacles pm": 14.6, "Interceptions pm": 8.6, "Fautes pm": 9.1, "Hors-jeux pm": 1.6},
    {"Équipe": "Rennes", "Tirs pm": 9.4, "Tacles pm": 14.4, "Interceptions pm": 8.3, "Fautes pm": 11.4, "Hors-jeux pm": 1.6}
]

data_20_21 = [
    {"Équipe": "Monaco", "Tirs pm": 8.6, "Tacles pm": 19.3, "Interceptions pm": 11.5, "Fautes pm": 12.8, "Hors-jeux pm": 1.8},
    {"Équipe": "Lorient", "Tirs pm": 12.2, "Tacles pm": 19, "Interceptions pm": 13.9, "Fautes pm": 12.5, "Hors-jeux pm": 1.2},
    {"Équipe": "Saint-Etienne", "Tirs pm": 10.8, "Tacles pm": 18.8, "Interceptions pm": 12.4, "Fautes pm": 13.9, "Hors-jeux pm": 1.3},
    {"Équipe": "Nantes", "Tirs pm": 12.1, "Tacles pm": 18.3, "Interceptions pm": 12.2, "Fautes pm": 13.5, "Hors-jeux pm": 1.4},
    {"Équipe": "Metz", "Tirs pm": 12.6, "Tacles pm": 18, "Interceptions pm": 15, "Fautes pm": 13.6, "Hors-jeux pm": 1.3},
    {"Équipe": "Reims", "Tirs pm": 13.5, "Tacles pm": 17.3, "Interceptions pm": 12.8, "Fautes pm": 11.6, "Hors-jeux pm": 1.6},
    {"Équipe": "Marseille", "Tirs pm": 11.7, "Tacles pm": 17.2, "Interceptions pm": 11.6, "Fautes pm": 13.2, "Hors-jeux pm": 1.4},
    {"Équipe": "Lille", "Tirs pm": 8.9, "Tacles pm": 17.2, "Interceptions pm": 11.2, "Fautes pm": 12.8, "Hors-jeux pm": 1.7},
    {"Équipe": "Strasbourg", "Tirs pm": 10.1, "Tacles pm": 16.7, "Interceptions pm": 13.7, "Fautes pm": 13.1, "Hors-jeux pm": 1},
    {"Équipe": "Angers", "Tirs pm": 10.4, "Tacles pm": 16.5, "Interceptions pm": 11.7, "Fautes pm": 12.1, "Hors-jeux pm": 1},
    {"Équipe": "Nimes", "Tirs pm": 13.7, "Tacles pm": 16.4, "Interceptions pm": 13.2, "Fautes pm": 11.1, "Hors-jeux pm": 1.3},
    {"Équipe": "Paris SG", "Tirs pm": 10, "Tacles pm": 16, "Interceptions pm": 9.8, "Fautes pm": 12.2, "Hors-jeux pm": 1.7},
    {"Équipe": "Brest", "Tirs pm": 12.8, "Tacles pm": 16, "Interceptions pm": 12.3, "Fautes pm": 12.9, "Hors-jeux pm": 1.6},
    {"Équipe": "Lens", "Tirs pm": 10.8, "Tacles pm": 15.8, "Interceptions pm": 13.4, "Fautes pm": 14.2, "Hors-jeux pm": 1.3},
    {"Équipe": "Dijon", "Tirs pm": 14.1, "Tacles pm": 15.8, "Interceptions pm": 12.4, "Fautes pm": 13.6, "Hors-jeux pm": 1.4},
    {"Équipe": "Nice", "Tirs pm": 13.3, "Tacles pm": 15.8, "Interceptions pm": 11.4, "Fautes pm": 12.1, "Hors-jeux pm": 1.1},
    {"Équipe": "Rennes", "Tirs pm": 9.7, "Tacles pm": 15.4, "Interceptions pm": 9.5, "Fautes pm": 13.2, "Hors-jeux pm": 0.9},
    {"Équipe": "Lyon", "Tirs pm": 11.6, "Tacles pm": 15.4, "Interceptions pm": 10.9, "Fautes pm": 12.7, "Hors-jeux pm": 1.7},
    {"Équipe": "Bordeaux", "Tirs pm": 12.4, "Tacles pm": 14.9, "Interceptions pm": 13.2, "Fautes pm": 12.2, "Hors-jeux pm": 1.3},
    {"Équipe": "Montpellier", "Tirs pm": 14.7, "Tacles pm": 13.9, "Interceptions pm": 11.1, "Fautes pm": 13.5, "Hors-jeux pm": 1.3}
]


data_23_24 = [
    {"Équipe": "Brest", "Tirs pm": 10.2, "Tacles pm": 20.2, "Interceptions pm": 9.6, "Fautes pm": 12.3, "Hors-jeux pm": 1},
    {"Équipe": "Monaco", "Tirs pm": 11.5, "Tacles pm": 19.5, "Interceptions pm": 11.4, "Fautes pm": 14.9, "Hors-jeux pm": 2.2},
    {"Équipe": "Strasbourg", "Tirs pm": 12.7, "Tacles pm": 18.8, "Interceptions pm": 10.9, "Fautes pm": 14, "Hors-jeux pm": 1.2},
    {"Équipe": "Reims", "Tirs pm": 12.1, "Tacles pm": 18.3, "Interceptions pm": 10.1, "Fautes pm": 12.8, "Hors-jeux pm": 2.5},
    {"Équipe": "Montpellier", "Tirs pm": 15.4, "Tacles pm": 18.1, "Interceptions pm": 9.3, "Fautes pm": 12.6, "Hors-jeux pm": 1.7},
    {"Équipe": "Toulouse", "Tirs pm": 14, "Tacles pm": 18, "Interceptions pm": 8.8, "Fautes pm": 15, "Hors-jeux pm": 1.3},
    {"Équipe": "Lille", "Tirs pm": 9.4, "Tacles pm": 17.8, "Interceptions pm": 9.1, "Fautes pm": 11.1, "Hors-jeux pm": 1.5},
    {"Équipe": "Marseille", "Tirs pm": 10.7, "Tacles pm": 17.3, "Interceptions pm": 9.9, "Fautes pm": "11", "Hors-jeux pm": 1.9},
    {"Équipe": "Metz", "Tirs pm": 14.5, "Tacles pm": 17.3, "Interceptions pm": 10.3, "Fautes pm": 12.2, "Hors-jeux pm": 1.4},
    {"Équipe": "Clermont F.", "Tirs pm": 15.5, "Tacles pm": 17.1, "Interceptions pm": 10.2, "Fautes pm": 11.8, "Hors-jeux pm": 1.3},
    {"Équipe": "Paris SG", "Tirs pm": 12, "Tacles pm": 16.9, "Interceptions pm": 8.3, "Fautes pm": 10.7, "Hors-jeux pm": 1.6},
    {"Équipe": "Nantes", "Tirs pm": 12.9, "Tacles pm": 16.8, "Interceptions pm": 10.1, "Fautes pm": 12.5, "Hors-jeux pm": 1.5},
    {"Équipe": "Lorient", "Tirs pm": 16.2, "Tacles pm": 16.4, "Interceptions pm": 9.9, "Fautes pm": 11.8, "Hors-jeux pm": 1.4},
    {"Équipe": "Lyon", "Tirs pm": 13.3, "Tacles pm": 16.3, "Interceptions pm": 8.8, "Fautes pm": 12.8, "Hors-jeux pm": 1.2},
    {"Équipe": "Nice", "Tirs pm": 10.6, "Tacles pm": 15.8, "Interceptions pm": 8.3, "Fautes pm": 11.9, "Hors-jeux pm": 1.7},
    {"Équipe": "Le Havre", "Tirs pm": 13.4, "Tacles pm": 15.7, "Interceptions pm": 10.3, "Fautes pm": 13.7, "Hors-jeux pm": 1.6},
    {"Équipe": "Rennes", "Tirs pm": 12, "Tacles pm": 15.7, "Interceptions pm": 10.2, "Fautes pm": 11.6, "Hors-jeux pm": 1.1},
    {"Équipe": "Lens", "Tirs pm": 11.5, "Tacles pm": 15.7, "Interceptions pm": 8.5, "Fautes pm": 12.9, "Hors-jeux pm": 2.1}
]

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