import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]



# Supprimer la collection si elle existe déjà
if "Stats_off_prevision_L1" in db.list_collection_names():
    db["Stats_off_prevision_L1"].drop()

# Recréer la collection
collection = db["Stats_off_prevision_L1"]

# Données pour les trois collections
data_22_23 = [
    {"Équipe": "Paris SG", "Tirs pm": 15, "Tirs CA pm": 6.6, "Dribbles pm": 11.9, "Fautes subies pm": 10.9},
    {"Équipe": "Lille", "Tirs pm": 14.7, "Tirs CA pm": 5.7, "Dribbles pm": 10.5, "Fautes subies pm": 12.6},
    {"Équipe": "Marseille", "Tirs pm": 14.6, "Tirs CA pm": 4.9, "Dribbles pm": 9.4, "Fautes subies pm": 11},
    {"Équipe": "Rennes", "Tirs pm": 13.9, "Tirs CA pm": 5.1, "Dribbles pm": 10.8, "Fautes subies pm": 10},
    {"Équipe": "Lens", "Tirs pm": 13.9, "Tirs CA pm": 4.9, "Dribbles pm": 8.6, "Fautes subies pm": 12.7},
    {"Équipe": "Reims", "Tirs pm": 13.8, "Tirs CA pm": 4.7, "Dribbles pm": 9.1, "Fautes subies pm": 11.1},
    {"Équipe": "Lyon", "Tirs pm": 13.7, "Tirs CA pm": 5.4, "Dribbles pm": 10.5, "Fautes subies pm": 11.9},
    {"Équipe": "Nice", "Tirs pm": 13.5, "Tirs CA pm": 5.2, "Dribbles pm": 11.1, "Fautes subies pm": 9.9},
    {"Équipe": "Monaco", "Tirs pm": 12.6, "Tirs CA pm": 5.5, "Dribbles pm": 8.4, "Fautes subies pm": 10.8},
    {"Équipe": "Toulouse", "Tirs pm": 12, "Tirs CA pm": 4.8, "Dribbles pm": 7.4, "Fautes subies pm": 10.9},
    {"Équipe": "Strasbourg", "Tirs pm": 11.8, "Tirs CA pm": 4.3, "Dribbles pm": 8.9, "Fautes subies pm": 12.5},
    {"Équipe": "Nantes", "Tirs pm": 11.8, "Tirs CA pm": 4.2, "Dribbles pm": 9.9, "Fautes subies pm": 10.9},
    {"Équipe": "Montpellier", "Tirs pm": 11.6, "Tirs CA pm": 4.6, "Dribbles pm": 8.9, "Fautes subies pm": 13.8},
    {"Équipe": "Auxerre", "Tirs pm": 11.3, "Tirs CA pm": 3.3, "Dribbles pm": 8.9, "Fautes subies pm": 11},
    {"Équipe": "Brest", "Tirs pm": 11.2, "Tirs CA pm": 3.7, "Dribbles pm": 7.1, "Fautes subies pm": 12.2},
    {"Équipe": "Lorient", "Tirs pm": 10.6, "Tirs CA pm": 4.1, "Dribbles pm": 10.7, "Fautes subies pm": 13.2},
    {"Équipe": "Clermont F.", "Tirs pm": 10.6, "Tirs CA pm": 4, "Dribbles pm": 9.2, "Fautes subies pm": 15.6},
    {"Équipe": "Troyes", "Tirs pm": 10.4, "Tirs CA pm": 3.7, "Dribbles pm": 9.6, "Fautes subies pm": 10.6},
    {"Équipe": "Angers", "Tirs pm": 9.8, "Tirs CA pm": 3.4, "Dribbles pm": 11.6, "Fautes subies pm": 11.1},
    {"Équipe": "AC Ajaccio", "Tirs pm": 8.4, "Tirs CA pm": 2.3, "Dribbles pm": 7.4, "Fautes subies pm": 13}
]


data_21_22 = [
    {"Équipe": "Paris SG", "Tirs pm": 14.8, "Tirs CA pm": 5.7, "Dribbles pm": 12.9, "Fautes subies pm": 10.9},
    {"Équipe": "Lyon", "Tirs pm": 14.6, "Tirs CA pm": 5.4, "Dribbles pm": 12.8, "Fautes subies pm": 11.2},
    {"Équipe": "Rennes", "Tirs pm": 14.5, "Tirs CA pm": 5.3, "Dribbles pm": 7.6, "Fautes subies pm": 9.9},
    {"Équipe": "Lens", "Tirs pm": 13.6, "Tirs CA pm": 5.1, "Dribbles pm": 9.1, "Fautes subies pm": 9.4},
    {"Équipe": "Marseille", "Tirs pm": 13.1, "Tirs CA pm": 4.4, "Dribbles pm": 8.1, "Fautes subies pm": 12.1},
    {"Équipe": "Strasbourg", "Tirs pm": 12.6, "Tirs CA pm": 4.2, "Dribbles pm": 7.2, "Fautes subies pm": 10.5},
    {"Équipe": "Lille", "Tirs pm": 12.3, "Tirs CA pm": 4.1, "Dribbles pm": 8.2, "Fautes subies pm": 11.6},
    {"Équipe": "Monaco", "Tirs pm": 11.9, "Tirs CA pm": 4.4, "Dribbles pm": 9.6, "Fautes subies pm": 10.7},
    {"Équipe": "Nice", "Tirs pm": 11.9, "Tirs CA pm": 4, "Dribbles pm": 10.2, "Fautes subies pm": 9.8},
    {"Équipe": "Bordeaux", "Tirs pm": 11.8, "Tirs CA pm": 4, "Dribbles pm": 9.3, "Fautes subies pm": 10.3},
    {"Équipe": "Lorient", "Tirs pm": 11.7, "Tirs CA pm": 3.9, "Dribbles pm": 8.1, "Fautes subies pm": 11.1},
    {"Équipe": "Saint-Etienne", "Tirs pm": 11.6, "Tirs CA pm": 4, "Dribbles pm": 10.4, "Fautes subies pm": 11.6},
    {"Équipe": "Clermont F.", "Tirs pm": 11.5, "Tirs CA pm": 3.6, "Dribbles pm": 8.6, "Fautes subies pm": 12.7},
    {"Équipe": "Montpellier", "Tirs pm": 11.5, "Tirs CA pm": 4.2, "Dribbles pm": 8.3, "Fautes subies pm": 12.2},
    {"Équipe": "Nantes", "Tirs pm": 11.2, "Tirs CA pm": 4.1, "Dribbles pm": 8.8, "Fautes subies pm": 9.8},
    {"Équipe": "Nice", "Tirs pm": 10.8, "Tirs CA pm": 3.8, "Dribbles pm": 11.6, "Fautes subies pm": 11.5},
    {"Équipe": "Angers", "Tirs pm": 10.7, "Tirs CA pm": 3.8, "Dribbles pm": 10.2, "Fautes subies pm": 12.7},
    {"Équipe": "Nimes", "Tirs pm": 10.3, "Tirs CA pm": 3.7, "Dribbles pm": 8.4, "Fautes subies pm": 14},
    {"Équipe": "Marseille", "Tirs pm": 10, "Tirs CA pm": 3.6, "Dribbles pm": 8.4, "Fautes subies pm": 12.4},
    {"Équipe": "Reims", "Tirs pm": 9.6, "Tirs CA pm": 3.3, "Dribbles pm": 9.7, "Fautes subies pm": 10.9},
    {"Équipe": "Dijon", "Tirs pm": 9.2, "Tirs CA pm": 2.4, "Dribbles pm": 10.1, "Fautes subies pm": 11.1}
]

data_20_21 = [
{"Équipe": "Lyon", "Tirs pm": 16.1, "Tirs CA pm": 6.2, "Dribbles pm": 12.6, "Fautes subies pm": 11.6},
{"Équipe": "Paris SG", "Tirs pm": 15.0, "Tirs CA pm": 5.7, "Dribbles pm": 12.9, "Fautes subies pm": 11.3},
{"Équipe": "Rennes", "Tirs pm": 13.5, "Tirs CA pm": 4.3, "Dribbles pm": 10.7, "Fautes subies pm": 12.8},
{"Équipe": "Monaco", "Tirs pm": 12.8, "Tirs CA pm": 4.6, "Dribbles pm": 8.1, "Fautes subies pm": 12.4},
{"Équipe": "Lille", "Tirs pm": 12.8, "Tirs CA pm": 4.8, "Dribbles pm": 10.7, "Fautes subies pm": 11.8},
{"Équipe": "Montpellier", "Tirs pm": 12.2, "Tirs CA pm": 4.6, "Dribbles pm": 7.9, "Fautes subies pm": 11.9},
{"Équipe": "Brest", "Tirs pm": 11.8, "Tirs CA pm": 4.2, "Dribbles pm": 10.1, "Fautes subies pm": 11.4},
{"Équipe": "Lens", "Tirs pm": 11.7, "Tirs CA pm": 3.8, "Dribbles pm": 9.6, "Fautes subies pm": 12.2},
{"Équipe": "Saint-Etienne", "Tirs pm": 11.6, "Tirs CA pm": 4.0, "Dribbles pm": 11.4, "Fautes subies pm": 12.9},
{"Équipe": "Metz", "Tirs pm": 11.5, "Tirs CA pm": 3.7, "Dribbles pm": 10.0, "Fautes subies pm": 12.5},
{"Équipe": "Strasbourg", "Tirs pm": 11.3, "Tirs CA pm": 3.8, "Dribbles pm": 7.9, "Fautes subies pm": 10.8},
{"Équipe": "Lorient", "Tirs pm": 11.2, "Tirs CA pm": 3.7, "Dribbles pm": 9.5, "Fautes subies pm": 12.3},
{"Équipe": "Bordeaux", "Tirs pm": 11.1, "Tirs CA pm": 3.7, "Dribbles pm": 10.3, "Fautes subies pm": 12.6},
{"Équipe": "Nantes", "Tirs pm": 10.8, "Tirs CA pm": 3.4, "Dribbles pm": 9.9, "Fautes subies pm": 13.1},
{"Équipe": "Nice", "Tirs pm": 10.8, "Tirs CA pm": 3.8, "Dribbles pm": 11.6, "Fautes subies pm": 11.5},
{"Équipe": "Angers", "Tirs pm": 10.7, "Tirs CA pm": 3.8, "Dribbles pm": 10.2, "Fautes subies pm": 12.7},
{"Équipe": "Nimes", "Tirs pm": 10.3, "Tirs CA pm": 3.7, "Dribbles pm": 8.4, "Fautes subies pm": 14.0},
{"Équipe": "Marseille", "Tirs pm": 10.0, "Tirs CA pm": 3.6, "Dribbles pm": 8.4, "Fautes subies pm": 12.4},
{"Équipe": "Reims", "Tirs pm": 9.6, "Tirs CA pm": 3.3, "Dribbles pm": 9.7, "Fautes subies pm": 10.9},
{"Équipe": "Dijon", "Tirs pm": 9.2, "Tirs CA pm": 2.4, "Dribbles pm": 10.1, "Fautes subies pm": 11.1}
]

data_23_24 = [
    {"Équipe": "Monaco", "Tirs pm": 15.1, "Tirs CA pm": 6.2, "Dribbles pm": 9.3, "Fautes subies pm": 11.3},
    {"Équipe": "Paris SG", "Tirs pm": 15.0, "Tirs CA pm": 6.0, "Dribbles pm": 12.5, "Fautes subies pm": 11.3},
    {"Équipe": "Brest", "Tirs pm": 14.2, "Tirs CA pm": 5.2, "Dribbles pm": 10.1, "Fautes subies pm": 12.5},
    {"Équipe": "Lens", "Tirs pm": 13.9, "Tirs CA pm": 5.1, "Dribbles pm": 6.5, "Fautes subies pm": 12.6},
    {"Équipe": "Rennes", "Tirs pm": 13.7, "Tirs CA pm": 4.8, "Dribbles pm": 9.8, "Fautes subies pm": 12.5},
    {"Équipe": "Nice", "Tirs pm": 13.7, "Tirs CA pm": 4.7, "Dribbles pm": 11.6, "Fautes subies pm": 11.0},
    {"Équipe": "Marseille", "Tirs pm": 13.7, "Tirs CA pm": 4.5, "Dribbles pm": 8.0, "Fautes subies pm": 13.0},
    {"Équipe": "Montpellier", "Tirs pm": 13.6, "Tirs CA pm": 5.2, "Dribbles pm": 8.7, "Fautes subies pm": 13.2},
    {"Équipe": "Lille", "Tirs pm": 13.4, "Tirs CA pm": 5.3, "Dribbles pm": 10.4, "Fautes subies pm": 14.4},
    {"Équipe": "Lyon", "Tirs pm": 12.7, "Tirs CA pm": 4.9, "Dribbles pm": 10.7, "Fautes subies pm": 11.2},
    {"Équipe": "Reims", "Tirs pm": 12.5, "Tirs CA pm": 4.2, "Dribbles pm": 10.0, "Fautes subies pm": 9.4},
    {"Équipe": "Toulouse", "Tirs pm": 11.8, "Tirs CA pm": 4.2, "Dribbles pm": 8.5, "Fautes subies pm": 12.7},
    {"Équipe": "Clermont F.", "Tirs pm": 11.7, "Tirs CA pm": 3.9, "Dribbles pm": 7.3, "Fautes subies pm": 12.4},
    {"Équipe": "Nantes", "Tirs pm": 11.4, "Tirs CA pm": 4.0, "Dribbles pm": 6.5, "Fautes subies pm": 11.2},
    {"Équipe": "Le Havre", "Tirs pm": 11.2, "Tirs CA pm": 3.6, "Dribbles pm": 7.5, "Fautes subies pm": 10.5},
    {"Équipe": "Strasbourg", "Tirs pm": 11.0, "Tirs CA pm": 3.6, "Dribbles pm": 9.3, "Fautes subies pm": 12.2},
    {"Équipe": "Metz", "Tirs pm": 9.9, "Tirs CA pm": 3.3, "Dribbles pm": 8.1, "Fautes subies pm": 11.2},
    {"Équipe": "Lorient", "Tirs pm": 9.5, "Tirs CA pm": 3.7, "Dribbles pm": 10.1, "Fautes subies pm": 12.7}

#     {"Équipe": "Monaco", "Tirs pm": 15.1, "Tirs CA pm": 6.2, "Dribbles pm": 9.3, "Fautes subies pm": 11.3, "Journée": 27},
#     {"Équipe": "Paris SG", "Tirs pm": 15.0, "Tirs CA pm": 6.0, "Dribbles pm": 12.5, "Fautes subies pm": 11.3, "Journée": 27},
#     {"Équipe": "Brest", "Tirs pm": 14.2, "Tirs CA pm": 5.2, "Dribbles pm": 10.1, "Fautes subies pm": 12.5, "Journée": 27},
#     {"Équipe": "Lens", "Tirs pm": 13.9, "Tirs CA pm": 5.1, "Dribbles pm": 6.5, "Fautes subies pm": 12.6, "Journée": 27},
#     {"Équipe": "Rennes", "Tirs pm": 13.7, "Tirs CA pm": 4.8, "Dribbles pm": 9.8, "Fautes subies pm": 12.5, "Journée": 27},
#     {"Équipe": "Nice", "Tirs pm": 13.7, "Tirs CA pm": 4.7, "Dribbles pm": 11.6, "Fautes subies pm": 11.0, "Journée": 27},
#     {"Équipe": "Marseille", "Tirs pm": 13.7, "Tirs CA pm": 4.5, "Dribbles pm": 8.0, "Fautes subies pm": 13.0, "Journée": 27},
#     {"Équipe": "Montpellier", "Tirs pm": 13.6, "Tirs CA pm": 5.2, "Dribbles pm": 8.7, "Fautes subies pm": 13.2, "Journée": 27},
#     {"Équipe": "Lille", "Tirs pm": 13.4, "Tirs CA pm": 5.3, "Dribbles pm": 10.4, "Fautes subies pm": 14.4, "Journée": 27},
#     {"Équipe": "Lyon", "Tirs pm": 12.7, "Tirs CA pm": 4.9, "Dribbles pm": 10.7, "Fautes subies pm": 11.2, "Journée": 27},
#     {"Équipe": "Reims", "Tirs pm": 12.5, "Tirs CA pm": 4.2, "Dribbles pm": 10.0, "Fautes subies pm": 9.4, "Journée": 27},
#     {"Équipe": "Toulouse", "Tirs pm": 11.8, "Tirs CA pm": 4.2, "Dribbles pm": 8.5, "Fautes subies pm": 12.7, "Journée": 27},
#     {"Équipe": "Clermont F.", "Tirs pm": 11.7, "Tirs CA pm": 3.9, "Dribbles pm": 7.3, "Fautes subies pm": 12.4, "Journée": 27},
#     {"Équipe": "Nantes", "Tirs pm": 11.4, "Tirs CA pm": 4.0, "Dribbles pm": 6.5, "Fautes subies pm": 11.2, "Journée": 27},
#     {"Équipe": "Le Havre", "Tirs pm": 11.2, "Tirs CA pm": 3.6, "Dribbles pm": 7.5, "Fautes subies pm": 10.5, "Journée": 27},
#     {"Équipe": "Strasbourg", "Tirs pm": 11.0, "Tirs CA pm": 3.6, "Dribbles pm": 9.3, "Fautes subies pm": 12.2, "Journée": 27},
#     {"Équipe": "Metz", "Tirs pm": 9.9, "Tirs CA pm": 3.3, "Dribbles pm": 8.1, "Fautes subies pm": 11.2, "Journée": 27},
#     {"Équipe": "Lorient", "Tirs pm": 9.5, "Tirs CA pm": 3.7, "Dribbles pm": 10.1, "Fautes subies pm": 12.7, "Journée": 27},

#     {"Équipe": "Monaco", "Tirs pm": 15.1, "Tirs CA pm": 6.2, "Dribbles pm": 9.3, "Fautes subies pm": 11.3, "Journée": 28},
#     {"Équipe": "Paris SG", "Tirs pm": 15.0, "Tirs CA pm": 6.0, "Dribbles pm": 12.5, "Fautes subies pm": 11.3, "Journée": 28},
#     {"Équipe": "Brest", "Tirs pm": 14.2, "Tirs CA pm": 5.2, "Dribbles pm": 10.1, "Fautes subies pm": 12.5, "Journée": 28},
#     {"Équipe": "Lens", "Tirs pm": 13.9, "Tirs CA pm": 5.1, "Dribbles pm": 6.5, "Fautes subies pm": 12.6, "Journée": 28},
#     {"Équipe": "Rennes", "Tirs pm": 13.7, "Tirs CA pm": 4.8, "Dribbles pm": 9.8, "Fautes subies pm": 12.5, "Journée": 28},
#     {"Équipe": "Nice", "Tirs pm": 13.7, "Tirs CA pm": 4.7, "Dribbles pm": 11.6, "Fautes subies pm": 11.0, "Journée": 28},
#     {"Équipe": "Marseille", "Tirs pm": 13.7, "Tirs CA pm": 4.5, "Dribbles pm": 8.0, "Fautes subies pm": 13.0, "Journée": 28},
#     {"Équipe": "Montpellier", "Tirs pm": 13.6, "Tirs CA pm": 5.2, "Dribbles pm": 8.7, "Fautes subies pm": 13.2, "Journée": 28},
#     {"Équipe": "Lille", "Tirs pm": 13.4, "Tirs CA pm": 5.3, "Dribbles pm": 10.4, "Fautes subies pm": 14.4, "Journée": 28},
#     {"Équipe": "Lyon", "Tirs pm": 12.7, "Tirs CA pm": 4.9, "Dribbles pm": 10.7, "Fautes subies pm": 11.2, "Journée": 28},
#     {"Équipe": "Reims", "Tirs pm": 12.5, "Tirs CA pm": 4.2, "Dribbles pm": 10.0, "Fautes subies pm": 9.4, "Journée": 28},
#     {"Équipe": "Toulouse", "Tirs pm": 11.8, "Tirs CA pm": 4.2, "Dribbles pm": 8.5, "Fautes subies pm": 12.7, "Journée": 28},
#     {"Équipe": "Clermont F.", "Tirs pm": 11.7, "Tirs CA pm": 3.9, "Dribbles pm": 7.3, "Fautes subies pm": 12.4, "Journée": 28},
#     {"Équipe": "Nantes", "Tirs pm": 11.4, "Tirs CA pm": 4.0, "Dribbles pm": 6.5, "Fautes subies pm": 11.2, "Journée": 28},
#     {"Équipe": "Le Havre", "Tirs pm": 11.2, "Tirs CA pm": 3.6, "Dribbles pm": 7.5, "Fautes subies pm": 10.5, "Journée": 28},
#     {"Équipe": "Strasbourg", "Tirs pm": 11.0, "Tirs CA pm": 3.6, "Dribbles pm": 9.3, "Fautes subies pm": 12.2, "Journée": 28},
#     {"Équipe": "Metz", "Tirs pm": 9.9, "Tirs CA pm": 3.3, "Dribbles pm": 8.1, "Fautes subies pm": 11.2, "Journée": 28},
#     {"Équipe": "Lorient", "Tirs pm": 9.5, "Tirs CA pm": 3.7, "Dribbles pm": 10.1, "Fautes subies pm": 12.7, "Journée": 28}
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