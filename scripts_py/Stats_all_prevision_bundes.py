import pymongo

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]

# Nom de la collection
collection_name = "Resultats_Predits_bundes"

# Supprimer la collection si elle existe déjà
if collection_name in db.list_collection_names():
    db[collection_name].drop()

# Nouvelle collection pour stocker les résultats prédits et les statistiques de prédiction
new_collection = db[collection_name]

# Parcourir chaque collection de matchs pour chaque saison
saisons = ["matchs_bundesliga", "matchs_bundesliga_22-23", "matchs_bundesliga_20-21"]

# Récupérer les moyennes des statistiques offensives pour chaque équipe
stats_off = {}
for document in db["Stats_off_prevision_bundes"].find():
    equipe = document["Équipe"]
    del document["_id"]
    del document["Équipe"]
    stats_off[equipe] = document

# Récupérer les moyennes des statistiques défensives pour chaque équipe
stats_def = {}
for document in db["Stats_def_prevision_bundes"].find():
    equipe = document["Équipe"]
    del document["_id"]
    del document["Équipe"]
    stats_def[equipe] = document

# Récupérer les moyennes des statistiques générales pour chaque équipe
stats_gen = {}
for document in db["Stats_gen_prevision_bundes"].find():
    equipe = document["Équipe"]
    del document["_id"]
    del document["Équipe"]
    stats_gen[equipe] = document

for saison in saisons:
    # Récupérer tous les matchs de la saison
    matchs = db[saison].find()

    # Parcourir chaque match
    for match in matchs:
        equipe_dom = match["Equipe_Domicile"]
        equipe_ext = match["Equipe_Extérieur"]
        score = match["Score"]

                # Extraire les statistiques pertinentes pour chaque équipe à partir des matchs
            # Extraire les statistiques pertinentes pour chaque équipe à partir des matchs
        stats_dom = stats_off.get(equipe_dom, {})
        possession_dom = stats_gen.get(equipe_dom, {}).get("Possession%", 0)  # Possession pour l'équipe domicile
        stats_dom["Possession%"] = min(round(possession_dom, 2), 100)  # Limiter la possession à 100% et arrondir à deux décimales
        stats_ext = stats_off.get(equipe_ext, {})
        possession_ext = stats_gen.get(equipe_ext, {}).get("Possession%", 0)  # Possession pour l'équipe extérieure
        stats_ext["Possession%"] = min(round(100 - possession_dom, 2), round(possession_ext, 2))  # Limiter la possession à ce qui reste de 100% et arrondir à deux décimales

        # Ajuster la possession en fonction du nombre de tirs par minute
        tirs_pm_dom = stats_dom.get("Tirs pm", 0)
        tirs_pm_ext = stats_ext.get("Tirs pm", 0)
        total_tirs_pm = tirs_pm_dom + tirs_pm_ext

        # Vérifier si la somme des tirs par minute est non nulle pour ajuster la possession
        if total_tirs_pm > 0:
            # Calculer la proportion de possession en fonction du nombre de tirs par minute
            proportion_dom = tirs_pm_dom / total_tirs_pm
            proportion_ext = tirs_pm_ext / total_tirs_pm

            # Ajuster la possession en fonction des proportions calculées
            stats_dom["Possession%"] = min(round(proportion_dom * 100, 2), 100)  # Arrondir à deux décimales
            stats_ext["Possession%"] = min(round(proportion_ext * 100, 2), 100)  # Arrondir à deux décimales
        else:
            # Si aucun tir n'a été enregistré, attribuer 50% de possession à chaque équipe
            stats_dom["Possession%"] = 50
            stats_ext["Possession%"] = 50

        # Calculer les prédictions en fonction des statistiques et de l'historique des performances
        # Vous devrez définir votre propre algorithme de prédiction ici
        # Par exemple, vous pourriez utiliser une formule qui prend en compte les statistiques et le classement actuel

        # Enregistrement des statistiques dans la nouvelle collection
        new_collection.insert_one({
            "Saison": saison,
            "Journée": match["Journée"],
            "Equipe_Domicile": equipe_dom,
            "Equipe_Extérieur": equipe_ext,
            "Score": score,
            "Stats_Domicile": stats_dom,
            "Stats_Extérieur": stats_ext,
            "Resultat_Predit": None  # à remplir lors du calcul des prédictions
        })

print("Extraction des données des matchs passés terminée. Nouvelle collection créée.")
