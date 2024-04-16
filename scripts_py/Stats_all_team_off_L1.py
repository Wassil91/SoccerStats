from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import csv

html_content = """
<div id="statistics-team-table-offensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsPerGame   desc  " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable shotOnTargetPerGame   " data-stat-name="shotOnTargetPerGame">Tirs CA pm</th><th class="global sortable dribbleWonPerGame   " data-stat-name="dribbleWonPerGame">Dribbles pm</th><th class="global sortable foulGivenPerGame   " data-stat-name="foulGivenPerGame">Fautes subies pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/248/Show/France-Monaco">1. Monaco</a></td> <td class="shotsPerGame   sorted  ">15.1</td><td class="shotOnTargetPerGame   ">6.2</td><td class="dribbleWonPerGame   ">9.3</td><td class="foulGivenPerGame   ">11.3</td><td class=" "><span class="stat-value rating">6.76</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/304/Show/France-Paris-Saint-Germain">2. Paris Saint-Germain</a></td> <td class="shotsPerGame   sorted  ">15</td><td class="shotOnTargetPerGame   ">6</td><td class="dribbleWonPerGame   ">12.5</td><td class="foulGivenPerGame   ">11.3</td><td class=" "><span class="stat-value rating">6.95</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2332/Show/France-Brest">3. Brest</a></td> <td class="shotsPerGame   sorted  ">14.2</td><td class="shotOnTargetPerGame   ">5.2</td><td class="dribbleWonPerGame   ">10.1</td><td class="foulGivenPerGame   ">12.5</td><td class=" "><span class="stat-value rating">6.76</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/309/Show/France-Lens">4. Lens</a></td> <td class="shotsPerGame   sorted  ">13.9</td><td class="shotOnTargetPerGame   ">5.1</td><td class="dribbleWonPerGame   ">6.5</td><td class="foulGivenPerGame   ">12.6</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/313/Show/France-Rennes">5. Rennes</a></td> <td class="shotsPerGame   sorted  ">13.7</td><td class="shotOnTargetPerGame   ">4.8</td><td class="dribbleWonPerGame   ">9.8</td><td class="foulGivenPerGame   ">12.5</td><td class=" "><span class="stat-value rating">6.67</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/613/Show/France-Nice">6. Nice</a></td> <td class="shotsPerGame   sorted  ">13.7</td><td class="shotOnTargetPerGame   ">4.7</td><td class="dribbleWonPerGame   ">11.6</td><td class="foulGivenPerGame   ">11</td><td class=" "><span class="stat-value rating">6.65</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/249/Show/France-Marseille">7. Marseille</a></td> <td class="shotsPerGame   sorted  ">13.7</td><td class="shotOnTargetPerGame   ">4.5</td><td class="dribbleWonPerGame   ">8</td><td class="foulGivenPerGame   ">13</td><td class=" "><span class="stat-value rating">6.73</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/311/Show/France-Montpellier">8. Montpellier</a></td> <td class="shotsPerGame   sorted  ">13.6</td><td class="shotOnTargetPerGame   ">5.2</td><td class="dribbleWonPerGame   ">8.7</td><td class="foulGivenPerGame   ">13.2</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/607/Show/France-Lille">9. Lille</a></td> <td class="shotsPerGame   sorted  ">13.4</td><td class="shotOnTargetPerGame   ">5.3</td><td class="dribbleWonPerGame   ">10.4</td><td class="foulGivenPerGame   ">14.4</td><td class=" "><span class="stat-value rating">6.76</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/228/Show/France-Lyon">10. Lyon</a></td> <td class="shotsPerGame   sorted  ">12.7</td><td class="shotOnTargetPerGame   ">4.9</td><td class="dribbleWonPerGame   ">10.7</td><td class="foulGivenPerGame   ">11.2</td><td class=" "><span class="stat-value rating">6.59</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/950/Show/France-Reims">11. Reims</a></td> <td class="shotsPerGame   sorted  ">12.5</td><td class="shotOnTargetPerGame   ">4.2</td><td class="dribbleWonPerGame   ">10</td><td class="foulGivenPerGame   ">9.4</td><td class=" "><span class="stat-value rating">6.63</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/246/Show/France-Toulouse">12. Toulouse</a></td> <td class="shotsPerGame   sorted  ">11.8</td><td class="shotOnTargetPerGame   ">4.2</td><td class="dribbleWonPerGame   ">8.5</td><td class="foulGivenPerGame   ">12.7</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/941/Show/France-Clermont-Foot">13. Clermont Foot</a></td> <td class="shotsPerGame   sorted  ">11.7</td><td class="shotOnTargetPerGame   ">3.9</td><td class="dribbleWonPerGame   ">7.3</td><td class="foulGivenPerGame   ">12.4</td><td class=" "><span class="stat-value rating">6.48</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/302/Show/France-Nantes">14. Nantes</a></td> <td class="shotsPerGame   sorted  ">11.4</td><td class="shotOnTargetPerGame   ">4</td><td class="dribbleWonPerGame   ">6.5</td><td class="foulGivenPerGame   ">11.2</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/217/Show/France-Le-Havre">15. Le Havre</a></td> <td class="shotsPerGame   sorted  ">11.2</td><td class="shotOnTargetPerGame   ">3.6</td><td class="dribbleWonPerGame   ">7.5</td><td class="foulGivenPerGame   ">10.5</td><td class=" "><span class="stat-value rating">6.53</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/148/Show/France-Strasbourg">16. Strasbourg</a></td> <td class="shotsPerGame   sorted  ">11</td><td class="shotOnTargetPerGame   ">3.6</td><td class="dribbleWonPerGame   ">9.3</td><td class="foulGivenPerGame   ">12.2</td><td class=" "><span class="stat-value rating">6.54</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/314/Show/France-Metz">17. Metz</a></td> <td class="shotsPerGame   sorted  ">9.9</td><td class="shotOnTargetPerGame   ">3.3</td><td class="dribbleWonPerGame   ">8.1</td><td class="foulGivenPerGame   ">11.2</td><td class=" "><span class="stat-value rating">6.52</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/146/Show/France-Lorient">18. Lorient</a></td> <td class="shotsPerGame   sorted  ">9.5</td><td class="shotOnTargetPerGame   ">3.7</td><td class="dribbleWonPerGame   ">10.1</td><td class="foulGivenPerGame   ">12.7</td><td class=" "><span class="stat-value rating">6.58</span></td></tr></tbody></table></div></div>
"""
soup = BeautifulSoup(html_content, 'html.parser')

# Extraction des données
data = []
for row in soup.find('tbody').find_all('tr'):
    cols = row.find_all('td')
    cols = [col.get_text(strip=True) for col in cols]
    data.append(cols)

# Créer un DataFrame pandas avec les données extraites
df = pd.DataFrame(data, columns=['Équipe', 'Tirs pm', 'Tirs CA pm', 'Dribbles pm', 'Fautes subies pm', 'Note'])

# Supprimer les colonnes "Note" et "Apps"
df.drop(columns=["Note"], inplace=True)

# Insérer une nouvelle colonne "Position"
df.insert(0, "Journée", "26")

# Supprimer la colonne "Position" si elle existe déjà
if "Position" in df.columns:
    df.drop(columns=["Position"], inplace=True)

# Insérer une nouvelle colonne "Position" à l'index 1
df.insert(1, "Position", range(1, len(df) + 1))

# Séparer la colonne "Équipe" pour obtenir seulement le nom de l'équipe
df["Équipe"] = df["Équipe"].str.split(". ", n=1).str[1]

# Remplacer "Paris Saint-Germain" par "Paris SG" dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Paris Saint-Germain", "Paris SG")

# Remplacer "Paris Saint-Germain" par "Paris SG" dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Clermont Foot", "Clermont F.")

# Enregistrer le DataFrame dans un fichier CSV
df.to_csv("team_stats_off.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_off.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_off_L1'

# # Supprimer la collection si elle existe
if collection_name in db.list_collection_names():
    db.drop_collection(collection_name)
    print("La collection existe. Elle a été supprimée.")

# # Recréer la collection
collection = db[collection_name]

df['Journée'] = df['Journée'].astype(int)


# # Convertir le DataFrame en dictionnaire pour l'insertion dans MongoDB
data_dict = df.to_dict(orient='records')

# # Insérer les données dans la collection MongoDB
collection.insert_many(data_dict)

print("Les données ont été insérées dans la collection MongoDB avec succès.")
