from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import csv

html_content = """
<div id="statistics-team-table-offensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsPerGame   desc  " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable shotOnTargetPerGame   " data-stat-name="shotOnTargetPerGame">Tirs CA pm</th><th class="global sortable dribbleWonPerGame   " data-stat-name="dribbleWonPerGame">Dribbles pm</th><th class="global sortable foulGivenPerGame   " data-stat-name="foulGivenPerGame">Fautes subies pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/52/Show/Espagne-Real-Madrid">1. Real Madrid</a></td> <td class="shotsPerGame   sorted  ">16.4</td><td class="shotOnTargetPerGame   ">6.4</td><td class="dribbleWonPerGame   ">10.6</td><td class="foulGivenPerGame   ">14.1</td><td class=" "><span class="stat-value rating">6.89</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/65/Show/Espagne-Barcelona">2. Barcelona</a></td> <td class="shotsPerGame   sorted  ">15.7</td><td class="shotOnTargetPerGame   ">6.1</td><td class="dribbleWonPerGame   ">10.4</td><td class="foulGivenPerGame   ">13</td><td class=" "><span class="stat-value rating">6.83</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/67/Show/Espagne-Sevilla">3. Sevilla</a></td> <td class="shotsPerGame   sorted  ">13.5</td><td class="shotOnTargetPerGame   ">4.5</td><td class="dribbleWonPerGame   ">8.1</td><td class="foulGivenPerGame   ">12.2</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/54/Show/Espagne-Real-Betis">4. Real Betis</a></td> <td class="shotsPerGame   sorted  ">13.2</td><td class="shotOnTargetPerGame   ">3.9</td><td class="dribbleWonPerGame   ">10.4</td><td class="foulGivenPerGame   ">13.3</td><td class=" "><span class="stat-value rating">6.66</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/53/Show/Espagne-Athletic-Club">5. Athletic Club</a></td> <td class="shotsPerGame   sorted  ">12.7</td><td class="shotOnTargetPerGame   ">4.8</td><td class="dribbleWonPerGame   ">9.1</td><td class="foulGivenPerGame   ">11.6</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/68/Show/Espagne-Real-Sociedad">6. Real Sociedad</a></td> <td class="shotsPerGame   sorted  ">12.7</td><td class="shotOnTargetPerGame   ">4.3</td><td class="dribbleWonPerGame   ">8.8</td><td class="foulGivenPerGame   ">11.9</td><td class=" "><span class="stat-value rating">6.66</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2783/Show/Espagne-Girona">7. Girona</a></td> <td class="shotsPerGame   sorted  ">12.7</td><td class="shotOnTargetPerGame   ">4.9</td><td class="dribbleWonPerGame   ">9.2</td><td class="foulGivenPerGame   ">12.5</td><td class=" "><span class="stat-value rating">6.74</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/62/Show/Espagne-Celta-Vigo">8. Celta Vigo</a></td> <td class="shotsPerGame   sorted  ">12.6</td><td class="shotOnTargetPerGame   ">4.1</td><td class="dribbleWonPerGame   ">7.9</td><td class="foulGivenPerGame   ">13.3</td><td class=" "><span class="stat-value rating">6.57</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/63/Show/Espagne-Atletico-Madrid">9. Atletico Madrid</a></td> <td class="shotsPerGame   sorted  ">12.6</td><td class="shotOnTargetPerGame   ">5.7</td><td class="dribbleWonPerGame   ">8.7</td><td class="foulGivenPerGame   ">11.3</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/60/Show/Espagne-Deportivo-Alaves">10. Deportivo Alaves</a></td> <td class="shotsPerGame   sorted  ">12.4</td><td class="shotOnTargetPerGame   ">3.6</td><td class="dribbleWonPerGame   ">8.6</td><td class="foulGivenPerGame   ">11.6</td><td class=" "><span class="stat-value rating">6.50</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1799/Show/Espagne-Almeria">11. Almeria</a></td> <td class="shotsPerGame   sorted  ">12</td><td class="shotOnTargetPerGame   ">4</td><td class="dribbleWonPerGame   ">7.7</td><td class="foulGivenPerGame   ">12.5</td><td class=" "><span class="stat-value rating">6.45</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/64/Show/Espagne-Rayo-Vallecano">12. Rayo Vallecano</a></td> <td class="shotsPerGame   sorted  ">12</td><td class="shotOnTargetPerGame   ">4.3</td><td class="dribbleWonPerGame   ">7.3</td><td class="foulGivenPerGame   ">13.7</td><td class=" "><span class="stat-value rating">6.47</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/839/Show/Espagne-Villarreal">13. Villarreal</a></td> <td class="shotsPerGame   sorted  ">11.8</td><td class="shotOnTargetPerGame   ">4.6</td><td class="dribbleWonPerGame   ">9.2</td><td class="foulGivenPerGame   ">12.4</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/819/Show/Espagne-Getafe">14. Getafe</a></td> <td class="shotsPerGame   sorted  ">11.6</td><td class="shotOnTargetPerGame   ">4.1</td><td class="dribbleWonPerGame   ">5.8</td><td class="foulGivenPerGame   ">10.9</td><td class=" "><span class="stat-value rating">6.54</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/131/Show/Espagne-Osasuna">15. Osasuna</a></td> <td class="shotsPerGame   sorted  ">11.3</td><td class="shotOnTargetPerGame   ">3.4</td><td class="dribbleWonPerGame   ">6</td><td class="foulGivenPerGame   ">10.4</td><td class=" "><span class="stat-value rating">6.50</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/51/Show/Espagne-Mallorca">16. Mallorca</a></td> <td class="shotsPerGame   sorted  ">11.1</td><td class="shotOnTargetPerGame   ">3.6</td><td class="dribbleWonPerGame   ">6.9</td><td class="foulGivenPerGame   ">11.2</td><td class=" "><span class="stat-value rating">6.55</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/925/Show/Espagne-Granada">17. Granada</a></td> <td class="shotsPerGame   sorted  ">10.8</td><td class="shotOnTargetPerGame   ">3.8</td><td class="dribbleWonPerGame   ">8.6</td><td class="foulGivenPerGame   ">14.2</td><td class=" "><span class="stat-value rating">6.40</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/838/Show/Espagne-Las-Palmas">18. Las Palmas</a></td> <td class="shotsPerGame   sorted  ">10.7</td><td class="shotOnTargetPerGame   ">3.4</td><td class="dribbleWonPerGame   ">8.2</td><td class="foulGivenPerGame   ">13.6</td><td class=" "><span class="stat-value rating">6.57</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1354/Show/Espagne-Cadiz">19. Cadiz</a></td> <td class="shotsPerGame   sorted  ">10</td><td class="shotOnTargetPerGame   ">3</td><td class="dribbleWonPerGame   ">5.8</td><td class="foulGivenPerGame   ">13.6</td><td class=" "><span class="stat-value rating">6.45</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/55/Show/Espagne-Valencia">20. Valencia</a></td> <td class="shotsPerGame   sorted  ">9.8</td><td class="shotOnTargetPerGame   ">3.5</td><td class="dribbleWonPerGame   ">7.4</td><td class="foulGivenPerGame   ">12.2</td><td class=" "><span class="stat-value rating">6.56</span></td></tr></tbody></table></div></div>
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
df.insert(0, "Journée", "29")

# Supprimer la colonne "Position" si elle existe déjà
if "Position" in df.columns:
    df.drop(columns=["Position"], inplace=True)

# Insérer une nouvelle colonne "Position" à l'index 1
df.insert(1, "Position", range(1, len(df) + 1))

# Séparer la colonne "Équipe" pour obtenir seulement le nom de l'équipe
df["Équipe"] = df["Équipe"].str.split(". ", n=1).str[1]


# Enregistrer le DataFrame dans un fichier CSV
df.to_csv("team_stats_off.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_off.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_off_Liga'

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
