from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import csv

html_content = """
<div id="statistics-team-table-offensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsPerGame   desc  " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable shotOnTargetPerGame   " data-stat-name="shotOnTargetPerGame">Tirs CA pm</th><th class="global sortable dribbleWonPerGame   " data-stat-name="dribbleWonPerGame">Dribbles pm</th><th class="global sortable foulGivenPerGame   " data-stat-name="foulGivenPerGame">Fautes subies pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/65/Show/Espagne-Barcelona">1. Barcelona</a></td> <td class="shotsPerGame   sorted  ">15.3</td><td class="shotOnTargetPerGame   ">6.4</td><td class="dribbleWonPerGame   ">13.5</td><td class="foulGivenPerGame   ">13.8</td><td class=" "><span class="stat-value rating">6.87</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/52/Show/Espagne-Real-Madrid">2. Real Madrid</a></td> <td class="shotsPerGame   sorted  ">14.4</td><td class="shotOnTargetPerGame   ">4.6</td><td class="dribbleWonPerGame   ">11.3</td><td class="foulGivenPerGame   ">12.5</td><td class=" "><span class="stat-value rating">6.86</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/67/Show/Espagne-Sevilla">3. Sevilla</a></td> <td class="shotsPerGame   sorted  ">12.1</td><td class="shotOnTargetPerGame   ">3.8</td><td class="dribbleWonPerGame   ">10.3</td><td class="foulGivenPerGame   ">12.8</td><td class=" "><span class="stat-value rating">6.70</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/63/Show/Espagne-Atletico-Madrid">4. Atletico Madrid</a></td> <td class="shotsPerGame   sorted  ">12.1</td><td class="shotOnTargetPerGame   ">4.9</td><td class="dribbleWonPerGame   ">9.8</td><td class="foulGivenPerGame   ">11.7</td><td class=" "><span class="stat-value rating">6.84</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/824/Show/Espagne-Eibar">5. Eibar</a></td> <td class="shotsPerGame   sorted  ">11.9</td><td class="shotOnTargetPerGame   ">3.6</td><td class="dribbleWonPerGame   ">8.4</td><td class="foulGivenPerGame   ">10.2</td><td class=" "><span class="stat-value rating">6.53</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/54/Show/Espagne-Real-Betis">6. Real Betis</a></td> <td class="shotsPerGame   sorted  ">11.7</td><td class="shotOnTargetPerGame   ">4.2</td><td class="dribbleWonPerGame   ">11</td><td class="foulGivenPerGame   ">12.7</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/68/Show/Espagne-Real-Sociedad">7. Real Sociedad</a></td> <td class="shotsPerGame   sorted  ">11.3</td><td class="shotOnTargetPerGame   ">4.1</td><td class="dribbleWonPerGame   ">8.7</td><td class="foulGivenPerGame   ">12.1</td><td class=" "><span class="stat-value rating">6.65</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/5006/Show/Espagne-SD-Huesca">8. SD Huesca</a></td> <td class="shotsPerGame   sorted  ">10.7</td><td class="shotOnTargetPerGame   ">3.6</td><td class="dribbleWonPerGame   ">11.2</td><td class="foulGivenPerGame   ">13.6</td><td class=" "><span class="stat-value rating">6.55</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/839/Show/Espagne-Villarreal">9. Villarreal</a></td> <td class="shotsPerGame   sorted  ">10.7</td><td class="shotOnTargetPerGame   ">4.3</td><td class="dribbleWonPerGame   ">11.5</td><td class="foulGivenPerGame   ">12.2</td><td class=" "><span class="stat-value rating">6.66</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/53/Show/Espagne-Athletic-Club">10. Athletic Club</a></td> <td class="shotsPerGame   sorted  ">10.6</td><td class="shotOnTargetPerGame   ">3.6</td><td class="dribbleWonPerGame   ">8</td><td class="foulGivenPerGame   ">12.6</td><td class=" "><span class="stat-value rating">6.54</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/55/Show/Espagne-Valencia">11. Valencia</a></td> <td class="shotsPerGame   sorted  ">10.3</td><td class="shotOnTargetPerGame   ">3.7</td><td class="dribbleWonPerGame   ">9</td><td class="foulGivenPerGame   ">13.8</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/832/Show/Espagne-Levante">12. Levante</a></td> <td class="shotsPerGame   sorted  ">10.1</td><td class="shotOnTargetPerGame   ">3.4</td><td class="dribbleWonPerGame   ">9.6</td><td class="foulGivenPerGame   ">10.4</td><td class=" "><span class="stat-value rating">6.50</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/131/Show/Espagne-Osasuna">13. Osasuna</a></td> <td class="shotsPerGame   sorted  ">9.8</td><td class="shotOnTargetPerGame   ">3.2</td><td class="dribbleWonPerGame   ">6.4</td><td class="foulGivenPerGame   ">10.7</td><td class=" "><span class="stat-value rating">6.54</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/58/Show/Espagne-Real-Valladolid">14. Real Valladolid</a></td> <td class="shotsPerGame   sorted  ">9.7</td><td class="shotOnTargetPerGame   ">3.1</td><td class="dribbleWonPerGame   ">7.7</td><td class="foulGivenPerGame   ">13.4</td><td class=" "><span class="stat-value rating">6.46</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/819/Show/Espagne-Getafe">15. Getafe</a></td> <td class="shotsPerGame   sorted  ">9.5</td><td class="shotOnTargetPerGame   ">2.8</td><td class="dribbleWonPerGame   ">6.9</td><td class="foulGivenPerGame   ">13.7</td><td class=" "><span class="stat-value rating">6.49</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/925/Show/Espagne-Granada">16. Granada</a></td> <td class="shotsPerGame   sorted  ">9.4</td><td class="shotOnTargetPerGame   ">3.3</td><td class="dribbleWonPerGame   ">8.2</td><td class="foulGivenPerGame   ">12.3</td><td class=" "><span class="stat-value rating">6.47</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/62/Show/Espagne-Celta-Vigo">17. Celta Vigo</a></td> <td class="shotsPerGame   sorted  ">9.4</td><td class="shotOnTargetPerGame   ">3.7</td><td class="dribbleWonPerGame   ">7.3</td><td class="foulGivenPerGame   ">12.9</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/60/Show/Espagne-Deportivo-Alaves">18. Deportivo Alaves</a></td> <td class="shotsPerGame   sorted  ">9.1</td><td class="shotOnTargetPerGame   ">2.7</td><td class="dribbleWonPerGame   ">6.7</td><td class="foulGivenPerGame   ">12.8</td><td class=" "><span class="stat-value rating">6.50</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1354/Show/Espagne-Cadiz">19. Cadiz</a></td> <td class="shotsPerGame   sorted  ">8</td><td class="shotOnTargetPerGame   ">2.7</td><td class="dribbleWonPerGame   ">8.4</td><td class="foulGivenPerGame   ">12.7</td><td class=" "><span class="stat-value rating">6.47</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/833/Show/Espagne-Elche">20. Elche</a></td> <td class="shotsPerGame   sorted  ">7.1</td><td class="shotOnTargetPerGame   ">2.4</td><td class="dribbleWonPerGame   ">9.6</td><td class="foulGivenPerGame   ">12</td><td class=" "><span class="stat-value rating">6.48</span></td></tr></tbody></table></div></div>

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
df.insert(0, "Journée", "38")

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
collection_name = 'Stats_All_Team_off_20-21_Liga'

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
