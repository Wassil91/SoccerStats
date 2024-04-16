from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import csv

html_content = """
<div id="statistics-team-table-offensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsPerGame   desc  " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable shotOnTargetPerGame   " data-stat-name="shotOnTargetPerGame">Tirs CA pm</th><th class="global sortable dribbleWonPerGame   " data-stat-name="dribbleWonPerGame">Dribbles pm</th><th class="global sortable foulGivenPerGame   " data-stat-name="foulGivenPerGame">Fautes subies pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/52/Show/Espagne-Real-Madrid">1. Real Madrid</a></td> <td class="shotsPerGame   sorted  ">17.3</td><td class="shotOnTargetPerGame   ">6.8</td><td class="dribbleWonPerGame   ">12</td><td class="foulGivenPerGame   ">12.5</td><td class=" "><span class="stat-value rating">6.90</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/65/Show/Espagne-Barcelona">2. Barcelona</a></td> <td class="shotsPerGame   sorted  ">13.6</td><td class="shotOnTargetPerGame   ">4.9</td><td class="dribbleWonPerGame   ">12.5</td><td class="foulGivenPerGame   ">12.9</td><td class=" "><span class="stat-value rating">6.77</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/54/Show/Espagne-Real-Betis">3. Real Betis</a></td> <td class="shotsPerGame   sorted  ">13.6</td><td class="shotOnTargetPerGame   ">5</td><td class="dribbleWonPerGame   ">9.8</td><td class="foulGivenPerGame   ">12.4</td><td class=" "><span class="stat-value rating">6.72</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/64/Show/Espagne-Rayo-Vallecano">4. Rayo Vallecano</a></td> <td class="shotsPerGame   sorted  ">13.3</td><td class="shotOnTargetPerGame   ">3.7</td><td class="dribbleWonPerGame   ">9.4</td><td class="foulGivenPerGame   ">13.4</td><td class=" "><span class="stat-value rating">6.56</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/53/Show/Espagne-Athletic-Club">5. Athletic Club</a></td> <td class="shotsPerGame   sorted  ">12.3</td><td class="shotOnTargetPerGame   ">4.3</td><td class="dribbleWonPerGame   ">7.8</td><td class="foulGivenPerGame   ">12.7</td><td class=" "><span class="stat-value rating">6.59</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/63/Show/Espagne-Atletico-Madrid">6. Atletico Madrid</a></td> <td class="shotsPerGame   sorted  ">12.2</td><td class="shotOnTargetPerGame   ">4.2</td><td class="dribbleWonPerGame   ">10.1</td><td class="foulGivenPerGame   ">13.2</td><td class=" "><span class="stat-value rating">6.68</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/839/Show/Espagne-Villarreal">7. Villarreal</a></td> <td class="shotsPerGame   sorted  ">12</td><td class="shotOnTargetPerGame   ">4.6</td><td class="dribbleWonPerGame   ">11.9</td><td class="foulGivenPerGame   ">12.1</td><td class=" "><span class="stat-value rating">6.72</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/51/Show/Espagne-Mallorca">8. Mallorca</a></td> <td class="shotsPerGame   sorted  ">11.8</td><td class="shotOnTargetPerGame   ">3.6</td><td class="dribbleWonPerGame   ">9.7</td><td class="foulGivenPerGame   ">14.1</td><td class=" "><span class="stat-value rating">6.48</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/832/Show/Espagne-Levante">9. Levante</a></td> <td class="shotsPerGame   sorted  ">11.7</td><td class="shotOnTargetPerGame   ">4.2</td><td class="dribbleWonPerGame   ">9.2</td><td class="foulGivenPerGame   ">10.6</td><td class=" "><span class="stat-value rating">6.46</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/68/Show/Espagne-Real-Sociedad">10. Real Sociedad</a></td> <td class="shotsPerGame   sorted  ">11.6</td><td class="shotOnTargetPerGame   ">4.1</td><td class="dribbleWonPerGame   ">9.5</td><td class="foulGivenPerGame   ">12.2</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/67/Show/Espagne-Sevilla">11. Sevilla</a></td> <td class="shotsPerGame   sorted  ">11.4</td><td class="shotOnTargetPerGame   ">3.8</td><td class="dribbleWonPerGame   ">8</td><td class="foulGivenPerGame   ">12.2</td><td class=" "><span class="stat-value rating">6.65</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/131/Show/Espagne-Osasuna">12. Osasuna</a></td> <td class="shotsPerGame   sorted  ">11.4</td><td class="shotOnTargetPerGame   ">3.3</td><td class="dribbleWonPerGame   ">6.9</td><td class="foulGivenPerGame   ">10.3</td><td class=" "><span class="stat-value rating">6.55</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/925/Show/Espagne-Granada">13. Granada</a></td> <td class="shotsPerGame   sorted  ">11.2</td><td class="shotOnTargetPerGame   ">3.6</td><td class="dribbleWonPerGame   ">7.1</td><td class="foulGivenPerGame   ">12.3</td><td class=" "><span class="stat-value rating">6.52</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1354/Show/Espagne-Cadiz">14. Cadiz</a></td> <td class="shotsPerGame   sorted  ">10.8</td><td class="shotOnTargetPerGame   ">3.4</td><td class="dribbleWonPerGame   ">7.4</td><td class="foulGivenPerGame   ">11.8</td><td class=" "><span class="stat-value rating">6.53</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/62/Show/Espagne-Celta-Vigo">15. Celta Vigo</a></td> <td class="shotsPerGame   sorted  ">10.8</td><td class="shotOnTargetPerGame   ">3.8</td><td class="dribbleWonPerGame   ">9.1</td><td class="foulGivenPerGame   ">12.2</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/819/Show/Espagne-Getafe">16. Getafe</a></td> <td class="shotsPerGame   sorted  ">10.8</td><td class="shotOnTargetPerGame   ">3.1</td><td class="dribbleWonPerGame   ">6.4</td><td class="foulGivenPerGame   ">12.7</td><td class=" "><span class="stat-value rating">6.55</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/70/Show/Espagne-Espanyol">17. Espanyol</a></td> <td class="shotsPerGame   sorted  ">10.7</td><td class="shotOnTargetPerGame   ">3.5</td><td class="dribbleWonPerGame   ">9.8</td><td class="foulGivenPerGame   ">13.6</td><td class=" "><span class="stat-value rating">6.52</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/55/Show/Espagne-Valencia">18. Valencia</a></td> <td class="shotsPerGame   sorted  ">10.6</td><td class="shotOnTargetPerGame   ">3.8</td><td class="dribbleWonPerGame   ">9.5</td><td class="foulGivenPerGame   ">15.4</td><td class=" "><span class="stat-value rating">6.54</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/60/Show/Espagne-Deportivo-Alaves">19. Deportivo Alaves</a></td> <td class="shotsPerGame   sorted  ">10</td><td class="shotOnTargetPerGame   ">3.1</td><td class="dribbleWonPerGame   ">7.1</td><td class="foulGivenPerGame   ">12</td><td class=" "><span class="stat-value rating">6.46</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/833/Show/Espagne-Elche">20. Elche</a></td> <td class="shotsPerGame   sorted  ">8.8</td><td class="shotOnTargetPerGame   ">2.8</td><td class="dribbleWonPerGame   ">8.9</td><td class="foulGivenPerGame   ">12.4</td><td class=" "><span class="stat-value rating">6.51</span></td></tr></tbody></table></div></div>
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
collection_name = 'Stats_All_Team_off_21-22_Liga'

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
