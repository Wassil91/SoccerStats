from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import csv

html_content = """
<div id="statistics-team-table-offensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsPerGame   desc  " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable shotOnTargetPerGame   " data-stat-name="shotOnTargetPerGame">Tirs CA pm</th><th class="global sortable dribbleWonPerGame   " data-stat-name="dribbleWonPerGame">Dribbles pm</th><th class="global sortable foulGivenPerGame   " data-stat-name="foulGivenPerGame">Fautes subies pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/276/Show/Italie-Napoli">1. Napoli</a></td> <td class="shotsPerGame   sorted  ">16.9</td><td class="shotOnTargetPerGame   ">5.4</td><td class="dribbleWonPerGame   ">8.4</td><td class="foulGivenPerGame   ">11.8</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/75/Show/Italie-Inter">2. Inter</a></td> <td class="shotsPerGame   sorted  ">15.4</td><td class="shotOnTargetPerGame   ">5.5</td><td class="dribbleWonPerGame   ">5.2</td><td class="foulGivenPerGame   ">10.4</td><td class=" "><span class="stat-value rating">6.88</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/87/Show/Italie-Juventus">3. Juventus</a></td> <td class="shotsPerGame   sorted  ">14.3</td><td class="shotOnTargetPerGame   ">4.1</td><td class="dribbleWonPerGame   ">7.1</td><td class="foulGivenPerGame   ">9.8</td><td class=" "><span class="stat-value rating">6.69</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/80/Show/Italie-AC-Milan">4. AC Milan</a></td> <td class="shotsPerGame   sorted  ">13.9</td><td class="shotOnTargetPerGame   ">5.4</td><td class="dribbleWonPerGame   ">9.1</td><td class="foulGivenPerGame   ">11</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/300/Show/Italie-Atalanta">5. Atalanta</a></td> <td class="shotsPerGame   sorted  ">13.9</td><td class="shotOnTargetPerGame   ">5.3</td><td class="dribbleWonPerGame   ">7.4</td><td class="foulGivenPerGame   ">9.5</td><td class=" "><span class="stat-value rating">6.74</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/73/Show/Italie-Fiorentina">6. Fiorentina</a></td> <td class="shotsPerGame   sorted  ">13.6</td><td class="shotOnTargetPerGame   ">4.6</td><td class="dribbleWonPerGame   ">7</td><td class="foulGivenPerGame   ">11.8</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/79/Show/Italie-Lecce">7. Lecce</a></td> <td class="shotsPerGame   sorted  ">13.2</td><td class="shotOnTargetPerGame   ">3.9</td><td class="dribbleWonPerGame   ">7.8</td><td class="foulGivenPerGame   ">13</td><td class=" "><span class="stat-value rating">6.46</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2889/Show/Italie-Sassuolo">8. Sassuolo</a></td> <td class="shotsPerGame   sorted  ">12.8</td><td class="shotOnTargetPerGame   ">4.2</td><td class="dribbleWonPerGame   ">7.3</td><td class="foulGivenPerGame   ">10.6</td><td class=" "><span class="stat-value rating">6.42</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/71/Show/Italie-Bologna">9. Bologna</a></td> <td class="shotsPerGame   sorted  ">12.8</td><td class="shotOnTargetPerGame   ">4.5</td><td class="dribbleWonPerGame   ">7.9</td><td class="foulGivenPerGame   ">11.7</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/86/Show/Italie-Udinese">10. Udinese</a></td> <td class="shotsPerGame   sorted  ">12.6</td><td class="shotOnTargetPerGame   ">4</td><td class="dribbleWonPerGame   ">9.4</td><td class="foulGivenPerGame   ">11.1</td><td class=" "><span class="stat-value rating">6.52</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/269/Show/Italie-Monza">11. Monza</a></td> <td class="shotsPerGame   sorted  ">12.3</td><td class="shotOnTargetPerGame   ">3.7</td><td class="dribbleWonPerGame   ">5.1</td><td class="foulGivenPerGame   ">10.5</td><td class=" "><span class="stat-value rating">6.55</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/78/Show/Italie-Cagliari">12. Cagliari</a></td> <td class="shotsPerGame   sorted  ">11.9</td><td class="shotOnTargetPerGame   ">3.5</td><td class="dribbleWonPerGame   ">6.2</td><td class="foulGivenPerGame   ">10.4</td><td class=" "><span class="stat-value rating">6.43</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/84/Show/Italie-Roma">13. Roma</a></td> <td class="shotsPerGame   sorted  ">11.9</td><td class="shotOnTargetPerGame   ">4</td><td class="dribbleWonPerGame   ">5.6</td><td class="foulGivenPerGame   ">11.1</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2732/Show/Italie-Frosinone">14. Frosinone</a></td> <td class="shotsPerGame   sorted  ">11.7</td><td class="shotOnTargetPerGame   ">4</td><td class="dribbleWonPerGame   ">8.1</td><td class="foulGivenPerGame   ">11.9</td><td class=" "><span class="stat-value rating">6.45</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/272/Show/Italie-Empoli">15. Empoli</a></td> <td class="shotsPerGame   sorted  ">11.6</td><td class="shotOnTargetPerGame   ">3.3</td><td class="dribbleWonPerGame   ">6.1</td><td class="foulGivenPerGame   ">13.3</td><td class=" "><span class="stat-value rating">6.46</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/77/Show/Italie-Lazio">16. Lazio</a></td> <td class="shotsPerGame   sorted  ">11.5</td><td class="shotOnTargetPerGame   ">3.6</td><td class="dribbleWonPerGame   ">7.2</td><td class="foulGivenPerGame   ">11.6</td><td class=" "><span class="stat-value rating">6.56</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/72/Show/Italie-Torino">17. Torino</a></td> <td class="shotsPerGame   sorted  ">11.5</td><td class="shotOnTargetPerGame   ">3.4</td><td class="dribbleWonPerGame   ">6.6</td><td class="foulGivenPerGame   ">10.5</td><td class=" "><span class="stat-value rating">6.58</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/76/Show/Italie-Verona">18. Verona</a></td> <td class="shotsPerGame   sorted  ">11.3</td><td class="shotOnTargetPerGame   ">3.8</td><td class="dribbleWonPerGame   ">7</td><td class="foulGivenPerGame   ">12.1</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/278/Show/Italie-Genoa">19. Genoa</a></td> <td class="shotsPerGame   sorted  ">10.8</td><td class="shotOnTargetPerGame   ">3.4</td><td class="dribbleWonPerGame   ">5</td><td class="foulGivenPerGame   ">10.8</td><td class=" "><span class="stat-value rating">6.55</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/143/Show/Italie-Salernitana">20. Salernitana</a></td> <td class="shotsPerGame   sorted  ">10.8</td><td class="shotOnTargetPerGame   ">3.3</td><td class="dribbleWonPerGame   ">5.8</td><td class="foulGivenPerGame   ">12.5</td><td class=" "><span class="stat-value rating">6.36</span></td></tr></tbody></table></div></div>
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
collection_name = 'Stats_All_Team_off_serieA'

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
