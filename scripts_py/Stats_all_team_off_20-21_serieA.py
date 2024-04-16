from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import csv

html_content = """
<div id="statistics-team-table-offensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsPerGame   desc  " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable shotOnTargetPerGame   " data-stat-name="shotOnTargetPerGame">Tirs CA pm</th><th class="global sortable dribbleWonPerGame   " data-stat-name="dribbleWonPerGame">Dribbles pm</th><th class="global sortable foulGivenPerGame   " data-stat-name="foulGivenPerGame">Fautes subies pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/276/Show/Italie-Napoli">1. Napoli</a></td> <td class="shotsPerGame   sorted  ">17</td><td class="shotOnTargetPerGame   ">5.9</td><td class="dribbleWonPerGame   ">10.2</td><td class="foulGivenPerGame   ">14.4</td><td class=" "><span class="stat-value rating">6.81</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/300/Show/Italie-Atalanta">2. Atalanta</a></td> <td class="shotsPerGame   sorted  ">16.3</td><td class="shotOnTargetPerGame   ">6.1</td><td class="dribbleWonPerGame   ">10.4</td><td class="foulGivenPerGame   ">12.4</td><td class=" "><span class="stat-value rating">6.84</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/87/Show/Italie-Juventus">3. Juventus</a></td> <td class="shotsPerGame   sorted  ">15.7</td><td class="shotOnTargetPerGame   ">5.7</td><td class="dribbleWonPerGame   ">11.9</td><td class="foulGivenPerGame   ">12.4</td><td class=" "><span class="stat-value rating">6.85</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/80/Show/Italie-AC-Milan">4. AC Milan</a></td> <td class="shotsPerGame   sorted  ">14.7</td><td class="shotOnTargetPerGame   ">5.4</td><td class="dribbleWonPerGame   ">10.9</td><td class="foulGivenPerGame   ">12.5</td><td class=" "><span class="stat-value rating">6.82</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/75/Show/Italie-Inter">5. Inter</a></td> <td class="shotsPerGame   sorted  ">14.5</td><td class="shotOnTargetPerGame   ">5.4</td><td class="dribbleWonPerGame   ">6.8</td><td class="foulGivenPerGame   ">11.2</td><td class=" "><span class="stat-value rating">6.81</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/84/Show/Italie-Roma">6. Roma</a></td> <td class="shotsPerGame   sorted  ">14.3</td><td class="shotOnTargetPerGame   ">5.3</td><td class="dribbleWonPerGame   ">10.8</td><td class="foulGivenPerGame   ">11.2</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2889/Show/Italie-Sassuolo">7. Sassuolo</a></td> <td class="shotsPerGame   sorted  ">13.9</td><td class="shotOnTargetPerGame   ">4.7</td><td class="dribbleWonPerGame   ">11.3</td><td class="foulGivenPerGame   ">12.5</td><td class=" "><span class="stat-value rating">6.67</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/77/Show/Italie-Lazio">8. Lazio</a></td> <td class="shotsPerGame   sorted  ">13.8</td><td class="shotOnTargetPerGame   ">4.9</td><td class="dribbleWonPerGame   ">7.8</td><td class="foulGivenPerGame   ">11.2</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/71/Show/Italie-Bologna">9. Bologna</a></td> <td class="shotsPerGame   sorted  ">13.1</td><td class="shotOnTargetPerGame   ">4.7</td><td class="dribbleWonPerGame   ">9.1</td><td class="foulGivenPerGame   ">12.5</td><td class=" "><span class="stat-value rating">6.56</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/72/Show/Italie-Torino">10. Torino</a></td> <td class="shotsPerGame   sorted  ">12.2</td><td class="shotOnTargetPerGame   ">4</td><td class="dribbleWonPerGame   ">10.2</td><td class="foulGivenPerGame   ">15.8</td><td class=" "><span class="stat-value rating">6.59</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/78/Show/Italie-Cagliari">11. Cagliari</a></td> <td class="shotsPerGame   sorted  ">11.4</td><td class="shotOnTargetPerGame   ">3.4</td><td class="dribbleWonPerGame   ">8.8</td><td class="foulGivenPerGame   ">12.3</td><td class=" "><span class="stat-value rating">6.58</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/271/Show/Italie-Sampdoria">12. Sampdoria</a></td> <td class="shotsPerGame   sorted  ">11.3</td><td class="shotOnTargetPerGame   ">3.7</td><td class="dribbleWonPerGame   ">7.7</td><td class="foulGivenPerGame   ">12.8</td><td class=" "><span class="stat-value rating">6.63</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1503/Show/Italie-Benevento">13. Benevento</a></td> <td class="shotsPerGame   sorted  ">11</td><td class="shotOnTargetPerGame   ">3.5</td><td class="dribbleWonPerGame   ">7.2</td><td class="foulGivenPerGame   ">15.6</td><td class=" "><span class="stat-value rating">6.43</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/86/Show/Italie-Udinese">14. Udinese</a></td> <td class="shotsPerGame   sorted  ">10.9</td><td class="shotOnTargetPerGame   ">3.4</td><td class="dribbleWonPerGame   ">9.9</td><td class="foulGivenPerGame   ">13.7</td><td class=" "><span class="stat-value rating">6.55</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/76/Show/Italie-Verona">15. Verona</a></td> <td class="shotsPerGame   sorted  ">10.6</td><td class="shotOnTargetPerGame   ">3.3</td><td class="dribbleWonPerGame   ">8</td><td class="foulGivenPerGame   ">13.7</td><td class=" "><span class="stat-value rating">6.59</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/24341/Show/Italie-Parma-Calcio-1913">16. Parma Calcio 1913</a></td> <td class="shotsPerGame   sorted  ">10.4</td><td class="shotOnTargetPerGame   ">3.4</td><td class="dribbleWonPerGame   ">9.5</td><td class="foulGivenPerGame   ">12.7</td><td class=" "><span class="stat-value rating">6.49</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1501/Show/Italie-Spezia">17. Spezia</a></td> <td class="shotsPerGame   sorted  ">10.2</td><td class="shotOnTargetPerGame   ">3.6</td><td class="dribbleWonPerGame   ">8.5</td><td class="foulGivenPerGame   ">13.9</td><td class=" "><span class="stat-value rating">6.50</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/73/Show/Italie-Fiorentina">18. Fiorentina</a></td> <td class="shotsPerGame   sorted  ">9.8</td><td class="shotOnTargetPerGame   ">3.3</td><td class="dribbleWonPerGame   ">8.6</td><td class="foulGivenPerGame   ">13.8</td><td class=" "><span class="stat-value rating">6.52</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/779/Show/Italie-Crotone">19. Crotone</a></td> <td class="shotsPerGame   sorted  ">9.5</td><td class="shotOnTargetPerGame   ">3.2</td><td class="dribbleWonPerGame   ">10.1</td><td class="foulGivenPerGame   ">12.9</td><td class=" "><span class="stat-value rating">6.43</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/278/Show/Italie-Genoa">20. Genoa</a></td> <td class="shotsPerGame   sorted  ">9</td><td class="shotOnTargetPerGame   ">3.3</td><td class="dribbleWonPerGame   ">7.7</td><td class="foulGivenPerGame   ">12.3</td><td class=" "><span class="stat-value rating">6.54</span></td></tr></tbody></table></div></div>
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
collection_name = 'Stats_All_Team_off_20-21_serieA'

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
