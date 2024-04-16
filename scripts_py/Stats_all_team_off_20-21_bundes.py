from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import csv

html_content = """
<div id="statistics-team-table-offensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsPerGame   desc  " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable shotOnTargetPerGame   " data-stat-name="shotOnTargetPerGame">Tirs CA pm</th><th class="global sortable dribbleWonPerGame   " data-stat-name="dribbleWonPerGame">Dribbles pm</th><th class="global sortable foulGivenPerGame   " data-stat-name="foulGivenPerGame">Fautes subies pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/37/Show/Allemagne-Bayern-Munich">1. Bayern Munich</a></td> <td class="shotsPerGame   sorted  ">17.1</td><td class="shotOnTargetPerGame   ">6.9</td><td class="dribbleWonPerGame   ">12.9</td><td class="foulGivenPerGame   ">11</td><td class=" "><span class="stat-value rating">6.95</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/7614/Show/Allemagne-RB-Leipzig">2. RB Leipzig</a></td> <td class="shotsPerGame   sorted  ">16</td><td class="shotOnTargetPerGame   ">6</td><td class="dribbleWonPerGame   ">9.3</td><td class="foulGivenPerGame   ">11.9</td><td class=" "><span class="stat-value rating">6.78</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/44/Show/Allemagne-Borussia-Dortmund">3. Borussia Dortmund</a></td> <td class="shotsPerGame   sorted  ">14.6</td><td class="shotOnTargetPerGame   ">5.7</td><td class="dribbleWonPerGame   ">11.8</td><td class="foulGivenPerGame   ">11</td><td class=" "><span class="stat-value rating">6.84</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/33/Show/Allemagne-Wolfsburg">4. Wolfsburg</a></td> <td class="shotsPerGame   sorted  ">14.1</td><td class="shotOnTargetPerGame   ">4.9</td><td class="dribbleWonPerGame   ">8.7</td><td class="foulGivenPerGame   ">11.1</td><td class=" "><span class="stat-value rating">6.80</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/41/Show/Allemagne-VfB-Stuttgart">5. VfB Stuttgart</a></td> <td class="shotsPerGame   sorted  ">13.4</td><td class="shotOnTargetPerGame   ">4.7</td><td class="dribbleWonPerGame   ">12.1</td><td class="foulGivenPerGame   ">12.8</td><td class=" "><span class="stat-value rating">6.68</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/134/Show/Allemagne-Borussia-M-Gladbach">6. Borussia M.Gladbach</a></td> <td class="shotsPerGame   sorted  ">13.4</td><td class="shotOnTargetPerGame   ">5.2</td><td class="dribbleWonPerGame   ">9.4</td><td class="foulGivenPerGame   ">12.1</td><td class=" "><span class="stat-value rating">6.70</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/45/Show/Allemagne-Eintracht-Frankfurt">7. Eintracht Frankfurt</a></td> <td class="shotsPerGame   sorted  ">13.2</td><td class="shotOnTargetPerGame   ">5.1</td><td class="dribbleWonPerGame   ">8.1</td><td class="foulGivenPerGame   ">10.8</td><td class=" "><span class="stat-value rating">6.73</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/36/Show/Allemagne-Bayer-Leverkusen">8. Bayer Leverkusen</a></td> <td class="shotsPerGame   sorted  ">13</td><td class="shotOnTargetPerGame   ">4.9</td><td class="dribbleWonPerGame   ">12.5</td><td class="foulGivenPerGame   ">9.9</td><td class=" "><span class="stat-value rating">6.73</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1211/Show/Allemagne-Hoffenheim">9. Hoffenheim</a></td> <td class="shotsPerGame   sorted  ">12.6</td><td class="shotOnTargetPerGame   ">4.6</td><td class="dribbleWonPerGame   ">8.2</td><td class="foulGivenPerGame   ">12</td><td class=" "><span class="stat-value rating">6.63</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/796/Show/Allemagne-Union-Berlin">10. Union Berlin</a></td> <td class="shotsPerGame   sorted  ">11.7</td><td class="shotOnTargetPerGame   ">4.2</td><td class="dribbleWonPerGame   ">6.1</td><td class="foulGivenPerGame   ">11</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/50/Show/Allemagne-Freiburg">11. Freiburg</a></td> <td class="shotsPerGame   sorted  ">11.4</td><td class="shotOnTargetPerGame   ">4.4</td><td class="dribbleWonPerGame   ">6.8</td><td class="foulGivenPerGame   ">11.8</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/47/Show/Allemagne-Hertha-Berlin">12. Hertha Berlin</a></td> <td class="shotsPerGame   sorted  ">11.3</td><td class="shotOnTargetPerGame   ">4.4</td><td class="dribbleWonPerGame   ">9.7</td><td class="foulGivenPerGame   ">12.6</td><td class=" "><span class="stat-value rating">6.58</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/219/Show/Allemagne-Mainz-05">13. Mainz 05</a></td> <td class="shotsPerGame   sorted  ">11.1</td><td class="shotOnTargetPerGame   ">4.1</td><td class="dribbleWonPerGame   ">7.9</td><td class="foulGivenPerGame   ">11.1</td><td class=" "><span class="stat-value rating">6.57</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/42/Show/Allemagne-Werder-Bremen">14. Werder Bremen</a></td> <td class="shotsPerGame   sorted  ">10.6</td><td class="shotOnTargetPerGame   ">3.7</td><td class="dribbleWonPerGame   ">5.5</td><td class="foulGivenPerGame   ">13.7</td><td class=" "><span class="stat-value rating">6.52</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/282/Show/Allemagne-FC-Koln">15. FC Koln</a></td> <td class="shotsPerGame   sorted  ">10.6</td><td class="shotOnTargetPerGame   ">3.2</td><td class="dribbleWonPerGame   ">7</td><td class="foulGivenPerGame   ">12.6</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1730/Show/Allemagne-Augsburg">16. Augsburg</a></td> <td class="shotsPerGame   sorted  ">9.9</td><td class="shotOnTargetPerGame   ">3.3</td><td class="dribbleWonPerGame   ">8.3</td><td class="foulGivenPerGame   ">11.9</td><td class=" "><span class="stat-value rating">6.56</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/40/Show/Allemagne-Arminia-Bielefeld">17. Arminia Bielefeld</a></td> <td class="shotsPerGame   sorted  ">9.8</td><td class="shotOnTargetPerGame   ">3</td><td class="dribbleWonPerGame   ">6.9</td><td class="foulGivenPerGame   ">12.3</td><td class=" "><span class="stat-value rating">6.53</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/39/Show/Allemagne-Schalke-04">18. Schalke 04</a></td> <td class="shotsPerGame   sorted  ">8.9</td><td class="shotOnTargetPerGame   ">2.5</td><td class="dribbleWonPerGame   ">9.8</td><td class="foulGivenPerGame   ">11.9</td><td class=" "><span class="stat-value rating">6.41</span></td></tr></tbody></table></div></div>
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
df.to_csv("team_stats_off2020.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_off2020.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_off_20-21_bundes'

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
