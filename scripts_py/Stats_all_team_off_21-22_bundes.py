from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import csv

html_content = """
<div id="statistics-team-table-offensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsPerGame   desc  " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable shotOnTargetPerGame   " data-stat-name="shotOnTargetPerGame">Tirs CA pm</th><th class="global sortable dribbleWonPerGame   " data-stat-name="dribbleWonPerGame">Dribbles pm</th><th class="global sortable foulGivenPerGame   " data-stat-name="foulGivenPerGame">Fautes subies pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/37/Show/Allemagne-Bayern-Munich">1. Bayern Munich</a></td> <td class="shotsPerGame   sorted  ">19.8</td><td class="shotOnTargetPerGame   ">7.8</td><td class="dribbleWonPerGame   ">14.5</td><td class="foulGivenPerGame   ">10.4</td><td class=" "><span class="stat-value rating">6.98</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/134/Show/Allemagne-Borussia-M-Gladbach">2. Borussia M.Gladbach</a></td> <td class="shotsPerGame   sorted  ">14.8</td><td class="shotOnTargetPerGame   ">5.4</td><td class="dribbleWonPerGame   ">10.9</td><td class="foulGivenPerGame   ">13.9</td><td class=" "><span class="stat-value rating">6.67</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/219/Show/Allemagne-Mainz-05">3. Mainz 05</a></td> <td class="shotsPerGame   sorted  ">13.8</td><td class="shotOnTargetPerGame   ">4.6</td><td class="dribbleWonPerGame   ">7.9</td><td class="foulGivenPerGame   ">11.4</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/282/Show/Allemagne-FC-Koln">4. FC Koln</a></td> <td class="shotsPerGame   sorted  ">13.8</td><td class="shotOnTargetPerGame   ">4.4</td><td class="dribbleWonPerGame   ">7.4</td><td class="foulGivenPerGame   ">13.5</td><td class=" "><span class="stat-value rating">6.65</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/50/Show/Allemagne-Freiburg">5. Freiburg</a></td> <td class="shotsPerGame   sorted  ">13.6</td><td class="shotOnTargetPerGame   ">4.8</td><td class="dribbleWonPerGame   ">6.6</td><td class="foulGivenPerGame   ">11</td><td class=" "><span class="stat-value rating">6.68</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/36/Show/Allemagne-Bayer-Leverkusen">6. Bayer Leverkusen</a></td> <td class="shotsPerGame   sorted  ">13.5</td><td class="shotOnTargetPerGame   ">5.6</td><td class="dribbleWonPerGame   ">11.8</td><td class="foulGivenPerGame   ">9.7</td><td class=" "><span class="stat-value rating">6.77</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/44/Show/Allemagne-Borussia-Dortmund">7. Borussia Dortmund</a></td> <td class="shotsPerGame   sorted  ">13.3</td><td class="shotOnTargetPerGame   ">5.2</td><td class="dribbleWonPerGame   ">10.4</td><td class="foulGivenPerGame   ">11.7</td><td class=" "><span class="stat-value rating">6.77</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/41/Show/Allemagne-VfB-Stuttgart">8. VfB Stuttgart</a></td> <td class="shotsPerGame   sorted  ">13.3</td><td class="shotOnTargetPerGame   ">4.6</td><td class="dribbleWonPerGame   ">11</td><td class="foulGivenPerGame   ">12.3</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1211/Show/Allemagne-Hoffenheim">9. Hoffenheim</a></td> <td class="shotsPerGame   sorted  ">13.3</td><td class="shotOnTargetPerGame   ">4.4</td><td class="dribbleWonPerGame   ">7.4</td><td class="foulGivenPerGame   ">10.2</td><td class=" "><span class="stat-value rating">6.59</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/45/Show/Allemagne-Eintracht-Frankfurt">10. Eintracht Frankfurt</a></td> <td class="shotsPerGame   sorted  ">13.2</td><td class="shotOnTargetPerGame   ">4.1</td><td class="dribbleWonPerGame   ">8.4</td><td class="foulGivenPerGame   ">8.8</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/7614/Show/Allemagne-RB-Leipzig">11. RB Leipzig</a></td> <td class="shotsPerGame   sorted  ">12.9</td><td class="shotOnTargetPerGame   ">5.4</td><td class="dribbleWonPerGame   ">10.2</td><td class="foulGivenPerGame   ">12.5</td><td class=" "><span class="stat-value rating">6.77</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/33/Show/Allemagne-Wolfsburg">12. Wolfsburg</a></td> <td class="shotsPerGame   sorted  ">12.4</td><td class="shotOnTargetPerGame   ">4.2</td><td class="dribbleWonPerGame   ">9.4</td><td class="foulGivenPerGame   ">12.2</td><td class=" "><span class="stat-value rating">6.63</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/796/Show/Allemagne-Union-Berlin">13. Union Berlin</a></td> <td class="shotsPerGame   sorted  ">12.1</td><td class="shotOnTargetPerGame   ">4.6</td><td class="dribbleWonPerGame   ">7.2</td><td class="foulGivenPerGame   ">9.8</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/109/Show/Allemagne-Bochum">14. Bochum</a></td> <td class="shotsPerGame   sorted  ">12.1</td><td class="shotOnTargetPerGame   ">4.1</td><td class="dribbleWonPerGame   ">6.8</td><td class="foulGivenPerGame   ">11.5</td><td class=" "><span class="stat-value rating">6.58</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/47/Show/Allemagne-Hertha-Berlin">15. Hertha Berlin</a></td> <td class="shotsPerGame   sorted  ">10.8</td><td class="shotOnTargetPerGame   ">3.6</td><td class="dribbleWonPerGame   ">8.3</td><td class="foulGivenPerGame   ">12.6</td><td class=" "><span class="stat-value rating">6.48</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1730/Show/Allemagne-Augsburg">16. Augsburg</a></td> <td class="shotsPerGame   sorted  ">10.8</td><td class="shotOnTargetPerGame   ">3.7</td><td class="dribbleWonPerGame   ">7.7</td><td class="foulGivenPerGame   ">10.5</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/40/Show/Allemagne-Arminia-Bielefeld">17. Arminia Bielefeld</a></td> <td class="shotsPerGame   sorted  ">10.7</td><td class="shotOnTargetPerGame   ">3.3</td><td class="dribbleWonPerGame   ">8.4</td><td class="foulGivenPerGame   ">9</td><td class=" "><span class="stat-value rating">6.50</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/89/Show/Allemagne-Greuther-Fuerth">18. Greuther Fuerth</a></td> <td class="shotsPerGame   sorted  ">9.2</td><td class="shotOnTargetPerGame   ">3.1</td><td class="dribbleWonPerGame   ">7.8</td><td class="foulGivenPerGame   ">12.8</td><td class=" "><span class="stat-value rating">6.38</span></td></tr></tbody></table></div></div>
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
df.to_csv("team_stats_off2021.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_off2021.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_off_21-22_bundes'

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
