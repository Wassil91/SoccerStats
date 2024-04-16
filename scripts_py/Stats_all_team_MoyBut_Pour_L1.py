from bs4 import BeautifulSoup
import pandas as pd
import csv
from pymongo import MongoClient


html_content = """
<div id="statistics-team-table-xg" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable xG   desc  " data-stat-name="xG">xG</th><th class="global sortable goalExcOwn   " data-stat-name="goalExcOwn">Buts*</th><th class="global sortable xGDiff   " data-stat-name="xGDiff">xGDiff</th><th class="global sortable totalShots   " data-stat-name="totalShots">Tirs</th><th class="global sortable xGPerShot   " data-stat-name="xGPerShot">xG/Tir</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/304/Show/France-Paris-Saint-Germain">1. Paris Saint-Germain</a></td> <td class="xG   sorted  ">53.02</td><td class="goalExcOwn   ">60</td><td class="xGDiff   ">6.98</td><td class="totalShots   ">390</td><td class="xGPerShot   ">0.14</td><td class=" "><span class="stat-value rating">6.95</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/309/Show/France-Lens">2. Lens</a></td> <td class="xG   sorted  ">45.2</td><td class="goalExcOwn   ">35</td><td class="xGDiff   ">-10.2</td><td class="totalShots   ">361</td><td class="xGPerShot   ">0.13</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/248/Show/France-Monaco">3. Monaco</a></td> <td class="xG   sorted  ">43.54</td><td class="goalExcOwn   ">45</td><td class="xGDiff   ">1.46</td><td class="totalShots   ">393</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.76</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/249/Show/France-Marseille">4. Marseille</a></td> <td class="xG   sorted  ">41.92</td><td class="goalExcOwn   ">38</td><td class="xGDiff   ">-3.92</td><td class="totalShots   ">355</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.73</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/607/Show/France-Lille">5. Lille</a></td> <td class="xG   sorted  ">41.79</td><td class="goalExcOwn   ">35</td><td class="xGDiff   ">-6.79</td><td class="totalShots   ">349</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.76</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/950/Show/France-Reims">6. Reims</a></td> <td class="xG   sorted  ">40.03</td><td class="goalExcOwn   ">33</td><td class="xGDiff   ">-7.03</td><td class="totalShots   ">326</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.63</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/313/Show/France-Rennes">7. Rennes</a></td> <td class="xG   sorted  ">39.98</td><td class="goalExcOwn   ">40</td><td class="xGDiff   ">0.02</td><td class="totalShots   ">357</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.67</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/613/Show/France-Nice">8. Nice</a></td> <td class="xG   sorted  ">39.5</td><td class="goalExcOwn   ">26</td><td class="xGDiff   ">-13.5</td><td class="totalShots   ">357</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.65</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/311/Show/France-Montpellier">9. Montpellier</a></td> <td class="xG   sorted  ">39.02</td><td class="goalExcOwn   ">29</td><td class="xGDiff   ">-10.02</td><td class="totalShots   ">354</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2332/Show/France-Brest">10. Brest</a></td> <td class="xG   sorted  ">37.61</td><td class="goalExcOwn   ">35</td><td class="xGDiff   ">-2.61</td><td class="totalShots   ">369</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.76</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/228/Show/France-Lyon">11. Lyon</a></td> <td class="xG   sorted  ">37.28</td><td class="goalExcOwn   ">29</td><td class="xGDiff   ">-8.28</td><td class="totalShots   ">329</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.59</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/246/Show/France-Toulouse">12. Toulouse</a></td> <td class="xG   sorted  ">34.54</td><td class="goalExcOwn   ">29</td><td class="xGDiff   ">-5.54</td><td class="totalShots   ">307</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/148/Show/France-Strasbourg">13. Strasbourg</a></td> <td class="xG   sorted  ">32.73</td><td class="goalExcOwn   ">26</td><td class="xGDiff   ">-6.73</td><td class="totalShots   ">285</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.54</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/302/Show/France-Nantes">14. Nantes</a></td> <td class="xG   sorted  ">30.67</td><td class="goalExcOwn   ">24</td><td class="xGDiff   ">-6.67</td><td class="totalShots   ">297</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/941/Show/France-Clermont-Foot">15. Clermont Foot</a></td> <td class="xG   sorted  ">29.34</td><td class="goalExcOwn   ">19</td><td class="xGDiff   ">-10.34</td><td class="totalShots   ">303</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.48</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/217/Show/France-Le-Havre">16. Le Havre</a></td> <td class="xG   sorted  ">27.67</td><td class="goalExcOwn   ">25</td><td class="xGDiff   ">-2.67</td><td class="totalShots   ">291</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.53</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/146/Show/France-Lorient">17. Lorient</a></td> <td class="xG   sorted  ">26.34</td><td class="goalExcOwn   ">33</td><td class="xGDiff   ">6.66</td><td class="totalShots   ">247</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.58</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/314/Show/France-Metz">18. Metz</a></td> <td class="xG   sorted  ">24.63</td><td class="goalExcOwn   ">23</td><td class="xGDiff   ">-1.63</td><td class="totalShots   ">258</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.52</span></td></tr></tbody></table></div></div>
"""
# Parse du contenu HTML
soup = BeautifulSoup(html_content, 'html.parser')

# Extraction des données
data = []
for row in soup.find('tbody').find_all('tr'):
    cols = row.find_all('td')
    cols = [col.get_text(strip=True) for col in cols]
    data.append(cols)

# Écriture des données dans un fichier CSV
with open('data_xg.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    # Écriture de l'en-tête
    writer.writerow(['Équipe', 'xG', 'Buts*', 'xGDiff', 'Tirs', 'xG/Tir', 'Note'])
    # Écriture des lignes de données
    writer.writerows(data)

print("Les données ont été extraites avec succès et stockées dans le fichier data_xg.csv.")

# Créer un DataFrame pandas avec les données extraites
df = pd.DataFrame(data, columns=['Équipe', 'xG', 'Buts*', 'xGDiff', 'Tirs', 'xG/Tir', 'Note'])

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

df['Journée'] = df['Journée'].astype(int)
df['Buts*'] = df['Buts*'].astype(int)
df['Tirs'] = df['Tirs'].astype(int)


# Enregistrer le DataFrame dans un fichier CSV
df.to_csv("team_stats_xg.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_xg.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_Moyenne_But_Pour_L1'

# # Supprimer la collection si elle existe
if collection_name in db.list_collection_names():
    db.drop_collection(collection_name)
    print("La collection existe. Elle a été supprimée.")

# # Recréer la collection
collection = db[collection_name]


# # Convertir le DataFrame en dictionnaire pour l'insertion dans MongoDB
data_dict = df.to_dict(orient='records')

# # Insérer les données dans la collection MongoDB
collection.insert_many(data_dict)

print("Les données ont été insérées dans la collection MongoDB avec succès.")

