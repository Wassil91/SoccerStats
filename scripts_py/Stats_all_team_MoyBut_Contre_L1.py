from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient

# Votre contenu HTML ici
html_content = """
<div id="statistics-team-table-xg" class="" data-fwsc="2"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable xG   desc  " data-stat-name="xG">xG</th><th class="global sortable goalExcOwn   " data-stat-name="goalExcOwn">Buts*</th><th class="global sortable xGDiff   " data-stat-name="xGDiff">xGDiff</th><th class="global sortable totalShots   " data-stat-name="totalShots">Tirs</th><th class="global sortable xGPerShot   " data-stat-name="xGPerShot">xG/Tir</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/941/Show/France-Clermont-Foot">1. Clermont Foot</a></td> <td class="xG   sorted  ">45.76</td><td class="goalExcOwn   ">42</td><td class="xGDiff   ">-3.76</td><td class="totalShots   ">403</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.48</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/302/Show/France-Nantes">2. Nantes</a></td> <td class="xG   sorted  ">44.73</td><td class="goalExcOwn   ">41</td><td class="xGDiff   ">-3.73</td><td class="totalShots   ">335</td><td class="xGPerShot   ">0.13</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/146/Show/France-Lorient">3. Lorient</a></td> <td class="xG   sorted  ">43.52</td><td class="goalExcOwn   ">48</td><td class="xGDiff   ">4.48</td><td class="totalShots   ">421</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.58</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/217/Show/France-Le-Havre">4. Le Havre</a></td> <td class="xG   sorted  ">41.74</td><td class="goalExcOwn   ">31</td><td class="xGDiff   ">-10.74</td><td class="totalShots   ">349</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.53</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/228/Show/France-Lyon">5. Lyon</a></td> <td class="xG   sorted  ">41.5</td><td class="goalExcOwn   ">40</td><td class="xGDiff   ">-1.5</td><td class="totalShots   ">346</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.59</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/314/Show/France-Metz">6. Metz</a></td> <td class="xG   sorted  ">41.17</td><td class="goalExcOwn   ">39</td><td class="xGDiff   ">-2.17</td><td class="totalShots   ">377</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.52</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/246/Show/France-Toulouse">7. Toulouse</a></td> <td class="xG   sorted  ">41.1</td><td class="goalExcOwn   ">36</td><td class="xGDiff   ">-5.1</td><td class="totalShots   ">365</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/311/Show/France-Montpellier">8. Montpellier</a></td> <td class="xG   sorted  ">40.37</td><td class="goalExcOwn   ">37</td><td class="xGDiff   ">-3.37</td><td class="totalShots   ">400</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/309/Show/France-Lens">9. Lens</a></td> <td class="xG   sorted  ">38.9</td><td class="goalExcOwn   ">26</td><td class="xGDiff   ">-12.9</td><td class="totalShots   ">299</td><td class="xGPerShot   ">0.13</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/304/Show/France-Paris-Saint-Germain">10. Paris Saint-Germain</a></td> <td class="xG   sorted  ">36.05</td><td class="goalExcOwn   ">23</td><td class="xGDiff   ">-13.05</td><td class="totalShots   ">312</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.95</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/148/Show/France-Strasbourg">11. Strasbourg</a></td> <td class="xG   sorted  ">36.04</td><td class="goalExcOwn   ">36</td><td class="xGDiff   ">-0.04</td><td class="totalShots   ">330</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.54</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/248/Show/France-Monaco">12. Monaco</a></td> <td class="xG   sorted  ">34.26</td><td class="goalExcOwn   ">34</td><td class="xGDiff   ">-0.26</td><td class="totalShots   ">299</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.76</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/950/Show/France-Reims">13. Reims</a></td> <td class="xG   sorted  ">34.09</td><td class="goalExcOwn   ">34</td><td class="xGDiff   ">-0.09</td><td class="totalShots   ">315</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.63</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/313/Show/France-Rennes">14. Rennes</a></td> <td class="xG   sorted  ">32.96</td><td class="goalExcOwn   ">29</td><td class="xGDiff   ">-3.96</td><td class="totalShots   ">313</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.67</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/607/Show/France-Lille">15. Lille</a></td> <td class="xG   sorted  ">28.84</td><td class="goalExcOwn   ">21</td><td class="xGDiff   ">-7.84</td><td class="totalShots   ">244</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.76</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2332/Show/France-Brest">16. Brest</a></td> <td class="xG   sorted  ">28.81</td><td class="goalExcOwn   ">20</td><td class="xGDiff   ">-8.81</td><td class="totalShots   ">266</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.76</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/613/Show/France-Nice">17. Nice</a></td> <td class="xG   sorted  ">27.69</td><td class="goalExcOwn   ">19</td><td class="xGDiff   ">-8.69</td><td class="totalShots   ">275</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.65</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/249/Show/France-Marseille">18. Marseille</a></td> <td class="xG   sorted  ">27.29</td><td class="goalExcOwn   ">28</td><td class="xGDiff   ">0.71</td><td class="totalShots   ">279</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.73</span></td></tr></tbody></table></div></div>
"""

# Parser le contenu HTML
soup = BeautifulSoup(html_content, 'html.parser')

# Extraction des données
data = []
for row in soup.find('tbody').find_all('tr'):
    cols = row.find_all('td')
    cols = [col.get_text(strip=True) for col in cols]
    data.append(cols)

# Créer un DataFrame pandas avec les données extraites
df = pd.DataFrame(data, columns=['Équipe', 'xG', 'Buts*', 'xGDiff', 'Tirs', 'xG/Tir', 'Note'])

# Supprimer les colonnes "Note" et "Apps"
df.drop(columns=["Note"], inplace=True)

# Insérer une nouvelle colonne "Journée"
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

# Remplacer "Clermont Foot" par "Clermont F." dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Clermont Foot", "Clermont F.")

# Enregistrer le DataFrame dans un fichier CSV
df.to_csv("team_stats_xg.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_xg.csv.")

df['Journée'] = df['Journée'].astype(int)
df['Buts*'] = df['Buts*'].astype(int)
df['Tirs'] = df['Tirs'].astype(int)

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_Moyenne_But_Contre_L1'

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

