from bs4 import BeautifulSoup
import pandas as pd
import csv
from pymongo import MongoClient


html_content = """
<div id="statistics-team-table-xg" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable xG   desc  " data-stat-name="xG">xG</th><th class="global sortable goalExcOwn   " data-stat-name="goalExcOwn">Buts*</th><th class="global sortable xGDiff   " data-stat-name="xGDiff">xGDiff</th><th class="global sortable totalShots   " data-stat-name="totalShots">Tirs</th><th class="global sortable xGPerShot   " data-stat-name="xGPerShot">xG/Tir</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/304/Show/France-Paris-Saint-Germain">1. Paris Saint-Germain</a></td> <td class="xG   sorted  ">77.85</td><td class="goalExcOwn   ">88</td><td class="xGDiff   ">10.15</td><td class="totalShots   ">563</td><td class="xGPerShot   ">0.14</td><td class=" "><span class="stat-value rating">6.90</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/228/Show/France-Lyon">2. Lyon</a></td> <td class="xG   sorted  ">70.19</td><td class="goalExcOwn   ">64</td><td class="xGDiff   ">-6.19</td><td class="totalShots   ">554</td><td class="xGPerShot   ">0.13</td><td class=" "><span class="stat-value rating">6.78</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/313/Show/France-Rennes">3. Rennes</a></td> <td class="xG   sorted  ">63.64</td><td class="goalExcOwn   ">81</td><td class="xGDiff   ">17.36</td><td class="totalShots   ">552</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.78</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/248/Show/France-Monaco">4. Monaco</a></td> <td class="xG   sorted  ">60.01</td><td class="goalExcOwn   ">62</td><td class="xGDiff   ">1.99</td><td class="totalShots   ">454</td><td class="xGPerShot   ">0.13</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/148/Show/France-Strasbourg">5. Strasbourg</a></td> <td class="xG   sorted  ">59.85</td><td class="goalExcOwn   ">56</td><td class="xGDiff   ">-3.85</td><td class="totalShots   ">478</td><td class="xGPerShot   ">0.13</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/249/Show/France-Marseille">6. Marseille</a></td> <td class="xG   sorted  ">59.62</td><td class="goalExcOwn   ">61</td><td class="xGDiff   ">1.38</td><td class="totalShots   ">499</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/613/Show/France-Nice">7. Nice</a></td> <td class="xG   sorted  ">58.67</td><td class="goalExcOwn   ">52</td><td class="xGDiff   ">-6.67</td><td class="totalShots   ">452</td><td class="xGPerShot   ">0.13</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/607/Show/France-Lille">8. Lille</a></td> <td class="xG   sorted  ">54.28</td><td class="goalExcOwn   ">45</td><td class="xGDiff   ">-9.28</td><td class="totalShots   ">466</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/309/Show/France-Lens">9. Lens</a></td> <td class="xG   sorted  ">52.28</td><td class="goalExcOwn   ">60</td><td class="xGDiff   ">7.72</td><td class="totalShots   ">516</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.67</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/614/Show/France-Angers">10. Angers</a></td> <td class="xG   sorted  ">47.09</td><td class="goalExcOwn   ">41</td><td class="xGDiff   ">-6.09</td><td class="totalShots   ">393</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.54</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/302/Show/France-Nantes">11. Nantes</a></td> <td class="xG   sorted  ">46.1</td><td class="goalExcOwn   ">53</td><td class="xGDiff   ">6.9</td><td class="totalShots   ">425</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.65</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/315/Show/France-Bordeaux">12. Bordeaux</a></td> <td class="xG   sorted  ">45.6</td><td class="goalExcOwn   ">50</td><td class="xGDiff   ">4.4</td><td class="totalShots   ">450</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.44</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2332/Show/France-Brest">13. Brest</a></td> <td class="xG   sorted  ">43.14</td><td class="goalExcOwn   ">48</td><td class="xGDiff   ">4.86</td><td class="totalShots   ">402</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.65</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/941/Show/France-Clermont-Foot">14. Clermont Foot</a></td> <td class="xG   sorted  ">43.08</td><td class="goalExcOwn   ">36</td><td class="xGDiff   ">-7.08</td><td class="totalShots   ">438</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.46</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/311/Show/France-Montpellier">15. Montpellier</a></td> <td class="xG   sorted  ">42.53</td><td class="goalExcOwn   ">48</td><td class="xGDiff   ">5.47</td><td class="totalShots   ">437</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.56</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/229/Show/France-Troyes">16. Troyes</a></td> <td class="xG   sorted  ">41.93</td><td class="goalExcOwn   ">35</td><td class="xGDiff   ">-6.93</td><td class="totalShots   ">386</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/145/Show/France-Saint-Etienne">17. Saint-Etienne</a></td> <td class="xG   sorted  ">40.87</td><td class="goalExcOwn   ">41</td><td class="xGDiff   ">0.13</td><td class="totalShots   ">439</td><td class="xGPerShot   ">0.09</td><td class=" "><span class="stat-value rating">6.47</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/146/Show/France-Lorient">18. Lorient</a></td> <td class="xG   sorted  ">40.86</td><td class="goalExcOwn   ">35</td><td class="xGDiff   ">-5.86</td><td class="totalShots   ">445</td><td class="xGPerShot   ">0.09</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/950/Show/France-Reims">19. Reims</a></td> <td class="xG   sorted  ">39.5</td><td class="goalExcOwn   ">42</td><td class="xGDiff   ">2.5</td><td class="totalShots   ">395</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/314/Show/France-Metz">20. Metz</a></td> <td class="xG   sorted  ">30.2</td><td class="goalExcOwn   ">33</td><td class="xGDiff   ">2.8</td><td class="totalShots   ">371</td><td class="xGPerShot   ">0.08</td><td class=" "><span class="stat-value rating">6.52</span></td></tr></tbody></table></div></div>
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

print("Les données ont été extraites avec succès et stockées dans le fichier data_xg2021.csv.")

# Créer un DataFrame pandas avec les données extraites
df = pd.DataFrame(data, columns=['Équipe', 'xG', 'Buts*', 'xGDiff', 'Tirs', 'xG/Tir', 'Note'])

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

# Remplacer "Paris Saint-Germain" par "Paris SG" dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Paris Saint-Germain", "Paris SG")

# Remplacer "Paris Saint-Germain" par "Paris SG" dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Clermont Foot", "Clermont F.")

df['Journée'] = df['Journée'].astype(int)
df['Buts*'] = df['Buts*'].astype(int)
df['Tirs'] = df['Tirs'].astype(int)


# Enregistrer le DataFrame dans un fichier CSV
df.to_csv("team_stats_xg21.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_xg21.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_Moyenne_But_Pour_21-22_L1'

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

