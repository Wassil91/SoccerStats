from bs4 import BeautifulSoup
import pandas as pd
import csv
from pymongo import MongoClient


html_content = """
<div id="statistics-team-table-xg" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable xG   desc  " data-stat-name="xG">xG</th><th class="global sortable goalExcOwn   " data-stat-name="goalExcOwn">Buts*</th><th class="global sortable xGDiff   " data-stat-name="xGDiff">xGDiff</th><th class="global sortable totalShots   " data-stat-name="totalShots">Tirs</th><th class="global sortable xGPerShot   " data-stat-name="xGPerShot">xG/Tir</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/304/Show/France-Paris-Saint-Germain">1. Paris Saint-Germain</a></td> <td class="xG   sorted  ">78.99</td><td class="goalExcOwn   ">86</td><td class="xGDiff   ">7.01</td><td class="totalShots   ">569</td><td class="xGPerShot   ">0.14</td><td class=" "><span class="stat-value rating">6.88</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/607/Show/France-Lille">2. Lille</a></td> <td class="xG   sorted  ">70.91</td><td class="goalExcOwn   ">65</td><td class="xGDiff   ">-5.91</td><td class="totalShots   ">557</td><td class="xGPerShot   ">0.13</td><td class=" "><span class="stat-value rating">6.73</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/309/Show/France-Lens">3. Lens</a></td> <td class="xG   sorted  ">67.18</td><td class="goalExcOwn   ">66</td><td class="xGDiff   ">-1.18</td><td class="totalShots   ">527</td><td class="xGPerShot   ">0.13</td><td class=" "><span class="stat-value rating">6.75</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/249/Show/France-Marseille">4. Marseille</a></td> <td class="xG   sorted  ">66.58</td><td class="goalExcOwn   ">61</td><td class="xGDiff   ">-5.58</td><td class="totalShots   ">553</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.75</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/248/Show/France-Monaco">5. Monaco</a></td> <td class="xG   sorted  ">63.73</td><td class="goalExcOwn   ">69</td><td class="xGDiff   ">5.27</td><td class="totalShots   ">478</td><td class="xGPerShot   ">0.13</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/228/Show/France-Lyon">6. Lyon</a></td> <td class="xG   sorted  ">63.15</td><td class="goalExcOwn   ">63</td><td class="xGDiff   ">-0.15</td><td class="totalShots   ">519</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.77</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/313/Show/France-Rennes">7. Rennes</a></td> <td class="xG   sorted  ">62.99</td><td class="goalExcOwn   ">66</td><td class="xGDiff   ">3.01</td><td class="totalShots   ">529</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.73</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/950/Show/France-Reims">8. Reims</a></td> <td class="xG   sorted  ">60.57</td><td class="goalExcOwn   ">45</td><td class="xGDiff   ">-15.57</td><td class="totalShots   ">524</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.63</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/246/Show/France-Toulouse">9. Toulouse</a></td> <td class="xG   sorted  ">54.19</td><td class="goalExcOwn   ">50</td><td class="xGDiff   ">-4.19</td><td class="totalShots   ">457</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/613/Show/France-Nice">10. Nice</a></td> <td class="xG   sorted  ">53.46</td><td class="goalExcOwn   ">47</td><td class="xGDiff   ">-6.46</td><td class="totalShots   ">514</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.73</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/311/Show/France-Montpellier">11. Montpellier</a></td> <td class="xG   sorted  ">51.43</td><td class="goalExcOwn   ">64</td><td class="xGDiff   ">12.57</td><td class="totalShots   ">442</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/148/Show/France-Strasbourg">12. Strasbourg</a></td> <td class="xG   sorted  ">49.59</td><td class="goalExcOwn   ">48</td><td class="xGDiff   ">-1.59</td><td class="totalShots   ">449</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/941/Show/France-Clermont-Foot">13. Clermont Foot</a></td> <td class="xG   sorted  ">48.99</td><td class="goalExcOwn   ">44</td><td class="xGDiff   ">-4.99</td><td class="totalShots   ">403</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.57</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/302/Show/France-Nantes">14. Nantes</a></td> <td class="xG   sorted  ">44.67</td><td class="goalExcOwn   ">35</td><td class="xGDiff   ">-9.67</td><td class="totalShots   ">448</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2332/Show/France-Brest">15. Brest</a></td> <td class="xG   sorted  ">43.75</td><td class="goalExcOwn   ">43</td><td class="xGDiff   ">-0.75</td><td class="totalShots   ">425</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/146/Show/France-Lorient">16. Lorient</a></td> <td class="xG   sorted  ">43.19</td><td class="goalExcOwn   ">49</td><td class="xGDiff   ">5.81</td><td class="totalShots   ">403</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.66</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/308/Show/France-Auxerre">17. Auxerre</a></td> <td class="xG   sorted  ">42.32</td><td class="goalExcOwn   ">32</td><td class="xGDiff   ">-10.32</td><td class="totalShots   ">429</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/614/Show/France-Angers">18. Angers</a></td> <td class="xG   sorted  ">40.23</td><td class="goalExcOwn   ">31</td><td class="xGDiff   ">-9.23</td><td class="totalShots   ">371</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.43</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/229/Show/France-Troyes">19. Troyes</a></td> <td class="xG   sorted  ">39.15</td><td class="goalExcOwn   ">43</td><td class="xGDiff   ">3.85</td><td class="totalShots   ">395</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/610/Show/France-AC-Ajaccio">20. AC Ajaccio</a></td> <td class="xG   sorted  ">33.94</td><td class="goalExcOwn   ">22</td><td class="xGDiff   ">-11.94</td><td class="totalShots   ">320</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.34</span></td></tr></tbody></table></div></div>
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
df.to_csv("team_stats_xg22.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_xg22.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_Moyenne_But_Pour_22-23_L1'

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

