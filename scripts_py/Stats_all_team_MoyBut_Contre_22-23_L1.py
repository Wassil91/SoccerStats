from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient

# Votre contenu HTML ici
html_content = """
<div id="statistics-team-table-xg" class="" data-fwsc="2"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable xG   desc  " data-stat-name="xG">xG</th><th class="global sortable goalExcOwn   " data-stat-name="goalExcOwn">Buts*</th><th class="global sortable xGDiff   " data-stat-name="xGDiff">xGDiff</th><th class="global sortable totalShots   " data-stat-name="totalShots">Tirs</th><th class="global sortable xGPerShot   " data-stat-name="xGPerShot">xG/Tir</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/229/Show/France-Troyes">1. Troyes</a></td> <td class="xG   sorted  ">77.87</td><td class="goalExcOwn   ">79</td><td class="xGDiff   ">1.13</td><td class="totalShots   ">626</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/614/Show/France-Angers">2. Angers</a></td> <td class="xG   sorted  ">64.48</td><td class="goalExcOwn   ">77</td><td class="xGDiff   ">12.52</td><td class="totalShots   ">463</td><td class="xGPerShot   ">0.14</td><td class=" "><span class="stat-value rating">6.43</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/311/Show/France-Montpellier">3. Montpellier</a></td> <td class="xG   sorted  ">63.65</td><td class="goalExcOwn   ">60</td><td class="xGDiff   ">-3.65</td><td class="totalShots   ">523</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/246/Show/France-Toulouse">4. Toulouse</a></td> <td class="xG   sorted  ">59.67</td><td class="goalExcOwn   ">54</td><td class="xGDiff   ">-5.67</td><td class="totalShots   ">541</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/146/Show/France-Lorient">5. Lorient</a></td> <td class="xG   sorted  ">59.24</td><td class="goalExcOwn   ">52</td><td class="xGDiff   ">-7.24</td><td class="totalShots   ">565</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.66</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/308/Show/France-Auxerre">6. Auxerre</a></td> <td class="xG   sorted  ">57.85</td><td class="goalExcOwn   ">62</td><td class="xGDiff   ">4.15</td><td class="totalShots   ">472</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2332/Show/France-Brest">7. Brest</a></td> <td class="xG   sorted  ">57.75</td><td class="goalExcOwn   ">52</td><td class="xGDiff   ">-5.75</td><td class="totalShots   ">489</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/941/Show/France-Clermont-Foot">8. Clermont Foot</a></td> <td class="xG   sorted  ">57.48</td><td class="goalExcOwn   ">49</td><td class="xGDiff   ">-8.48</td><td class="totalShots   ">487</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.57</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/950/Show/France-Reims">9. Reims</a></td> <td class="xG   sorted  ">53.97</td><td class="goalExcOwn   ">43</td><td class="xGDiff   ">-10.97</td><td class="totalShots   ">488</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.63</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/248/Show/France-Monaco">10. Monaco</a></td> <td class="xG   sorted  ">53.92</td><td class="goalExcOwn   ">55</td><td class="xGDiff   ">1.08</td><td class="totalShots   ">512</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/302/Show/France-Nantes">11. Nantes</a></td> <td class="xG   sorted  ">53.58</td><td class="goalExcOwn   ">51</td><td class="xGDiff   ">-2.58</td><td class="totalShots   ">457</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/148/Show/France-Strasbourg">12. Strasbourg</a></td> <td class="xG   sorted  ">53.35</td><td class="goalExcOwn   ">59</td><td class="xGDiff   ">5.65</td><td class="totalShots   ">425</td><td class="xGPerShot   ">0.13</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/228/Show/France-Lyon">13. Lyon</a></td> <td class="xG   sorted  ">52.34</td><td class="goalExcOwn   ">44</td><td class="xGDiff   ">-8.34</td><td class="totalShots   ">428</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.77</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/610/Show/France-AC-Ajaccio">14. AC Ajaccio</a></td> <td class="xG   sorted  ">51.75</td><td class="goalExcOwn   ">72</td><td class="xGDiff   ">20.25</td><td class="totalShots   ">420</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.34</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/304/Show/France-Paris-Saint-Germain">15. Paris Saint-Germain</a></td> <td class="xG   sorted  ">46.79</td><td class="goalExcOwn   ">39</td><td class="xGDiff   ">-7.79</td><td class="totalShots   ">447</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.88</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/613/Show/France-Nice">16. Nice</a></td> <td class="xG   sorted  ">43.82</td><td class="goalExcOwn   ">37</td><td class="xGDiff   ">-6.82</td><td class="totalShots   ">446</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.73</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/249/Show/France-Marseille">17. Marseille</a></td> <td class="xG   sorted  ">43.79</td><td class="goalExcOwn   ">37</td><td class="xGDiff   ">-6.79</td><td class="totalShots   ">427</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.75</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/309/Show/France-Lens">18. Lens</a></td> <td class="xG   sorted  ">43.53</td><td class="goalExcOwn   ">28</td><td class="xGDiff   ">-15.53</td><td class="totalShots   ">392</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.75</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/313/Show/France-Rennes">19. Rennes</a></td> <td class="xG   sorted  ">42.86</td><td class="goalExcOwn   ">38</td><td class="xGDiff   ">-4.86</td><td class="totalShots   ">375</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.73</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/607/Show/France-Lille">20. Lille</a></td> <td class="xG   sorted  ">41.32</td><td class="goalExcOwn   ">41</td><td class="xGDiff   ">-0.32</td><td class="totalShots   ">329</td><td class="xGPerShot   ">0.13</td><td class=" "><span class="stat-value rating">6.73</span></td></tr></tbody></table></div></div>

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

# Remplacer "Clermont Foot" par "Clermont F." dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Clermont Foot", "Clermont F.")

# Enregistrer le DataFrame dans un fichier CSV
df.to_csv("team_stats_xg22.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_xg2022.csv.")

df['Journée'] = df['Journée'].astype(int)
df['Buts*'] = df['Buts*'].astype(int)
df['Tirs'] = df['Tirs'].astype(int)

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_Moyenne_But_Contre_22-23_L1'

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

