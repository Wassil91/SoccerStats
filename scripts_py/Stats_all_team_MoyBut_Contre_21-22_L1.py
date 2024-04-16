from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient

# Votre contenu HTML ici
html_content = """
<div id="stage-team-stats-xg" class="statistics-table-tab" style="">
            <div id="statistics-team-mini-filter-xg" class="statistics-table-filter"><div class="clear"></div><div class="grid-toolbar"><dl id="field" class="listbox left"><dd><a class="option  selected " data-value="Overall" data-backbone-model-attribute="field">Général</a></dd><dd><a class="option " data-value="Home" data-backbone-model-attribute="field">Domicile</a></dd><dd><a class="option " data-value="Away" data-backbone-model-attribute="field">Extérieur</a></dd></dl></div><div class="grid-toolbar"><dl id="against" class="listbox left"><dd><a class="option " data-value="false" data-backbone-model-attribute="against">Pour</a></dd><dd><a class="option  selected " data-value="true" data-backbone-model-attribute="against">Contre</a></dd></dl><dl id="pens" class="listbox right"><dd><a class="option  selected " data-value="true" data-backbone-model-attribute="incPens">Tout</a></dd><dd><a class="option " data-value="false" data-backbone-model-attribute="incPens">Exclure les pénalités</a></dd></dl></div></div>
            <div id="statistics-team-table-xg-loading" class="loading-wrapper" style="display: none;"><div class="loading-spinner-container"> <div class="loading-spinner"><div class="spinner-container loading-container1"><div class="circle1"></div><div class="circle2"></div><div class="circle3"></div><div class="circle4"></div></div><div class="spinner-container loading-container2"><div class="circle1"></div><div class="circle2"></div><div class="circle3"></div><div class="circle4"></div></div><div class="spinner-container loading-container3"><div class="circle1"></div><div class="circle2"></div><div class="circle3"></div><div class="circle4"></div></div></div><div class="loading-spinner-container-shade"></div></div></div>
            <div id="statistics-team-table-xg" class="" data-fwsc="2"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable xG   desc  " data-stat-name="xG">xG</th><th class="global sortable goalExcOwn   " data-stat-name="goalExcOwn">Buts*</th><th class="global sortable xGDiff   " data-stat-name="xGDiff">xGDiff</th><th class="global sortable totalShots   " data-stat-name="totalShots">Tirs</th><th class="global sortable xGPerShot   " data-stat-name="xGPerShot">xG/Tir</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/315/Show/France-Bordeaux">1. Bordeaux</a></td> <td class="xG   sorted  ">67.75</td><td class="goalExcOwn   ">88</td><td class="xGDiff   ">20.25</td><td class="totalShots   ">522</td><td class="xGPerShot   ">0.13</td><td class=" "><span class="stat-value rating">6.44</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/314/Show/France-Metz">2. Metz</a></td> <td class="xG   sorted  ">66.43</td><td class="goalExcOwn   ">67</td><td class="xGDiff   ">0.57</td><td class="totalShots   ">563</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.52</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/145/Show/France-Saint-Etienne">3. Saint-Etienne</a></td> <td class="xG   sorted  ">62.6</td><td class="goalExcOwn   ">73</td><td class="xGDiff   ">10.4</td><td class="totalShots   ">460</td><td class="xGPerShot   ">0.14</td><td class=" "><span class="stat-value rating">6.47</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2332/Show/France-Brest">4. Brest</a></td> <td class="xG   sorted  ">57.87</td><td class="goalExcOwn   ">55</td><td class="xGDiff   ">-2.87</td><td class="totalShots   ">494</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.65</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/311/Show/France-Montpellier">5. Montpellier</a></td> <td class="xG   sorted  ">57.5</td><td class="goalExcOwn   ">60</td><td class="xGDiff   ">2.5</td><td class="totalShots   ">602</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.56</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/228/Show/France-Lyon">6. Lyon</a></td> <td class="xG   sorted  ">56.7</td><td class="goalExcOwn   ">48</td><td class="xGDiff   ">-8.7</td><td class="totalShots   ">464</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.78</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/146/Show/France-Lorient">7. Lorient</a></td> <td class="xG   sorted  ">53.97</td><td class="goalExcOwn   ">60</td><td class="xGDiff   ">6.03</td><td class="totalShots   ">465</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/302/Show/France-Nantes">8. Nantes</a></td> <td class="xG   sorted  ">53.41</td><td class="goalExcOwn   ">47</td><td class="xGDiff   ">-6.41</td><td class="totalShots   ">474</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.65</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/229/Show/France-Troyes">9. Troyes</a></td> <td class="xG   sorted  ">52.54</td><td class="goalExcOwn   ">51</td><td class="xGDiff   ">-1.54</td><td class="totalShots   ">462</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/941/Show/France-Clermont-Foot">10. Clermont Foot</a></td> <td class="xG   sorted  ">52.46</td><td class="goalExcOwn   ">66</td><td class="xGDiff   ">13.54</td><td class="totalShots   ">440</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.46</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/614/Show/France-Angers">11. Angers</a></td> <td class="xG   sorted  ">50.78</td><td class="goalExcOwn   ">52</td><td class="xGDiff   ">1.22</td><td class="totalShots   ">431</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.54</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/950/Show/France-Reims">12. Reims</a></td> <td class="xG   sorted  ">49.09</td><td class="goalExcOwn   ">44</td><td class="xGDiff   ">-5.09</td><td class="totalShots   ">513</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/613/Show/France-Nice">13. Nice</a></td> <td class="xG   sorted  ">44.94</td><td class="goalExcOwn   ">36</td><td class="xGDiff   ">-8.94</td><td class="totalShots   ">434</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/148/Show/France-Strasbourg">14. Strasbourg</a></td> <td class="xG   sorted  ">44.18</td><td class="goalExcOwn   ">42</td><td class="xGDiff   ">-2.18</td><td class="totalShots   ">413</td><td class="xGPerShot   ">0.11</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/309/Show/France-Lens">15. Lens</a></td> <td class="xG   sorted  ">43.24</td><td class="goalExcOwn   ">48</td><td class="xGDiff   ">4.76</td><td class="totalShots   ">413</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.67</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/248/Show/France-Monaco">16. Monaco</a></td> <td class="xG   sorted  ">42.89</td><td class="goalExcOwn   ">37</td><td class="xGDiff   ">-5.89</td><td class="totalShots   ">418</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/249/Show/France-Marseille">17. Marseille</a></td> <td class="xG   sorted  ">41.58</td><td class="goalExcOwn   ">37</td><td class="xGDiff   ">-4.58</td><td class="totalShots   ">332</td><td class="xGPerShot   ">0.13</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/313/Show/France-Rennes">18. Rennes</a></td> <td class="xG   sorted  ">41.07</td><td class="goalExcOwn   ">39</td><td class="xGDiff   ">-2.07</td><td class="totalShots   ">356</td><td class="xGPerShot   ">0.12</td><td class=" "><span class="stat-value rating">6.78</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/304/Show/France-Paris-Saint-Germain">19. Paris Saint-Germain</a></td> <td class="xG   sorted  ">39.92</td><td class="goalExcOwn   ">35</td><td class="xGDiff   ">-4.92</td><td class="totalShots   ">411</td><td class="xGPerShot   ">0.1</td><td class=" "><span class="stat-value rating">6.90</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/607/Show/France-Lille">20. Lille</a></td> <td class="xG   sorted  ">38.35</td><td class="goalExcOwn   ">46</td><td class="xGDiff   ">7.65</td><td class="totalShots   ">448</td><td class="xGPerShot   ">0.09</td><td class=" "><span class="stat-value rating">6.62</span></td></tr></tbody></table></div></div>
            <div id="statistics-team-table-xg-column-legend"><div class="table-column-legend info"><div class="col12-lg-4 col12-m-4 col12-s-6 col12-xs-6"><strong>Buts*</strong>: GoalExcOwn</div>  <div class="col12-lg-4 col12-m-4 col12-s-6 col12-xs-6"><strong>Tirs</strong>: TotalShots</div><div class="col12-lg-4 col12-m-4 col12-s-6 col12-xs-6"><strong>xG/Tir</strong>: XGPerShot</div></div></div>
        </div>
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
df.to_csv("team_stats_xg21.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_xg2021.csv.")

df['Journée'] = df['Journée'].astype(int)
df['Buts*'] = df['Buts*'].astype(int)
df['Tirs'] = df['Tirs'].astype(int)

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_Moyenne_But_Contre_21-22_L1'

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

