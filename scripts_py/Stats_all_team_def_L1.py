from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient

html_content = """
<div id="statistics-team-table-defensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsConcededPerGame   " data-stat-name="shotsConcededPerGame">Tirs pm</th><th class="global sortable tacklePerGame   desc  " data-stat-name="tacklePerGame">Tacles pm</th><th class="global sortable interceptionPerGame   " data-stat-name="interceptionPerGame">Interceptions pm</th><th class="global sortable foulsPerGame   " data-stat-name="foulsPerGame">Fautes pm</th><th class="global sortable offsideGivenPerGame   " data-stat-name="offsideGivenPerGame">Hors-jeux pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2332/Show/France-Brest">1. Brest</a></td> <td class="shotsConcededPerGame   ">10.2</td><td class="tacklePerGame   sorted  ">20.2</td><td class="interceptionPerGame   ">9.6</td><td class="foulsPerGame   ">12.3</td><td class="offsideGivenPerGame   ">1</td><td class=" "><span class="stat-value rating">6.76</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/248/Show/France-Monaco">2. Monaco</a></td> <td class="shotsConcededPerGame   ">11.5</td><td class="tacklePerGame   sorted  ">19.5</td><td class="interceptionPerGame   ">11.4</td><td class="foulsPerGame   ">14.9</td><td class="offsideGivenPerGame   ">2.2</td><td class=" "><span class="stat-value rating">6.76</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/148/Show/France-Strasbourg">3. Strasbourg</a></td> <td class="shotsConcededPerGame   ">12.7</td><td class="tacklePerGame   sorted  ">18.8</td><td class="interceptionPerGame   ">10.9</td><td class="foulsPerGame   ">14</td><td class="offsideGivenPerGame   ">1.2</td><td class=" "><span class="stat-value rating">6.54</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/950/Show/France-Reims">4. Reims</a></td> <td class="shotsConcededPerGame   ">12.1</td><td class="tacklePerGame   sorted  ">18.3</td><td class="interceptionPerGame   ">10.1</td><td class="foulsPerGame   ">12.8</td><td class="offsideGivenPerGame   ">2.5</td><td class=" "><span class="stat-value rating">6.63</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/311/Show/France-Montpellier">5. Montpellier</a></td> <td class="shotsConcededPerGame   ">15.4</td><td class="tacklePerGame   sorted  ">18.1</td><td class="interceptionPerGame   ">9.3</td><td class="foulsPerGame   ">12.6</td><td class="offsideGivenPerGame   ">1.7</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/246/Show/France-Toulouse">6. Toulouse</a></td> <td class="shotsConcededPerGame   ">14</td><td class="tacklePerGame   sorted  ">18</td><td class="interceptionPerGame   ">8.8</td><td class="foulsPerGame   ">15</td><td class="offsideGivenPerGame   ">1.3</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/607/Show/France-Lille">7. Lille</a></td> <td class="shotsConcededPerGame   ">9.4</td><td class="tacklePerGame   sorted  ">17.8</td><td class="interceptionPerGame   ">9.1</td><td class="foulsPerGame   ">11.1</td><td class="offsideGivenPerGame   ">1.5</td><td class=" "><span class="stat-value rating">6.76</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/249/Show/France-Marseille">8. Marseille</a></td> <td class="shotsConcededPerGame   ">10.7</td><td class="tacklePerGame   sorted  ">17.3</td><td class="interceptionPerGame   ">9.9</td><td class="foulsPerGame   ">11</td><td class="offsideGivenPerGame   ">1.9</td><td class=" "><span class="stat-value rating">6.73</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/314/Show/France-Metz">9. Metz</a></td> <td class="shotsConcededPerGame   ">14.5</td><td class="tacklePerGame   sorted  ">17.3</td><td class="interceptionPerGame   ">10.3</td><td class="foulsPerGame   ">12.2</td><td class="offsideGivenPerGame   ">1.4</td><td class=" "><span class="stat-value rating">6.52</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/941/Show/France-Clermont-Foot">10. Clermont Foot</a></td> <td class="shotsConcededPerGame   ">15.5</td><td class="tacklePerGame   sorted  ">17.1</td><td class="interceptionPerGame   ">10.2</td><td class="foulsPerGame   ">11.8</td><td class="offsideGivenPerGame   ">1.3</td><td class=" "><span class="stat-value rating">6.48</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/304/Show/France-Paris-Saint-Germain">11. Paris Saint-Germain</a></td> <td class="shotsConcededPerGame   ">12</td><td class="tacklePerGame   sorted  ">16.9</td><td class="interceptionPerGame   ">8.3</td><td class="foulsPerGame   ">10.7</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.95</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/302/Show/France-Nantes">12. Nantes</a></td> <td class="shotsConcededPerGame   ">12.9</td><td class="tacklePerGame   sorted  ">16.8</td><td class="interceptionPerGame   ">10.1</td><td class="foulsPerGame   ">12.5</td><td class="offsideGivenPerGame   ">1.5</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/146/Show/France-Lorient">13. Lorient</a></td> <td class="shotsConcededPerGame   ">16.2</td><td class="tacklePerGame   sorted  ">16.4</td><td class="interceptionPerGame   ">9.9</td><td class="foulsPerGame   ">11.8</td><td class="offsideGivenPerGame   ">1.4</td><td class=" "><span class="stat-value rating">6.58</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/228/Show/France-Lyon">14. Lyon</a></td> <td class="shotsConcededPerGame   ">13.3</td><td class="tacklePerGame   sorted  ">16.3</td><td class="interceptionPerGame   ">8.8</td><td class="foulsPerGame   ">12.8</td><td class="offsideGivenPerGame   ">1.2</td><td class=" "><span class="stat-value rating">6.59</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/613/Show/France-Nice">15. Nice</a></td> <td class="shotsConcededPerGame   ">10.6</td><td class="tacklePerGame   sorted  ">15.8</td><td class="interceptionPerGame   ">8.3</td><td class="foulsPerGame   ">11.9</td><td class="offsideGivenPerGame   ">1.7</td><td class=" "><span class="stat-value rating">6.65</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/217/Show/France-Le-Havre">16. Le Havre</a></td> <td class="shotsConcededPerGame   ">13.4</td><td class="tacklePerGame   sorted  ">15.7</td><td class="interceptionPerGame   ">10.3</td><td class="foulsPerGame   ">13.7</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.53</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/313/Show/France-Rennes">17. Rennes</a></td> <td class="shotsConcededPerGame   ">12</td><td class="tacklePerGame   sorted  ">15.7</td><td class="interceptionPerGame   ">10.2</td><td class="foulsPerGame   ">11.6</td><td class="offsideGivenPerGame   ">1.1</td><td class=" "><span class="stat-value rating">6.67</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/309/Show/France-Lens">18. Lens</a></td> <td class="shotsConcededPerGame   ">11.5</td><td class="tacklePerGame   sorted  ">15.7</td><td class="interceptionPerGame   ">8.5</td><td class="foulsPerGame   ">12.9</td><td class="offsideGivenPerGame   ">2.1</td><td class=" "><span class="stat-value rating">6.61</span></td></tr></tbody></table></div></div>
"""

soup = BeautifulSoup(html_content, "html.parser")

# Extraire les données de la table
data = []
for row in soup.find_all("tr"):
    row_data = []
    for cell in row.find_all(["th", "td"]):
        row_data.append(cell.get_text(strip=True))
    if row_data:
        data.append(row_data)

# Créer un DataFrame pandas avec les données extraites
df = pd.DataFrame(data[1:], columns=data[0])

# Enregistrer les données dans un fichier CSV
df.to_csv("donnees_stats_equipes.csv", index=False)

# Supprimer les colonnes "Note" et "Apps"
df.drop(columns=["Note"], inplace=True)

df.insert(0, "Journée", "26")

# Supprimer la colonne "Position" si elle existe déjà
if "Position" in df.columns:
    df.drop(columns=["Position"], inplace=True)

# Insérer une nouvelle colonne "Position" à l'index 1 (après la colonne "Journée")
df.insert(1, "Position", range(1, len(df) + 1))


# Séparer la colonne "Équipe" pour obtenir seulement le nom de l'équipe
df["Équipe"] = df["Équipe"].str.split(". ", n=1).str[1]

# Remplacer "Paris Saint-Germain" par "Paris SG" dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Paris Saint-Germain", "Paris SG")

# Remplacer "Paris Saint-Germain" par "Paris SG" dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Clermont Foot", "Clermont F.")

# Save DataFrame to CSV
df.to_csv("team_stats_def.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_def.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_def_L1'

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