from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient

html_content = """
<div id="statistics-team-table-defensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsConcededPerGame   " data-stat-name="shotsConcededPerGame">Tirs pm</th><th class="global sortable tacklePerGame   desc  " data-stat-name="tacklePerGame">Tacles pm</th><th class="global sortable interceptionPerGame   " data-stat-name="interceptionPerGame">Interceptions pm</th><th class="global sortable foulsPerGame   " data-stat-name="foulsPerGame">Fautes pm</th><th class="global sortable offsideGivenPerGame   " data-stat-name="offsideGivenPerGame">Hors-jeux pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/278/Show/Italie-Genoa">1. Genoa</a></td> <td class="shotsConcededPerGame   ">12.5</td><td class="tacklePerGame   sorted  ">17.1</td><td class="interceptionPerGame   ">8.9</td><td class="foulsPerGame   ">11.8</td><td class="offsideGivenPerGame   ">1</td><td class=" "><span class="stat-value rating">6.55</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/71/Show/Italie-Bologna">2. Bologna</a></td> <td class="shotsConcededPerGame   ">10.7</td><td class="tacklePerGame   sorted  ">16.6</td><td class="interceptionPerGame   ">6.9</td><td class="foulsPerGame   ">12.2</td><td class="offsideGivenPerGame   ">1.3</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/272/Show/Italie-Empoli">3. Empoli</a></td> <td class="shotsConcededPerGame   ">15</td><td class="tacklePerGame   sorted  ">16.3</td><td class="interceptionPerGame   ">6.9</td><td class="foulsPerGame   ">13.3</td><td class="offsideGivenPerGame   ">2.1</td><td class=" "><span class="stat-value rating">6.46</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/76/Show/Italie-Verona">4. Verona</a></td> <td class="shotsConcededPerGame   ">13.7</td><td class="tacklePerGame   sorted  ">16.2</td><td class="interceptionPerGame   ">7.4</td><td class="foulsPerGame   ">13.7</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/300/Show/Italie-Atalanta">5. Atalanta</a></td> <td class="shotsConcededPerGame   ">12.2</td><td class="tacklePerGame   sorted  ">16.1</td><td class="interceptionPerGame   ">9.1</td><td class="foulsPerGame   ">12.6</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.74</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/79/Show/Italie-Lecce">6. Lecce</a></td> <td class="shotsConcededPerGame   ">13.8</td><td class="tacklePerGame   sorted  ">16</td><td class="interceptionPerGame   ">8.8</td><td class="foulsPerGame   ">13.5</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.46</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/73/Show/Italie-Fiorentina">7. Fiorentina</a></td> <td class="shotsConcededPerGame   ">10.3</td><td class="tacklePerGame   sorted  ">15.6</td><td class="interceptionPerGame   ">6.2</td><td class="foulsPerGame   ">12.6</td><td class="offsideGivenPerGame   ">1.4</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/269/Show/Italie-Monza">8. Monza</a></td> <td class="shotsConcededPerGame   ">14.7</td><td class="tacklePerGame   sorted  ">15.4</td><td class="interceptionPerGame   ">7.5</td><td class="foulsPerGame   ">11.8</td><td class="offsideGivenPerGame   ">1.2</td><td class=" "><span class="stat-value rating">6.55</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/80/Show/Italie-AC-Milan">9. AC Milan</a></td> <td class="shotsConcededPerGame   ">13.5</td><td class="tacklePerGame   sorted  ">15.3</td><td class="interceptionPerGame   ">6.8</td><td class="foulsPerGame   ">11.7</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/86/Show/Italie-Udinese">10. Udinese</a></td> <td class="shotsConcededPerGame   ">12.6</td><td class="tacklePerGame   sorted  ">15.3</td><td class="interceptionPerGame   ">9.2</td><td class="foulsPerGame   ">13</td><td class="offsideGivenPerGame   ">1.3</td><td class=" "><span class="stat-value rating">6.52</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/77/Show/Italie-Lazio">11. Lazio</a></td> <td class="shotsConcededPerGame   ">12.9</td><td class="tacklePerGame   sorted  ">15.2</td><td class="interceptionPerGame   ">7.7</td><td class="foulsPerGame   ">11.9</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.56</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/87/Show/Italie-Juventus">12. Juventus</a></td> <td class="shotsConcededPerGame   ">11.5</td><td class="tacklePerGame   sorted  ">15.2</td><td class="interceptionPerGame   ">7.1</td><td class="foulsPerGame   ">12.2</td><td class="offsideGivenPerGame   ">1.9</td><td class=" "><span class="stat-value rating">6.69</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/276/Show/Italie-Napoli">13. Napoli</a></td> <td class="shotsConcededPerGame   ">10.5</td><td class="tacklePerGame   sorted  ">14.8</td><td class="interceptionPerGame   ">6.1</td><td class="foulsPerGame   ">9.7</td><td class="offsideGivenPerGame   ">2</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/84/Show/Italie-Roma">14. Roma</a></td> <td class="shotsConcededPerGame   ">11.3</td><td class="tacklePerGame   sorted  ">14.8</td><td class="interceptionPerGame   ">6.1</td><td class="foulsPerGame   ">12.6</td><td class="offsideGivenPerGame   ">2</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/75/Show/Italie-Inter">15. Inter</a></td> <td class="shotsConcededPerGame   ">10.1</td><td class="tacklePerGame   sorted  ">14.6</td><td class="interceptionPerGame   ">7.5</td><td class="foulsPerGame   ">11.1</td><td class="offsideGivenPerGame   ">1.7</td><td class=" "><span class="stat-value rating">6.88</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2732/Show/Italie-Frosinone">16. Frosinone</a></td> <td class="shotsConcededPerGame   ">15</td><td class="tacklePerGame   sorted  ">14.5</td><td class="interceptionPerGame   ">6.7</td><td class="foulsPerGame   ">10</td><td class="offsideGivenPerGame   ">1.4</td><td class=" "><span class="stat-value rating">6.45</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/78/Show/Italie-Cagliari">17. Cagliari</a></td> <td class="shotsConcededPerGame   ">13.3</td><td class="tacklePerGame   sorted  ">14</td><td class="interceptionPerGame   ">7.2</td><td class="foulsPerGame   ">12.3</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.43</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/143/Show/Italie-Salernitana">18. Salernitana</a></td> <td class="shotsConcededPerGame   ">15.4</td><td class="tacklePerGame   sorted  ">13.7</td><td class="interceptionPerGame   ">7.2</td><td class="foulsPerGame   ">12</td><td class="offsideGivenPerGame   ">1.8</td><td class=" "><span class="stat-value rating">6.36</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2889/Show/Italie-Sassuolo">19. Sassuolo</a></td> <td class="shotsConcededPerGame   ">14.8</td><td class="tacklePerGame   sorted  ">13.3</td><td class="interceptionPerGame   ">6.8</td><td class="foulsPerGame   ">10</td><td class="offsideGivenPerGame   ">0.7</td><td class=" "><span class="stat-value rating">6.42</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/72/Show/Italie-Torino">20. Torino</a></td> <td class="shotsConcededPerGame   ">10.9</td><td class="tacklePerGame   sorted  ">11.9</td><td class="interceptionPerGame   ">8.5</td><td class="foulsPerGame   ">12.3</td><td class="offsideGivenPerGame   ">1.8</td><td class=" "><span class="stat-value rating">6.58</span></td></tr></tbody></table></div></div>
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

df.insert(0, "Journée", "29")

# Supprimer la colonne "Position" si elle existe déjà
if "Position" in df.columns:
    df.drop(columns=["Position"], inplace=True)

# Insérer une nouvelle colonne "Position" à l'index 1 (après la colonne "Journée")
df.insert(1, "Position", range(1, len(df) + 1))


# Séparer la colonne "Équipe" pour obtenir seulement le nom de l'équipe
df["Équipe"] = df["Équipe"].str.split(". ", n=1).str[1]


# Save DataFrame to CSV
df.to_csv("team_stats_def.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_def.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_def_serieA'

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
