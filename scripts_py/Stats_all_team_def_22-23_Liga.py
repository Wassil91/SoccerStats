from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient


html_content = """
<div id="statistics-team-table-defensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsConcededPerGame   " data-stat-name="shotsConcededPerGame">Tirs pm</th><th class="global sortable tacklePerGame   desc  " data-stat-name="tacklePerGame">Tacles pm</th><th class="global sortable interceptionPerGame   " data-stat-name="interceptionPerGame">Interceptions pm</th><th class="global sortable foulsPerGame   " data-stat-name="foulsPerGame">Fautes pm</th><th class="global sortable offsideGivenPerGame   " data-stat-name="offsideGivenPerGame">Hors-jeux pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/55/Show/Espagne-Valencia">1. Valencia</a></td> <td class="shotsConcededPerGame   ">10.5</td><td class="tacklePerGame   sorted  ">17.8</td><td class="interceptionPerGame   ">7.6</td><td class="foulsPerGame   ">13.3</td><td class="offsideGivenPerGame   ">2.1</td><td class=" "><span class="stat-value rating">6.54</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/58/Show/Espagne-Real-Valladolid">2. Real Valladolid</a></td> <td class="shotsConcededPerGame   ">15.5</td><td class="tacklePerGame   sorted  ">17.7</td><td class="interceptionPerGame   ">9.1</td><td class="foulsPerGame   ">12.1</td><td class="offsideGivenPerGame   ">2.6</td><td class=" "><span class="stat-value rating">6.50</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/63/Show/Espagne-Atletico-Madrid">3. Atletico Madrid</a></td> <td class="shotsConcededPerGame   ">11.5</td><td class="tacklePerGame   sorted  ">17.6</td><td class="interceptionPerGame   ">8</td><td class="foulsPerGame   ">11.6</td><td class="offsideGivenPerGame   ">2.1</td><td class=" "><span class="stat-value rating">6.75</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/54/Show/Espagne-Real-Betis">4. Real Betis</a></td> <td class="shotsConcededPerGame   ">12.3</td><td class="tacklePerGame   sorted  ">17.5</td><td class="interceptionPerGame   ">9.6</td><td class="foulsPerGame   ">10.9</td><td class="offsideGivenPerGame   ">1.9</td><td class=" "><span class="stat-value rating">6.63</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/62/Show/Espagne-Celta-Vigo">5. Celta Vigo</a></td> <td class="shotsConcededPerGame   ">10.2</td><td class="tacklePerGame   sorted  ">17.5</td><td class="interceptionPerGame   ">9.7</td><td class="foulsPerGame   ">12.3</td><td class="offsideGivenPerGame   ">2.1</td><td class=" "><span class="stat-value rating">6.56</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/68/Show/Espagne-Real-Sociedad">6. Real Sociedad</a></td> <td class="shotsConcededPerGame   ">10.4</td><td class="tacklePerGame   sorted  ">17.4</td><td class="interceptionPerGame   ">7.1</td><td class="foulsPerGame   ">16.3</td><td class="offsideGivenPerGame   ">2.4</td><td class=" "><span class="stat-value rating">6.65</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/70/Show/Espagne-Espanyol">7. Espanyol</a></td> <td class="shotsConcededPerGame   ">14.3</td><td class="tacklePerGame   sorted  ">17.2</td><td class="interceptionPerGame   ">8.9</td><td class="foulsPerGame   ">12</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.53</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/64/Show/Espagne-Rayo-Vallecano">8. Rayo Vallecano</a></td> <td class="shotsConcededPerGame   ">11.5</td><td class="tacklePerGame   sorted  ">16.7</td><td class="interceptionPerGame   ">8.1</td><td class="foulsPerGame   ">14.3</td><td class="offsideGivenPerGame   ">2.6</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/833/Show/Espagne-Elche">9. Elche</a></td> <td class="shotsConcededPerGame   ">14.8</td><td class="tacklePerGame   sorted  ">16.7</td><td class="interceptionPerGame   ">8.5</td><td class="foulsPerGame   ">13.8</td><td class="offsideGivenPerGame   ">2.1</td><td class=" "><span class="stat-value rating">6.44</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1354/Show/Espagne-Cadiz">10. Cadiz</a></td> <td class="shotsConcededPerGame   ">14.5</td><td class="tacklePerGame   sorted  ">16.4</td><td class="interceptionPerGame   ">7.1</td><td class="foulsPerGame   ">14</td><td class="offsideGivenPerGame   ">2</td><td class=" "><span class="stat-value rating">6.49</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/53/Show/Espagne-Athletic-Club">11. Athletic Club</a></td> <td class="shotsConcededPerGame   ">10.3</td><td class="tacklePerGame   sorted  ">16</td><td class="interceptionPerGame   ">8.2</td><td class="foulsPerGame   ">13.5</td><td class="offsideGivenPerGame   ">2.5</td><td class=" "><span class="stat-value rating">6.59</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/67/Show/Espagne-Sevilla">12. Sevilla</a></td> <td class="shotsConcededPerGame   ">13.4</td><td class="tacklePerGame   sorted  ">15.4</td><td class="interceptionPerGame   ">8.9</td><td class="foulsPerGame   ">12.5</td><td class="offsideGivenPerGame   ">2.8</td><td class=" "><span class="stat-value rating">6.53</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/51/Show/Espagne-Mallorca">13. Mallorca</a></td> <td class="shotsConcededPerGame   ">13.1</td><td class="tacklePerGame   sorted  ">15.3</td><td class="interceptionPerGame   ">8.4</td><td class="foulsPerGame   ">16.1</td><td class="offsideGivenPerGame   ">2</td><td class=" "><span class="stat-value rating">6.56</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/839/Show/Espagne-Villarreal">14. Villarreal</a></td> <td class="shotsConcededPerGame   ">12.5</td><td class="tacklePerGame   sorted  ">15.2</td><td class="interceptionPerGame   ">6.9</td><td class="foulsPerGame   ">11.8</td><td class="offsideGivenPerGame   ">2.3</td><td class=" "><span class="stat-value rating">6.68</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1799/Show/Espagne-Almeria">15. Almeria</a></td> <td class="shotsConcededPerGame   ">14.3</td><td class="tacklePerGame   sorted  ">14.7</td><td class="interceptionPerGame   ">8.5</td><td class="foulsPerGame   ">11.7</td><td class="offsideGivenPerGame   ">2.2</td><td class=" "><span class="stat-value rating">6.50</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2783/Show/Espagne-Girona">16. Girona</a></td> <td class="shotsConcededPerGame   ">13</td><td class="tacklePerGame   sorted  ">14.5</td><td class="interceptionPerGame   ">7</td><td class="foulsPerGame   ">11.6</td><td class="offsideGivenPerGame   ">2.3</td><td class=" "><span class="stat-value rating">6.55</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/52/Show/Espagne-Real-Madrid">17. Real Madrid</a></td> <td class="shotsConcededPerGame   ">11.1</td><td class="tacklePerGame   sorted  ">14.3</td><td class="interceptionPerGame   ">7.8</td><td class="foulsPerGame   ">9.7</td><td class="offsideGivenPerGame   ">2.3</td><td class=" "><span class="stat-value rating">6.86</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/819/Show/Espagne-Getafe">18. Getafe</a></td> <td class="shotsConcededPerGame   ">12.8</td><td class="tacklePerGame   sorted  ">14.2</td><td class="interceptionPerGame   ">8.4</td><td class="foulsPerGame   ">15.8</td><td class="offsideGivenPerGame   ">2.1</td><td class=" "><span class="stat-value rating">6.49</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/131/Show/Espagne-Osasuna">19. Osasuna</a></td> <td class="shotsConcededPerGame   ">11.7</td><td class="tacklePerGame   sorted  ">14.1</td><td class="interceptionPerGame   ">8.3</td><td class="foulsPerGame   ">14.3</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.54</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/65/Show/Espagne-Barcelona">20. Barcelona</a></td> <td class="shotsConcededPerGame   ">8.7</td><td class="tacklePerGame   sorted  ">13.7</td><td class="interceptionPerGame   ">6.7</td><td class="foulsPerGame   ">11.4</td><td class="offsideGivenPerGame   ">2.6</td><td class=" "><span class="stat-value rating">6.85</span></td></tr></tbody></table></div></div>
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
df.to_csv("donnees_stats_equipes2020.csv", index=False)

# Supprimer les colonnes "Note" et "Apps"
df.drop(columns=["Note"], inplace=True)

df.insert(0, "Journée", "38")

# Supprimer la colonne "Position" si elle existe déjà
if "Position" in df.columns:
    df.drop(columns=["Position"], inplace=True)

# Insérer une nouvelle colonne "Position" à l'index 1 (après la colonne "Journée")
df.insert(1, "Position", range(1, len(df) + 1))


# Séparer la colonne "Équipe" pour obtenir seulement le nom de l'équipe
df["Équipe"] = df["Équipe"].str.split(". ", n=1).str[1]


# Save DataFrame to CSV
df.to_csv("donnees_stats_equipes2020.csv", index=False)

print("Les données ont été extraites et enregistrées dans donnees_stats_equipes2020.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_def_22-23_Liga'

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
