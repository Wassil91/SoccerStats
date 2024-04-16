from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient


html_content = """
<div id="statistics-team-table-defensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsConcededPerGame   " data-stat-name="shotsConcededPerGame">Tirs pm</th><th class="global sortable tacklePerGame   desc  " data-stat-name="tacklePerGame">Tacles pm</th><th class="global sortable interceptionPerGame   " data-stat-name="interceptionPerGame">Interceptions pm</th><th class="global sortable foulsPerGame   " data-stat-name="foulsPerGame">Fautes pm</th><th class="global sortable offsideGivenPerGame   " data-stat-name="offsideGivenPerGame">Hors-jeux pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/109/Show/Allemagne-Bochum">1. Bochum</a></td> <td class="shotsConcededPerGame   ">15.7</td><td class="tacklePerGame   sorted  ">18.4</td><td class="interceptionPerGame   ">9.9</td><td class="foulsPerGame   ">12.6</td><td class="offsideGivenPerGame   ">1.9</td><td class=" "><span class="stat-value rating">6.50</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/37/Show/Allemagne-Bayern-Munich">2. Bayern Munich</a></td> <td class="shotsConcededPerGame   ">9.2</td><td class="tacklePerGame   sorted  ">18.3</td><td class="interceptionPerGame   ">8.4</td><td class="foulsPerGame   ">9</td><td class="offsideGivenPerGame   ">2.5</td><td class=" "><span class="stat-value rating">6.94</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/282/Show/Allemagne-FC-Koln">3. FC Koln</a></td> <td class="shotsConcededPerGame   ">11.1</td><td class="tacklePerGame   sorted  ">18.1</td><td class="interceptionPerGame   ">10.9</td><td class="foulsPerGame   ">11.8</td><td class="offsideGivenPerGame   ">2</td><td class=" "><span class="stat-value rating">6.57</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/42/Show/Allemagne-Werder-Bremen">4. Werder Bremen</a></td> <td class="shotsConcededPerGame   ">14</td><td class="tacklePerGame   sorted  ">17.5</td><td class="interceptionPerGame   ">9.4</td><td class="foulsPerGame   ">12.5</td><td class="offsideGivenPerGame   ">1.9</td><td class=" "><span class="stat-value rating">6.58</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/44/Show/Allemagne-Borussia-Dortmund">5. Borussia Dortmund</a></td> <td class="shotsConcededPerGame   ">12.1</td><td class="tacklePerGame   sorted  ">17.1</td><td class="interceptionPerGame   ">8.8</td><td class="foulsPerGame   ">9.5</td><td class="offsideGivenPerGame   ">1.2</td><td class=" "><span class="stat-value rating">6.88</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/47/Show/Allemagne-Hertha-Berlin">6. Hertha Berlin</a></td> <td class="shotsConcededPerGame   ">14</td><td class="tacklePerGame   sorted  ">16.9</td><td class="interceptionPerGame   ">10.8</td><td class="foulsPerGame   ">12.4</td><td class="offsideGivenPerGame   ">1.7</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/45/Show/Allemagne-Eintracht-Frankfurt">7. Eintracht Frankfurt</a></td> <td class="shotsConcededPerGame   ">10.9</td><td class="tacklePerGame   sorted  ">16.5</td><td class="interceptionPerGame   ">8.4</td><td class="foulsPerGame   ">12.6</td><td class="offsideGivenPerGame   ">2.1</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/219/Show/Allemagne-Mainz-05">8. Mainz 05</a></td> <td class="shotsConcededPerGame   ">12.9</td><td class="tacklePerGame   sorted  ">16.1</td><td class="interceptionPerGame   ">12.7</td><td class="foulsPerGame   ">14</td><td class="offsideGivenPerGame   ">2.2</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/33/Show/Allemagne-Wolfsburg">9. Wolfsburg</a></td> <td class="shotsConcededPerGame   ">13.8</td><td class="tacklePerGame   sorted  ">16</td><td class="interceptionPerGame   ">8.6</td><td class="foulsPerGame   ">11.5</td><td class="offsideGivenPerGame   ">1.5</td><td class=" "><span class="stat-value rating">6.65</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1211/Show/Allemagne-Hoffenheim">10. Hoffenheim</a></td> <td class="shotsConcededPerGame   ">14.1</td><td class="tacklePerGame   sorted  ">16</td><td class="interceptionPerGame   ">8.4</td><td class="foulsPerGame   ">13.9</td><td class="offsideGivenPerGame   ">2.3</td><td class=" "><span class="stat-value rating">6.55</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/39/Show/Allemagne-Schalke-04">11. Schalke 04</a></td> <td class="shotsConcededPerGame   ">14.5</td><td class="tacklePerGame   sorted  ">15.9</td><td class="interceptionPerGame   ">9</td><td class="foulsPerGame   ">12.6</td><td class="offsideGivenPerGame   ">2.1</td><td class=" "><span class="stat-value rating">6.48</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/7614/Show/Allemagne-RB-Leipzig">12. RB Leipzig</a></td> <td class="shotsConcededPerGame   ">9.2</td><td class="tacklePerGame   sorted  ">15.7</td><td class="interceptionPerGame   ">9.1</td><td class="foulsPerGame   ">11.6</td><td class="offsideGivenPerGame   ">2.4</td><td class=" "><span class="stat-value rating">6.72</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/41/Show/Allemagne-VfB-Stuttgart">13. VfB Stuttgart</a></td> <td class="shotsConcededPerGame   ">11.8</td><td class="tacklePerGame   sorted  ">15.5</td><td class="interceptionPerGame   ">9.1</td><td class="foulsPerGame   ">10.2</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.57</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1730/Show/Allemagne-Augsburg">14. Augsburg</a></td> <td class="shotsConcededPerGame   ">15.9</td><td class="tacklePerGame   sorted  ">15.4</td><td class="interceptionPerGame   ">8.4</td><td class="foulsPerGame   ">12.7</td><td class="offsideGivenPerGame   ">1.7</td><td class=" "><span class="stat-value rating">6.48</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/50/Show/Allemagne-Freiburg">15. Freiburg</a></td> <td class="shotsConcededPerGame   ">13.1</td><td class="tacklePerGame   sorted  ">14.6</td><td class="interceptionPerGame   ">9.6</td><td class="foulsPerGame   ">11.9</td><td class="offsideGivenPerGame   ">1.7</td><td class=" "><span class="stat-value rating">6.67</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/134/Show/Allemagne-Borussia-M-Gladbach">16. Borussia M.Gladbach</a></td> <td class="shotsConcededPerGame   ">14.2</td><td class="tacklePerGame   sorted  ">14.6</td><td class="interceptionPerGame   ">7.9</td><td class="foulsPerGame   ">9.9</td><td class="offsideGivenPerGame   ">2.1</td><td class=" "><span class="stat-value rating">6.63</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/36/Show/Allemagne-Bayer-Leverkusen">17. Bayer Leverkusen</a></td> <td class="shotsConcededPerGame   ">11.6</td><td class="tacklePerGame   sorted  ">14.5</td><td class="interceptionPerGame   ">8.6</td><td class="foulsPerGame   ">11.2</td><td class="offsideGivenPerGame   ">1.5</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/796/Show/Allemagne-Union-Berlin">18. Union Berlin</a></td> <td class="shotsConcededPerGame   ">11.5</td><td class="tacklePerGame   sorted  ">14.5</td><td class="interceptionPerGame   ">7.7</td><td class="foulsPerGame   ">13.1</td><td class="offsideGivenPerGame   ">1.8</td><td class=" "><span class="stat-value rating">6.63</span></td></tr></tbody></table></div></div>
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
df.to_csv("donnees_stats_equipes2022.csv", index=False)

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
df.to_csv("donnees_stats_equipes2022.csv", index=False)

print("Les données ont été extraites et enregistrées dans donnees_stats_equipes2022.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_def_22-23_bundes'

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
