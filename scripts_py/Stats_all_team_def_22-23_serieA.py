from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient


html_content = """
<div id="statistics-team-table-defensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsConcededPerGame   " data-stat-name="shotsConcededPerGame">Tirs pm</th><th class="global sortable tacklePerGame   desc  " data-stat-name="tacklePerGame">Tacles pm</th><th class="global sortable interceptionPerGame   " data-stat-name="interceptionPerGame">Interceptions pm</th><th class="global sortable foulsPerGame   " data-stat-name="foulsPerGame">Fautes pm</th><th class="global sortable offsideGivenPerGame   " data-stat-name="offsideGivenPerGame">Hors-jeux pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/79/Show/Italie-Lecce">1. Lecce</a></td> <td class="shotsConcededPerGame   ">11.8</td><td class="tacklePerGame   sorted  ">18.6</td><td class="interceptionPerGame   ">9.7</td><td class="foulsPerGame   ">14.6</td><td class="offsideGivenPerGame   ">1.8</td><td class=" "><span class="stat-value rating">6.49</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2731/Show/Italie-Cremonese">2. Cremonese</a></td> <td class="shotsConcededPerGame   ">15.3</td><td class="tacklePerGame   sorted  ">18.5</td><td class="interceptionPerGame   ">10.3</td><td class="foulsPerGame   ">12.1</td><td class="offsideGivenPerGame   ">1.8</td><td class=" "><span class="stat-value rating">6.47</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/80/Show/Italie-AC-Milan">3. AC Milan</a></td> <td class="shotsConcededPerGame   ">10.7</td><td class="tacklePerGame   sorted  ">18.2</td><td class="interceptionPerGame   ">7.6</td><td class="foulsPerGame   ">11.8</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.70</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/86/Show/Italie-Udinese">4. Udinese</a></td> <td class="shotsConcededPerGame   ">12.3</td><td class="tacklePerGame   sorted  ">17.1</td><td class="interceptionPerGame   ">8.3</td><td class="foulsPerGame   ">12.6</td><td class="offsideGivenPerGame   ">1.3</td><td class=" "><span class="stat-value rating">6.58</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/71/Show/Italie-Bologna">5. Bologna</a></td> <td class="shotsConcededPerGame   ">12.7</td><td class="tacklePerGame   sorted  ">17</td><td class="interceptionPerGame   ">7.9</td><td class="foulsPerGame   ">12.6</td><td class="offsideGivenPerGame   ">2</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/300/Show/Italie-Atalanta">6. Atalanta</a></td> <td class="shotsConcededPerGame   ">11.8</td><td class="tacklePerGame   sorted  ">16.7</td><td class="interceptionPerGame   ">11.1</td><td class="foulsPerGame   ">12.4</td><td class="offsideGivenPerGame   ">1.5</td><td class=" "><span class="stat-value rating">6.67</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/84/Show/Italie-Roma">7. Roma</a></td> <td class="shotsConcededPerGame   ">10.4</td><td class="tacklePerGame   sorted  ">16.4</td><td class="interceptionPerGame   ">9.7</td><td class="foulsPerGame   ">11.7</td><td class="offsideGivenPerGame   ">1.5</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1501/Show/Italie-Spezia">8. Spezia</a></td> <td class="shotsConcededPerGame   ">15.8</td><td class="tacklePerGame   sorted  ">16.4</td><td class="interceptionPerGame   ">7.9</td><td class="foulsPerGame   ">13.7</td><td class="offsideGivenPerGame   ">1.4</td><td class=" "><span class="stat-value rating">6.43</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/272/Show/Italie-Empoli">9. Empoli</a></td> <td class="shotsConcededPerGame   ">16.1</td><td class="tacklePerGame   sorted  ">15.9</td><td class="interceptionPerGame   ">7.4</td><td class="foulsPerGame   ">11.6</td><td class="offsideGivenPerGame   ">1.4</td><td class=" "><span class="stat-value rating">6.53</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/76/Show/Italie-Verona">10. Verona</a></td> <td class="shotsConcededPerGame   ">14.2</td><td class="tacklePerGame   sorted  ">15.9</td><td class="interceptionPerGame   ">8.9</td><td class="foulsPerGame   ">14.2</td><td class="offsideGivenPerGame   ">2</td><td class=" "><span class="stat-value rating">6.47</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/269/Show/Italie-Monza">11. Monza</a></td> <td class="shotsConcededPerGame   ">12.8</td><td class="tacklePerGame   sorted  ">15.7</td><td class="interceptionPerGame   ">8.8</td><td class="foulsPerGame   ">12.9</td><td class="offsideGivenPerGame   ">1.8</td><td class=" "><span class="stat-value rating">6.55</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/87/Show/Italie-Juventus">12. Juventus</a></td> <td class="shotsConcededPerGame   ">12.3</td><td class="tacklePerGame   sorted  ">15.6</td><td class="interceptionPerGame   ">8.1</td><td class="foulsPerGame   ">12</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/271/Show/Italie-Sampdoria">13. Sampdoria</a></td> <td class="shotsConcededPerGame   ">15.8</td><td class="tacklePerGame   sorted  ">15.5</td><td class="interceptionPerGame   ">8.3</td><td class="foulsPerGame   ">13.3</td><td class="offsideGivenPerGame   ">1.4</td><td class=" "><span class="stat-value rating">6.41</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/143/Show/Italie-Salernitana">14. Salernitana</a></td> <td class="shotsConcededPerGame   ">15.6</td><td class="tacklePerGame   sorted  ">15.4</td><td class="interceptionPerGame   ">8.7</td><td class="foulsPerGame   ">12.4</td><td class="offsideGivenPerGame   ">1.8</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/77/Show/Italie-Lazio">15. Lazio</a></td> <td class="shotsConcededPerGame   ">12.2</td><td class="tacklePerGame   sorted  ">15.3</td><td class="interceptionPerGame   ">8.2</td><td class="foulsPerGame   ">10.5</td><td class="offsideGivenPerGame   ">1.4</td><td class=" "><span class="stat-value rating">6.74</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/73/Show/Italie-Fiorentina">16. Fiorentina</a></td> <td class="shotsConcededPerGame   ">9.1</td><td class="tacklePerGame   sorted  ">15.3</td><td class="interceptionPerGame   ">6.8</td><td class="foulsPerGame   ">12.8</td><td class="offsideGivenPerGame   ">1.5</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/75/Show/Italie-Inter">17. Inter</a></td> <td class="shotsConcededPerGame   ">11.2</td><td class="tacklePerGame   sorted  ">15.1</td><td class="interceptionPerGame   ">8.3</td><td class="foulsPerGame   ">11.7</td><td class="offsideGivenPerGame   ">1.8</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/276/Show/Italie-Napoli">18. Napoli</a></td> <td class="shotsConcededPerGame   ">9.6</td><td class="tacklePerGame   sorted  ">15</td><td class="interceptionPerGame   ">7.6</td><td class="foulsPerGame   ">10.2</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.81</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2889/Show/Italie-Sassuolo">19. Sassuolo</a></td> <td class="shotsConcededPerGame   ">12.7</td><td class="tacklePerGame   sorted  ">13.2</td><td class="interceptionPerGame   ">7.3</td><td class="foulsPerGame   ">10.7</td><td class="offsideGivenPerGame   ">1.1</td><td class=" "><span class="stat-value rating">6.48</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/72/Show/Italie-Torino">20. Torino</a></td> <td class="shotsConcededPerGame   ">11.4</td><td class="tacklePerGame   sorted  ">12.4</td><td class="interceptionPerGame   ">7.7</td><td class="foulsPerGame   ">13.3</td><td class="offsideGivenPerGame   ">2.1</td><td class=" "><span class="stat-value rating">6.57</span></td></tr></tbody></table></div></div>
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
collection_name = 'Stats_All_Team_def_22-23_serieA'

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