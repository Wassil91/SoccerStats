from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient


html_content = """
<div id="statistics-team-table-defensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsConcededPerGame   " data-stat-name="shotsConcededPerGame">Tirs pm</th><th class="global sortable tacklePerGame   desc  " data-stat-name="tacklePerGame">Tacles pm</th><th class="global sortable interceptionPerGame   " data-stat-name="interceptionPerGame">Interceptions pm</th><th class="global sortable foulsPerGame   " data-stat-name="foulsPerGame">Fautes pm</th><th class="global sortable offsideGivenPerGame   " data-stat-name="offsideGivenPerGame">Hors-jeux pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/80/Show/Italie-AC-Milan">1. AC Milan</a></td> <td class="shotsConcededPerGame   ">12.2</td><td class="tacklePerGame   sorted  ">16.5</td><td class="interceptionPerGame   ">10.1</td><td class="foulsPerGame   ">13.5</td><td class="offsideGivenPerGame   ">2.4</td><td class=" "><span class="stat-value rating">6.82</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/300/Show/Italie-Atalanta">2. Atalanta</a></td> <td class="shotsConcededPerGame   ">8.6</td><td class="tacklePerGame   sorted  ">15.9</td><td class="interceptionPerGame   ">13.6</td><td class="foulsPerGame   ">15.3</td><td class="offsideGivenPerGame   ">1.5</td><td class=" "><span class="stat-value rating">6.84</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/77/Show/Italie-Lazio">3. Lazio</a></td> <td class="shotsConcededPerGame   ">10.6</td><td class="tacklePerGame   sorted  ">15.7</td><td class="interceptionPerGame   ">11.7</td><td class="foulsPerGame   ">13.1</td><td class="offsideGivenPerGame   ">2.3</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/78/Show/Italie-Cagliari">4. Cagliari</a></td> <td class="shotsConcededPerGame   ">14.6</td><td class="tacklePerGame   sorted  ">15.5</td><td class="interceptionPerGame   ">10.2</td><td class="foulsPerGame   ">13.6</td><td class="offsideGivenPerGame   ">1.5</td><td class=" "><span class="stat-value rating">6.58</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/24341/Show/Italie-Parma-Calcio-1913">5. Parma Calcio 1913</a></td> <td class="shotsConcededPerGame   ">13.9</td><td class="tacklePerGame   sorted  ">15.4</td><td class="interceptionPerGame   ">12.2</td><td class="foulsPerGame   ">13.8</td><td class="offsideGivenPerGame   ">2.1</td><td class=" "><span class="stat-value rating">6.49</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/271/Show/Italie-Sampdoria">6. Sampdoria</a></td> <td class="shotsConcededPerGame   ">13.6</td><td class="tacklePerGame   sorted  ">15.2</td><td class="interceptionPerGame   ">11.9</td><td class="foulsPerGame   ">13.4</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.63</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/276/Show/Italie-Napoli">7. Napoli</a></td> <td class="shotsConcededPerGame   ">10.3</td><td class="tacklePerGame   sorted  ">15.1</td><td class="interceptionPerGame   ">9.7</td><td class="foulsPerGame   ">10.2</td><td class="offsideGivenPerGame   ">1.9</td><td class=" "><span class="stat-value rating">6.81</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1501/Show/Italie-Spezia">8. Spezia</a></td> <td class="shotsConcededPerGame   ">11.2</td><td class="tacklePerGame   sorted  ">14.9</td><td class="interceptionPerGame   ">10.4</td><td class="foulsPerGame   ">15.5</td><td class="offsideGivenPerGame   ">2.2</td><td class=" "><span class="stat-value rating">6.50</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/87/Show/Italie-Juventus">9. Juventus</a></td> <td class="shotsConcededPerGame   ">11.4</td><td class="tacklePerGame   sorted  ">14.9</td><td class="interceptionPerGame   ">12.1</td><td class="foulsPerGame   ">13.3</td><td class="offsideGivenPerGame   ">2.4</td><td class=" "><span class="stat-value rating">6.85</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/278/Show/Italie-Genoa">10. Genoa</a></td> <td class="shotsConcededPerGame   ">14.3</td><td class="tacklePerGame   sorted  ">14.6</td><td class="interceptionPerGame   ">13.7</td><td class="foulsPerGame   ">13.7</td><td class="offsideGivenPerGame   ">2.1</td><td class=" "><span class="stat-value rating">6.54</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/71/Show/Italie-Bologna">11. Bologna</a></td> <td class="shotsConcededPerGame   ">14.2</td><td class="tacklePerGame   sorted  ">14.3</td><td class="interceptionPerGame   ">11.2</td><td class="foulsPerGame   ">14.1</td><td class="offsideGivenPerGame   ">1.9</td><td class=" "><span class="stat-value rating">6.56</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/779/Show/Italie-Crotone">12. Crotone</a></td> <td class="shotsConcededPerGame   ">15.6</td><td class="tacklePerGame   sorted  ">14.2</td><td class="interceptionPerGame   ">10</td><td class="foulsPerGame   ">14</td><td class="offsideGivenPerGame   ">1.5</td><td class=" "><span class="stat-value rating">6.43</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/72/Show/Italie-Torino">13. Torino</a></td> <td class="shotsConcededPerGame   ">13.3</td><td class="tacklePerGame   sorted  ">13.9</td><td class="interceptionPerGame   ">11.7</td><td class="foulsPerGame   ">15.2</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.59</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2889/Show/Italie-Sassuolo">14. Sassuolo</a></td> <td class="shotsConcededPerGame   ">13.9</td><td class="tacklePerGame   sorted  ">13.9</td><td class="interceptionPerGame   ">11.2</td><td class="foulsPerGame   ">13</td><td class="offsideGivenPerGame   ">1.5</td><td class=" "><span class="stat-value rating">6.67</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1503/Show/Italie-Benevento">15. Benevento</a></td> <td class="shotsConcededPerGame   ">13.2</td><td class="tacklePerGame   sorted  ">13.4</td><td class="interceptionPerGame   ">10.1</td><td class="foulsPerGame   ">13.1</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.43</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/75/Show/Italie-Inter">16. Inter</a></td> <td class="shotsConcededPerGame   ">10.2</td><td class="tacklePerGame   sorted  ">13.3</td><td class="interceptionPerGame   ">9.8</td><td class="foulsPerGame   ">12.4</td><td class="offsideGivenPerGame   ">1.7</td><td class=" "><span class="stat-value rating">6.81</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/86/Show/Italie-Udinese">17. Udinese</a></td> <td class="shotsConcededPerGame   ">12.2</td><td class="tacklePerGame   sorted  ">13.2</td><td class="interceptionPerGame   ">11.6</td><td class="foulsPerGame   ">13.4</td><td class="offsideGivenPerGame   ">2.2</td><td class=" "><span class="stat-value rating">6.55</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/73/Show/Italie-Fiorentina">18. Fiorentina</a></td> <td class="shotsConcededPerGame   ">12</td><td class="tacklePerGame   sorted  ">13.1</td><td class="interceptionPerGame   ">10.2</td><td class="foulsPerGame   ">13.8</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.52</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/84/Show/Italie-Roma">19. Roma</a></td> <td class="shotsConcededPerGame   ">10.4</td><td class="tacklePerGame   sorted  ">12.9</td><td class="interceptionPerGame   ">12.9</td><td class="foulsPerGame   ">14.6</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/76/Show/Italie-Verona">20. Verona</a></td> <td class="shotsConcededPerGame   ">14.1</td><td class="tacklePerGame   sorted  ">12.5</td><td class="interceptionPerGame   ">12.9</td><td class="foulsPerGame   ">16.4</td><td class="offsideGivenPerGame   ">2.8</td><td class=" "><span class="stat-value rating">6.59</span></td></tr></tbody></table></div></div>
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

# Remplacer "Paris Saint-Germain" par "Paris SG" dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Paris Saint-Germain", "Paris SG")

# Remplacer "Paris Saint-Germain" par "Paris SG" dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Clermont Foot", "Clermont F.")

# Save DataFrame to CSV
df.to_csv("donnees_stats_equipes2020.csv", index=False)

print("Les données ont été extraites et enregistrées dans donnees_stats_equipes2020.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_def_20-21_serieA'

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
