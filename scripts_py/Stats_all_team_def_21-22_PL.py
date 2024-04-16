from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient


html_content = """
<div id="statistics-team-table-defensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsConcededPerGame   " data-stat-name="shotsConcededPerGame">Tirs pm</th><th class="global sortable tacklePerGame   desc  " data-stat-name="tacklePerGame">Tacles pm</th><th class="global sortable interceptionPerGame   " data-stat-name="interceptionPerGame">Interceptions pm</th><th class="global sortable foulsPerGame   " data-stat-name="foulsPerGame">Fautes pm</th><th class="global sortable offsideGivenPerGame   " data-stat-name="offsideGivenPerGame">Hors-jeux pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/19/Show/Angleterre-Leeds">1. Leeds</a></td> <td class="shotsConcededPerGame   ">15.3</td><td class="tacklePerGame   sorted  ">20.7</td><td class="interceptionPerGame   ">10.4</td><td class="foulsPerGame   ">12.3</td><td class="offsideGivenPerGame   ">1.7</td><td class=" "><span class="stat-value rating">6.53</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/31/Show/Angleterre-Everton">2. Everton</a></td> <td class="shotsConcededPerGame   ">13.7</td><td class="tacklePerGame   sorted  ">18.6</td><td class="interceptionPerGame   ">9.6</td><td class="foulsPerGame   ">9.7</td><td class="offsideGivenPerGame   ">1.4</td><td class=" "><span class="stat-value rating">6.59</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/14/Show/Angleterre-Leicester">3. Leicester</a></td> <td class="shotsConcededPerGame   ">14.9</td><td class="tacklePerGame   sorted  ">18.2</td><td class="interceptionPerGame   ">9.7</td><td class="foulsPerGame   ">9.4</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.70</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/161/Show/Angleterre-Wolves">4. Wolves</a></td> <td class="shotsConcededPerGame   ">13.4</td><td class="tacklePerGame   sorted  ">17.7</td><td class="interceptionPerGame   ">10.2</td><td class="foulsPerGame   ">9.6</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.69</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/211/Show/Angleterre-Brighton">5. Brighton</a></td> <td class="shotsConcededPerGame   ">12.5</td><td class="tacklePerGame   sorted  ">17.6</td><td class="interceptionPerGame   ">9.4</td><td class="foulsPerGame   ">10.3</td><td class="offsideGivenPerGame   ">1.4</td><td class=" "><span class="stat-value rating">6.70</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/23/Show/Angleterre-Newcastle">6. Newcastle</a></td> <td class="shotsConcededPerGame   ">13.8</td><td class="tacklePerGame   sorted  ">16.9</td><td class="interceptionPerGame   ">9.4</td><td class="foulsPerGame   ">10.1</td><td class="offsideGivenPerGame   ">1.9</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/162/Show/Angleterre-Crystal-Palace">7. Crystal Palace</a></td> <td class="shotsConcededPerGame   ">11.7</td><td class="tacklePerGame   sorted  ">16.7</td><td class="interceptionPerGame   ">8.8</td><td class="foulsPerGame   ">10.9</td><td class="offsideGivenPerGame   ">1.7</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/24/Show/Angleterre-Aston-Villa">8. Aston Villa</a></td> <td class="shotsConcededPerGame   ">12.6</td><td class="tacklePerGame   sorted  ">16.7</td><td class="interceptionPerGame   ">8.9</td><td class="foulsPerGame   ">10.6</td><td class="offsideGivenPerGame   ">1.4</td><td class=" "><span class="stat-value rating">6.68</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/15/Show/Angleterre-Chelsea">9. Chelsea</a></td> <td class="shotsConcededPerGame   ">9</td><td class="tacklePerGame   sorted  ">16.3</td><td class="interceptionPerGame   ">9.1</td><td class="foulsPerGame   ">10.8</td><td class="offsideGivenPerGame   ">1.8</td><td class=" "><span class="stat-value rating">6.92</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/27/Show/Angleterre-Watford">10. Watford</a></td> <td class="shotsConcededPerGame   ">14.8</td><td class="tacklePerGame   sorted  ">16.2</td><td class="interceptionPerGame   ">11.7</td><td class="foulsPerGame   ">11.6</td><td class="offsideGivenPerGame   ">1.4</td><td class=" "><span class="stat-value rating">6.53</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/168/Show/Angleterre-Norwich">11. Norwich</a></td> <td class="shotsConcededPerGame   ">16.6</td><td class="tacklePerGame   sorted  ">16.2</td><td class="interceptionPerGame   ">10</td><td class="foulsPerGame   ">9.4</td><td class="offsideGivenPerGame   ">1.4</td><td class=" "><span class="stat-value rating">6.42</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/189/Show/Angleterre-Brentford">12. Brentford</a></td> <td class="shotsConcededPerGame   ">13.2</td><td class="tacklePerGame   sorted  ">16</td><td class="interceptionPerGame   ">10.3</td><td class="foulsPerGame   ">9.8</td><td class="offsideGivenPerGame   ">1.7</td><td class=" "><span class="stat-value rating">6.65</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/18/Show/Angleterre-Southampton">13. Southampton</a></td> <td class="shotsConcededPerGame   ">13</td><td class="tacklePerGame   sorted  ">15.8</td><td class="interceptionPerGame   ">11.7</td><td class="foulsPerGame   ">10.6</td><td class="offsideGivenPerGame   ">1.1</td><td class=" "><span class="stat-value rating">6.63</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/184/Show/Angleterre-Burnley">14. Burnley</a></td> <td class="shotsConcededPerGame   ">15.9</td><td class="tacklePerGame   sorted  ">15.5</td><td class="interceptionPerGame   ">10.8</td><td class="foulsPerGame   ">10.4</td><td class="offsideGivenPerGame   ">2.7</td><td class=" "><span class="stat-value rating">6.65</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/32/Show/Angleterre-Manchester-United">15. Manchester United</a></td> <td class="shotsConcededPerGame   ">13.4</td><td class="tacklePerGame   sorted  ">15.4</td><td class="interceptionPerGame   ">8.9</td><td class="foulsPerGame   ">10.4</td><td class="offsideGivenPerGame   ">2.3</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/30/Show/Angleterre-Tottenham">16. Tottenham</a></td> <td class="shotsConcededPerGame   ">12.9</td><td class="tacklePerGame   sorted  ">15.3</td><td class="interceptionPerGame   ">9.3</td><td class="foulsPerGame   ">10.1</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.87</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/26/Show/Angleterre-Liverpool">17. Liverpool</a></td> <td class="shotsConcededPerGame   ">7.8</td><td class="tacklePerGame   sorted  ">14.6</td><td class="interceptionPerGame   ">9</td><td class="foulsPerGame   ">9.6</td><td class="offsideGivenPerGame   ">1.7</td><td class=" "><span class="stat-value rating">7.05</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/13/Show/Angleterre-Arsenal">18. Arsenal</a></td> <td class="shotsConcededPerGame   ">11.2</td><td class="tacklePerGame   sorted  ">14.2</td><td class="interceptionPerGame   ">7.8</td><td class="foulsPerGame   ">9.6</td><td class="offsideGivenPerGame   ">1.7</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/29/Show/Angleterre-West-Ham">19. West Ham</a></td> <td class="shotsConcededPerGame   ">14.4</td><td class="tacklePerGame   sorted  ">14.1</td><td class="interceptionPerGame   ">9.8</td><td class="foulsPerGame   ">8.5</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.75</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/167/Show/Angleterre-Manchester-City">20. Manchester City</a></td> <td class="shotsConcededPerGame   ">6.2</td><td class="tacklePerGame   sorted  ">13.1</td><td class="interceptionPerGame   ">7.3</td><td class="foulsPerGame   ">8.4</td><td class="offsideGivenPerGame   ">1.7</td><td class=" "><span class="stat-value rating">7.12</span></td></tr></tbody></table></div></div>
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
collection_name = 'Stats_All_Team_def_21-22_PL'

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
