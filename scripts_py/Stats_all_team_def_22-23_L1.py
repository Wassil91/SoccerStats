from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient


html_content = """
<div id="statistics-team-table-defensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsConcededPerGame   " data-stat-name="shotsConcededPerGame">Tirs pm</th><th class="global sortable tacklePerGame   desc  " data-stat-name="tacklePerGame">Tacles pm</th><th class="global sortable interceptionPerGame   " data-stat-name="interceptionPerGame">Interceptions pm</th><th class="global sortable foulsPerGame   " data-stat-name="foulsPerGame">Fautes pm</th><th class="global sortable offsideGivenPerGame   " data-stat-name="offsideGivenPerGame">Hors-jeux pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/613/Show/France-Nice">1. Nice</a></td> <td class="shotsConcededPerGame   ">11.7</td><td class="tacklePerGame   sorted  ">21.5</td><td class="interceptionPerGame   ">10.9</td><td class="foulsPerGame   ">10.1</td><td class="offsideGivenPerGame   ">1.3</td><td class=" "><span class="stat-value rating">6.73</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/146/Show/France-Lorient">2. Lorient</a></td> <td class="shotsConcededPerGame   ">14.9</td><td class="tacklePerGame   sorted  ">19.4</td><td class="interceptionPerGame   ">9.7</td><td class="foulsPerGame   ">10.4</td><td class="offsideGivenPerGame   ">1.3</td><td class=" "><span class="stat-value rating">6.66</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/228/Show/France-Lyon">3. Lyon</a></td> <td class="shotsConcededPerGame   ">11.3</td><td class="tacklePerGame   sorted  ">19</td><td class="interceptionPerGame   ">12.7</td><td class="foulsPerGame   ">13.2</td><td class="offsideGivenPerGame   ">1.5</td><td class=" "><span class="stat-value rating">6.77</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/246/Show/France-Toulouse">4. Toulouse</a></td> <td class="shotsConcededPerGame   ">14.2</td><td class="tacklePerGame   sorted  ">18.8</td><td class="interceptionPerGame   ">10.8</td><td class="foulsPerGame   ">12.4</td><td class="offsideGivenPerGame   ">1.9</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/302/Show/France-Nantes">5. Nantes</a></td> <td class="shotsConcededPerGame   ">12</td><td class="tacklePerGame   sorted  ">18.7</td><td class="interceptionPerGame   ">11.3</td><td class="foulsPerGame   ">12.7</td><td class="offsideGivenPerGame   ">1.4</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2332/Show/France-Brest">6. Brest</a></td> <td class="shotsConcededPerGame   ">12.9</td><td class="tacklePerGame   sorted  ">18.6</td><td class="interceptionPerGame   ">12.5</td><td class="foulsPerGame   ">13.1</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/950/Show/France-Reims">7. Reims</a></td> <td class="shotsConcededPerGame   ">12.8</td><td class="tacklePerGame   sorted  ">18.5</td><td class="interceptionPerGame   ">10.4</td><td class="foulsPerGame   ">13.3</td><td class="offsideGivenPerGame   ">2.2</td><td class=" "><span class="stat-value rating">6.63</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/248/Show/France-Monaco">8. Monaco</a></td> <td class="shotsConcededPerGame   ">13.5</td><td class="tacklePerGame   sorted  ">18.3</td><td class="interceptionPerGame   ">11.2</td><td class="foulsPerGame   ">12.8</td><td class="offsideGivenPerGame   ">2.2</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/148/Show/France-Strasbourg">9. Strasbourg</a></td> <td class="shotsConcededPerGame   ">11.2</td><td class="tacklePerGame   sorted  ">18.2</td><td class="interceptionPerGame   ">11.5</td><td class="foulsPerGame   ">13.9</td><td class="offsideGivenPerGame   ">1.1</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/249/Show/France-Marseille">10. Marseille</a></td> <td class="shotsConcededPerGame   ">11.2</td><td class="tacklePerGame   sorted  ">18</td><td class="interceptionPerGame   ">11.1</td><td class="foulsPerGame   ">14</td><td class="offsideGivenPerGame   ">1.8</td><td class=" "><span class="stat-value rating">6.75</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/941/Show/France-Clermont-Foot">11. Clermont Foot</a></td> <td class="shotsConcededPerGame   ">12.8</td><td class="tacklePerGame   sorted  ">18</td><td class="interceptionPerGame   ">11.6</td><td class="foulsPerGame   ">11.2</td><td class="offsideGivenPerGame   ">1.4</td><td class=" "><span class="stat-value rating">6.57</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/311/Show/France-Montpellier">12. Montpellier</a></td> <td class="shotsConcededPerGame   ">13.8</td><td class="tacklePerGame   sorted  ">18</td><td class="interceptionPerGame   ">10.6</td><td class="foulsPerGame   ">12.8</td><td class="offsideGivenPerGame   ">1.9</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/308/Show/France-Auxerre">13. Auxerre</a></td> <td class="shotsConcededPerGame   ">12.4</td><td class="tacklePerGame   sorted  ">17.7</td><td class="interceptionPerGame   ">11.9</td><td class="foulsPerGame   ">12.2</td><td class="offsideGivenPerGame   ">1.3</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/614/Show/France-Angers">14. Angers</a></td> <td class="shotsConcededPerGame   ">12.2</td><td class="tacklePerGame   sorted  ">17.2</td><td class="interceptionPerGame   ">9.9</td><td class="foulsPerGame   ">12.9</td><td class="offsideGivenPerGame   ">1.2</td><td class=" "><span class="stat-value rating">6.43</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/610/Show/France-AC-Ajaccio">15. AC Ajaccio</a></td> <td class="shotsConcededPerGame   ">11.1</td><td class="tacklePerGame   sorted  ">16.8</td><td class="interceptionPerGame   ">11.1</td><td class="foulsPerGame   ">14.2</td><td class="offsideGivenPerGame   ">1.7</td><td class=" "><span class="stat-value rating">6.34</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/607/Show/France-Lille">16. Lille</a></td> <td class="shotsConcededPerGame   ">8.7</td><td class="tacklePerGame   sorted  ">16.7</td><td class="interceptionPerGame   ">7.9</td><td class="foulsPerGame   ">13.1</td><td class="offsideGivenPerGame   ">1.7</td><td class=" "><span class="stat-value rating">6.73</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/304/Show/France-Paris-Saint-Germain">17. Paris Saint-Germain</a></td> <td class="shotsConcededPerGame   ">11.8</td><td class="tacklePerGame   sorted  ">16.1</td><td class="interceptionPerGame   ">7.8</td><td class="foulsPerGame   ">10</td><td class="offsideGivenPerGame   ">2</td><td class=" "><span class="stat-value rating">6.88</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/229/Show/France-Troyes">18. Troyes</a></td> <td class="shotsConcededPerGame   ">16.5</td><td class="tacklePerGame   sorted  ">16</td><td class="interceptionPerGame   ">10.5</td><td class="foulsPerGame   ">10.6</td><td class="offsideGivenPerGame   ">1.5</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/313/Show/France-Rennes">19. Rennes</a></td> <td class="shotsConcededPerGame   ">9.9</td><td class="tacklePerGame   sorted  ">15.8</td><td class="interceptionPerGame   ">9</td><td class="foulsPerGame   ">12.2</td><td class="offsideGivenPerGame   ">1.4</td><td class=" "><span class="stat-value rating">6.73</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/309/Show/France-Lens">20. Lens</a></td> <td class="shotsConcededPerGame   ">10.3</td><td class="tacklePerGame   sorted  ">14.2</td><td class="interceptionPerGame   ">8.9</td><td class="foulsPerGame   ">12.9</td><td class="offsideGivenPerGame   ">1.6</td><td class=" "><span class="stat-value rating">6.75</span></td></tr></tbody></table></div></div>
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

# Remplacer "Paris Saint-Germain" par "Paris SG" dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Paris Saint-Germain", "Paris SG")

# Remplacer "Paris Saint-Germain" par "Paris SG" dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Clermont Foot", "Clermont F.")

# Save DataFrame to CSV
df.to_csv("donnees_stats_equipes2022.csv", index=False)

print("Les données ont été extraites et enregistrées dans donnees_stats_equipes2022.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_def_22-23_L1'

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
