from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import csv

html_content = """
<div id="statistics-team-table-offensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsPerGame   desc  " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable shotOnTargetPerGame   " data-stat-name="shotOnTargetPerGame">Tirs CA pm</th><th class="global sortable dribbleWonPerGame   " data-stat-name="dribbleWonPerGame">Dribbles pm</th><th class="global sortable foulGivenPerGame   " data-stat-name="foulGivenPerGame">Fautes subies pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/26/Show/Angleterre-Liverpool">1. Liverpool</a></td> <td class="shotsPerGame   sorted  ">19.7</td><td class="shotOnTargetPerGame   ">6.9</td><td class="dribbleWonPerGame   ">8.7</td><td class="foulGivenPerGame   ">10.3</td><td class=" "><span class="stat-value rating">6.87</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/167/Show/Angleterre-Manchester-City">2. Manchester City</a></td> <td class="shotsPerGame   sorted  ">17.8</td><td class="shotOnTargetPerGame   ">6.8</td><td class="dribbleWonPerGame   ">10.6</td><td class="foulGivenPerGame   ">10.8</td><td class=" "><span class="stat-value rating">6.93</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/13/Show/Angleterre-Arsenal">3. Arsenal</a></td> <td class="shotsPerGame   sorted  ">16.7</td><td class="shotOnTargetPerGame   ">5.9</td><td class="dribbleWonPerGame   ">7.5</td><td class="foulGivenPerGame   ">10.5</td><td class=" "><span class="stat-value rating">6.84</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/30/Show/Angleterre-Tottenham">4. Tottenham</a></td> <td class="shotsPerGame   sorted  ">15.5</td><td class="shotOnTargetPerGame   ">5.8</td><td class="dribbleWonPerGame   ">10</td><td class="foulGivenPerGame   ">13.7</td><td class=" "><span class="stat-value rating">6.79</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/211/Show/Angleterre-Brighton">5. Brighton</a></td> <td class="shotsPerGame   sorted  ">14.8</td><td class="shotOnTargetPerGame   ">5.9</td><td class="dribbleWonPerGame   ">7.8</td><td class="foulGivenPerGame   ">12.4</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/24/Show/Angleterre-Aston-Villa">6. Aston Villa</a></td> <td class="shotsPerGame   sorted  ">14.4</td><td class="shotOnTargetPerGame   ">5.5</td><td class="dribbleWonPerGame   ">9.5</td><td class="foulGivenPerGame   ">12.6</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/32/Show/Angleterre-Manchester-United">7. Manchester United</a></td> <td class="shotsPerGame   sorted  ">14.2</td><td class="shotOnTargetPerGame   ">4.9</td><td class="dribbleWonPerGame   ">8.1</td><td class="foulGivenPerGame   ">9</td><td class=" "><span class="stat-value rating">6.69</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/183/Show/Angleterre-Bournemouth">8. Bournemouth</a></td> <td class="shotsPerGame   sorted  ">14.2</td><td class="shotOnTargetPerGame   ">5</td><td class="dribbleWonPerGame   ">10.2</td><td class="foulGivenPerGame   ">9.9</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/31/Show/Angleterre-Everton">9. Everton</a></td> <td class="shotsPerGame   sorted  ">13.9</td><td class="shotOnTargetPerGame   ">4.6</td><td class="dribbleWonPerGame   ">6.7</td><td class="foulGivenPerGame   ">9.2</td><td class=" "><span class="stat-value rating">6.66</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/15/Show/Angleterre-Chelsea">10. Chelsea</a></td> <td class="shotsPerGame   sorted  ">13.9</td><td class="shotOnTargetPerGame   ">5.4</td><td class="dribbleWonPerGame   ">10.1</td><td class="foulGivenPerGame   ">11.5</td><td class=" "><span class="stat-value rating">6.70</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/23/Show/Angleterre-Newcastle">11. Newcastle</a></td> <td class="shotsPerGame   sorted  ">13.9</td><td class="shotOnTargetPerGame   ">5.3</td><td class="dribbleWonPerGame   ">8.9</td><td class="foulGivenPerGame   ">12.6</td><td class=" "><span class="stat-value rating">6.74</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/170/Show/Angleterre-Fulham">12. Fulham</a></td> <td class="shotsPerGame   sorted  ">13.3</td><td class="shotOnTargetPerGame   ">4.8</td><td class="dribbleWonPerGame   ">7.2</td><td class="foulGivenPerGame   ">9.8</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/189/Show/Angleterre-Brentford">13. Brentford</a></td> <td class="shotsPerGame   sorted  ">13.1</td><td class="shotOnTargetPerGame   ">4.5</td><td class="dribbleWonPerGame   ">6</td><td class="foulGivenPerGame   ">11</td><td class=" "><span class="stat-value rating">6.59</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/29/Show/Angleterre-West-Ham">14. West Ham</a></td> <td class="shotsPerGame   sorted  ">11.9</td><td class="shotOnTargetPerGame   ">4</td><td class="dribbleWonPerGame   ">8.9</td><td class="foulGivenPerGame   ">10.3</td><td class=" "><span class="stat-value rating">6.74</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/161/Show/Angleterre-Wolves">15. Wolves</a></td> <td class="shotsPerGame   sorted  ">11.8</td><td class="shotOnTargetPerGame   ">4.2</td><td class="dribbleWonPerGame   ">11.4</td><td class="foulGivenPerGame   ">10.5</td><td class=" "><span class="stat-value rating">6.66</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/95/Show/Angleterre-Luton">16. Luton</a></td> <td class="shotsPerGame   sorted  ">11.7</td><td class="shotOnTargetPerGame   ">3.6</td><td class="dribbleWonPerGame   ">9.6</td><td class="foulGivenPerGame   ">11.8</td><td class=" "><span class="stat-value rating">6.58</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/162/Show/Angleterre-Crystal-Palace">17. Crystal Palace</a></td> <td class="shotsPerGame   sorted  ">11.7</td><td class="shotOnTargetPerGame   ">4.1</td><td class="dribbleWonPerGame   ">8.7</td><td class="foulGivenPerGame   ">11.8</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/174/Show/Angleterre-Nottingham-Forest">18. Nottingham Forest</a></td> <td class="shotsPerGame   sorted  ">11.2</td><td class="shotOnTargetPerGame   ">3.7</td><td class="dribbleWonPerGame   ">7.3</td><td class="foulGivenPerGame   ">9.8</td><td class=" "><span class="stat-value rating">6.57</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/184/Show/Angleterre-Burnley">19. Burnley</a></td> <td class="shotsPerGame   sorted  ">10.9</td><td class="shotOnTargetPerGame   ">3.5</td><td class="dribbleWonPerGame   ">8.2</td><td class="foulGivenPerGame   ">9.6</td><td class=" "><span class="stat-value rating">6.44</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/163/Show/Angleterre-Sheffield-United">20. Sheffield United</a></td> <td class="shotsPerGame   sorted  ">9</td><td class="shotOnTargetPerGame   ">3.1</td><td class="dribbleWonPerGame   ">5.9</td><td class="foulGivenPerGame   ">8.6</td><td class=" "><span class="stat-value rating">6.36</span></td></tr></tbody></table></div></div>
"""
soup = BeautifulSoup(html_content, 'html.parser')

# Extraction des données
data = []
for row in soup.find('tbody').find_all('tr'):
    cols = row.find_all('td')
    cols = [col.get_text(strip=True) for col in cols]
    data.append(cols)

# Créer un DataFrame pandas avec les données extraites
df = pd.DataFrame(data, columns=['Équipe', 'Tirs pm', 'Tirs CA pm', 'Dribbles pm', 'Fautes subies pm', 'Note'])

# Supprimer les colonnes "Note" et "Apps"
df.drop(columns=["Note"], inplace=True)

# Insérer une nouvelle colonne "Position"
df.insert(0, "Journée", "29")

# Supprimer la colonne "Position" si elle existe déjà
if "Position" in df.columns:
    df.drop(columns=["Position"], inplace=True)

# Insérer une nouvelle colonne "Position" à l'index 1
df.insert(1, "Position", range(1, len(df) + 1))

# Séparer la colonne "Équipe" pour obtenir seulement le nom de l'équipe
df["Équipe"] = df["Équipe"].str.split(". ", n=1).str[1]


# Enregistrer le DataFrame dans un fichier CSV
df.to_csv("team_stats_off.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_off.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_off_PL'

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