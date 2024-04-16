from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import csv

html_content = """
<div id="statistics-team-table-offensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsPerGame   desc  " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable shotOnTargetPerGame   " data-stat-name="shotOnTargetPerGame">Tirs CA pm</th><th class="global sortable dribbleWonPerGame   " data-stat-name="dribbleWonPerGame">Dribbles pm</th><th class="global sortable foulGivenPerGame   " data-stat-name="foulGivenPerGame">Fautes subies pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/211/Show/Angleterre-Brighton">1. Brighton</a></td> <td class="shotsPerGame   sorted  ">16.1</td><td class="shotOnTargetPerGame   ">6.1</td><td class="dribbleWonPerGame   ">9.1</td><td class="foulGivenPerGame   ">10.7</td><td class=" "><span class="stat-value rating">6.72</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/26/Show/Angleterre-Liverpool">2. Liverpool</a></td> <td class="shotsPerGame   sorted  ">15.9</td><td class="shotOnTargetPerGame   ">5.6</td><td class="dribbleWonPerGame   ">8.5</td><td class="foulGivenPerGame   ">8.4</td><td class=" "><span class="stat-value rating">6.75</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/167/Show/Angleterre-Manchester-City">3. Manchester City</a></td> <td class="shotsPerGame   sorted  ">15.8</td><td class="shotOnTargetPerGame   ">5.8</td><td class="dribbleWonPerGame   ">8.8</td><td class="foulGivenPerGame   ">10.1</td><td class=" "><span class="stat-value rating">6.90</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/13/Show/Angleterre-Arsenal">4. Arsenal</a></td> <td class="shotsPerGame   sorted  ">15.6</td><td class="shotOnTargetPerGame   ">5.4</td><td class="dribbleWonPerGame   ">9.4</td><td class="foulGivenPerGame   ">11.4</td><td class=" "><span class="stat-value rating">6.81</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/32/Show/Angleterre-Manchester-United">5. Manchester United</a></td> <td class="shotsPerGame   sorted  ">15.6</td><td class="shotOnTargetPerGame   ">5.7</td><td class="dribbleWonPerGame   ">8.4</td><td class="foulGivenPerGame   ">7.8</td><td class=" "><span class="stat-value rating">6.77</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/23/Show/Angleterre-Newcastle">6. Newcastle</a></td> <td class="shotsPerGame   sorted  ">15</td><td class="shotOnTargetPerGame   ">5.2</td><td class="dribbleWonPerGame   ">9.2</td><td class="foulGivenPerGame   ">10.5</td><td class=" "><span class="stat-value rating">6.79</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/30/Show/Angleterre-Tottenham">7. Tottenham</a></td> <td class="shotsPerGame   sorted  ">13.6</td><td class="shotOnTargetPerGame   ">5.2</td><td class="dribbleWonPerGame   ">8.1</td><td class="foulGivenPerGame   ">9.3</td><td class=" "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/15/Show/Angleterre-Chelsea">8. Chelsea</a></td> <td class="shotsPerGame   sorted  ">12.7</td><td class="shotOnTargetPerGame   ">4.2</td><td class="dribbleWonPerGame   ">10</td><td class="foulGivenPerGame   ">12.2</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/29/Show/Angleterre-West-Ham">9. West Ham</a></td> <td class="shotsPerGame   sorted  ">12.5</td><td class="shotOnTargetPerGame   ">3.8</td><td class="dribbleWonPerGame   ">6.6</td><td class="foulGivenPerGame   ">8.3</td><td class=" "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/19/Show/Angleterre-Leeds">10. Leeds</a></td> <td class="shotsPerGame   sorted  ">12.2</td><td class="shotOnTargetPerGame   ">3.8</td><td class="dribbleWonPerGame   ">7.1</td><td class="foulGivenPerGame   ">10.7</td><td class=" "><span class="stat-value rating">6.50</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/24/Show/Angleterre-Aston-Villa">11. Aston Villa</a></td> <td class="shotsPerGame   sorted  ">11.3</td><td class="shotOnTargetPerGame   ">4</td><td class="dribbleWonPerGame   ">7.7</td><td class="foulGivenPerGame   ">13.1</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/31/Show/Angleterre-Everton">12. Everton</a></td> <td class="shotsPerGame   sorted  ">11.3</td><td class="shotOnTargetPerGame   ">4</td><td class="dribbleWonPerGame   ">8</td><td class="foulGivenPerGame   ">10.1</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/170/Show/Angleterre-Fulham">13. Fulham</a></td> <td class="shotsPerGame   sorted  ">11.3</td><td class="shotOnTargetPerGame   ">3.9</td><td class="dribbleWonPerGame   ">7.2</td><td class="foulGivenPerGame   ">10.7</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/162/Show/Angleterre-Crystal-Palace">14. Crystal Palace</a></td> <td class="shotsPerGame   sorted  ">11.2</td><td class="shotOnTargetPerGame   ">3.6</td><td class="dribbleWonPerGame   ">9.7</td><td class="foulGivenPerGame   ">12.6</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/14/Show/Angleterre-Leicester">15. Leicester</a></td> <td class="shotsPerGame   sorted  ">11</td><td class="shotOnTargetPerGame   ">4</td><td class="dribbleWonPerGame   ">7.7</td><td class="foulGivenPerGame   ">10.4</td><td class=" "><span class="stat-value rating">6.57</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/18/Show/Angleterre-Southampton">16. Southampton</a></td> <td class="shotsPerGame   sorted  ">11</td><td class="shotOnTargetPerGame   ">3.7</td><td class="dribbleWonPerGame   ">9.5</td><td class="foulGivenPerGame   ">9.9</td><td class=" "><span class="stat-value rating">6.50</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/161/Show/Angleterre-Wolves">17. Wolves</a></td> <td class="shotsPerGame   sorted  ">10.8</td><td class="shotOnTargetPerGame   ">3.3</td><td class="dribbleWonPerGame   ">9.9</td><td class="foulGivenPerGame   ">10.2</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/189/Show/Angleterre-Brentford">18. Brentford</a></td> <td class="shotsPerGame   sorted  ">10.7</td><td class="shotOnTargetPerGame   ">4.3</td><td class="dribbleWonPerGame   ">6.9</td><td class="foulGivenPerGame   ">10.5</td><td class=" "><span class="stat-value rating">6.66</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/174/Show/Angleterre-Nottingham-Forest">19. Nottingham Forest</a></td> <td class="shotsPerGame   sorted  ">9.7</td><td class="shotOnTargetPerGame   ">3.1</td><td class="dribbleWonPerGame   ">6.7</td><td class="foulGivenPerGame   ">10.3</td><td class=" "><span class="stat-value rating">6.49</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/183/Show/Angleterre-Bournemouth">20. Bournemouth</a></td> <td class="shotsPerGame   sorted  ">9.4</td><td class="shotOnTargetPerGame   ">3.5</td><td class="dribbleWonPerGame   ">8.4</td><td class="foulGivenPerGame   ">9.6</td><td class=" "><span class="stat-value rating">6.53</span></td></tr></tbody></table></div></div>
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
df.insert(0, "Journée", "38")

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
collection_name = 'Stats_All_Team_off_22-23_PL'

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
