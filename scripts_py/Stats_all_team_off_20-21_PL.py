from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import csv

html_content = """
<div id="statistics-team-table-offensive" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable shotsPerGame   desc  " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable shotOnTargetPerGame   " data-stat-name="shotOnTargetPerGame">Tirs CA pm</th><th class="global sortable dribbleWonPerGame   " data-stat-name="dribbleWonPerGame">Dribbles pm</th><th class="global sortable foulGivenPerGame   " data-stat-name="foulGivenPerGame">Fautes subies pm</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/26/Show/Angleterre-Liverpool">1. Liverpool</a></td> <td class="shotsPerGame   sorted  ">16</td><td class="shotOnTargetPerGame   ">5.6</td><td class="dribbleWonPerGame   ">10.7</td><td class="foulGivenPerGame   ">8.3</td><td class=" "><span class="stat-value rating">6.82</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/167/Show/Angleterre-Manchester-City">2. Manchester City</a></td> <td class="shotsPerGame   sorted  ">15.4</td><td class="shotOnTargetPerGame   ">5.5</td><td class="dribbleWonPerGame   ">12.5</td><td class="foulGivenPerGame   ">9.7</td><td class=" "><span class="stat-value rating">6.99</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/15/Show/Angleterre-Chelsea">3. Chelsea</a></td> <td class="shotsPerGame   sorted  ">14.6</td><td class="shotOnTargetPerGame   ">5.5</td><td class="dribbleWonPerGame   ">9.4</td><td class="foulGivenPerGame   ">9.6</td><td class=" "><span class="stat-value rating">6.83</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/32/Show/Angleterre-Manchester-United">4. Manchester United</a></td> <td class="shotsPerGame   sorted  ">13.8</td><td class="shotOnTargetPerGame   ">5.6</td><td class="dribbleWonPerGame   ">10.9</td><td class="foulGivenPerGame   ">10.5</td><td class=" "><span class="stat-value rating">6.85</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/19/Show/Angleterre-Leeds">5. Leeds</a></td> <td class="shotsPerGame   sorted  ">13.7</td><td class="shotOnTargetPerGame   ">5.2</td><td class="dribbleWonPerGame   ">8.4</td><td class="foulGivenPerGame   ">10.8</td><td class=" "><span class="stat-value rating">6.77</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/24/Show/Angleterre-Aston-Villa">6. Aston Villa</a></td> <td class="shotsPerGame   sorted  ">13.7</td><td class="shotOnTargetPerGame   ">4.9</td><td class="dribbleWonPerGame   ">9.7</td><td class="foulGivenPerGame   ">14.6</td><td class=" "><span class="stat-value rating">6.84</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/211/Show/Angleterre-Brighton">7. Brighton</a></td> <td class="shotsPerGame   sorted  ">12.8</td><td class="shotOnTargetPerGame   ">3.8</td><td class="dribbleWonPerGame   ">9.2</td><td class="foulGivenPerGame   ">9.4</td><td class=" "><span class="stat-value rating">6.65</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/14/Show/Angleterre-Leicester">8. Leicester</a></td> <td class="shotsPerGame   sorted  ">12.8</td><td class="shotOnTargetPerGame   ">4.9</td><td class="dribbleWonPerGame   ">9.4</td><td class="foulGivenPerGame   ">11.1</td><td class=" "><span class="stat-value rating">6.80</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/29/Show/Angleterre-West-Ham">9. West Ham</a></td> <td class="shotsPerGame   sorted  ">12.3</td><td class="shotOnTargetPerGame   ">4.3</td><td class="dribbleWonPerGame   ">7.8</td><td class="foulGivenPerGame   ">9.3</td><td class=" "><span class="stat-value rating">6.77</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/13/Show/Angleterre-Arsenal">10. Arsenal</a></td> <td class="shotsPerGame   sorted  ">12.1</td><td class="shotOnTargetPerGame   ">4</td><td class="dribbleWonPerGame   ">7.8</td><td class="foulGivenPerGame   ">10.5</td><td class=" "><span class="stat-value rating">6.69</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/161/Show/Angleterre-Wolves">11. Wolves</a></td> <td class="shotsPerGame   sorted  ">12</td><td class="shotOnTargetPerGame   ">4</td><td class="dribbleWonPerGame   ">12.2</td><td class="foulGivenPerGame   ">9.9</td><td class=" "><span class="stat-value rating">6.66</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/30/Show/Angleterre-Tottenham">12. Tottenham</a></td> <td class="shotsPerGame   sorted  ">11.7</td><td class="shotOnTargetPerGame   ">4.6</td><td class="dribbleWonPerGame   ">10.6</td><td class="foulGivenPerGame   ">12.3</td><td class=" "><span class="stat-value rating">6.81</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/170/Show/Angleterre-Fulham">13. Fulham</a></td> <td class="shotsPerGame   sorted  ">11.6</td><td class="shotOnTargetPerGame   ">3.6</td><td class="dribbleWonPerGame   ">13.3</td><td class="foulGivenPerGame   ">9.8</td><td class=" "><span class="stat-value rating">6.63</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/18/Show/Angleterre-Southampton">14. Southampton</a></td> <td class="shotsPerGame   sorted  ">11.2</td><td class="shotOnTargetPerGame   ">4.4</td><td class="dribbleWonPerGame   ">9</td><td class="foulGivenPerGame   ">11.1</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/31/Show/Angleterre-Everton">15. Everton</a></td> <td class="shotsPerGame   sorted  ">10.5</td><td class="shotOnTargetPerGame   ">3.9</td><td class="dribbleWonPerGame   ">9.5</td><td class="foulGivenPerGame   ">10.3</td><td class=" "><span class="stat-value rating">6.73</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/23/Show/Angleterre-Newcastle">16. Newcastle</a></td> <td class="shotsPerGame   sorted  ">10.4</td><td class="shotOnTargetPerGame   ">3.7</td><td class="dribbleWonPerGame   ">9.2</td><td class="foulGivenPerGame   ">11.1</td><td class=" "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/184/Show/Angleterre-Burnley">17. Burnley</a></td> <td class="shotsPerGame   sorted  ">10.1</td><td class="shotOnTargetPerGame   ">3.4</td><td class="dribbleWonPerGame   ">6.2</td><td class="foulGivenPerGame   ">10.2</td><td class=" "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/162/Show/Angleterre-Crystal-Palace">18. Crystal Palace</a></td> <td class="shotsPerGame   sorted  ">9.2</td><td class="shotOnTargetPerGame   ">3.5</td><td class="dribbleWonPerGame   ">9.6</td><td class="foulGivenPerGame   ">11.5</td><td class=" "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/175/Show/Angleterre-West-Bromwich-Albion">19. West Bromwich Albion</a></td> <td class="shotsPerGame   sorted  ">8.9</td><td class="shotOnTargetPerGame   ">2.9</td><td class="dribbleWonPerGame   ">6.9</td><td class="foulGivenPerGame   ">10.8</td><td class=" "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/163/Show/Angleterre-Sheffield-United">20. Sheffield United</a></td> <td class="shotsPerGame   sorted  ">8.5</td><td class="shotOnTargetPerGame   ">2.6</td><td class="dribbleWonPerGame   ">7.1</td><td class="foulGivenPerGame   ">7.7</td><td class=" "><span class="stat-value rating">6.46</span></td></tr></tbody></table></div></div>
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
collection_name = 'Stats_All_Team_off_20-21_PL'

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