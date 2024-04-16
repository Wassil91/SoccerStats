from bs4 import BeautifulSoup
import csv
from pymongo import MongoClient
import pandas as pd

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/65/Show/Espagne-Barcelona">1. Barcelona</a></td> <td class="goal   ">85</td><td class="shotsPerGame   ">15.3</td><td class="aaa"><span class="yellow-card-box">68</span><span class="red-card-box">2</span></td><td class="possession   ">36.9</td><td class="passSuccess   ">89.7</td><td class="aerialWonPerGame   ">10.6</td><td class="  sorted "><span class="stat-value rating">6.87</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/52/Show/Espagne-Real-Madrid">2. Real Madrid</a></td> <td class="goal   ">67</td><td class="shotsPerGame   ">14.4</td><td class="aaa"><span class="yellow-card-box">57</span><span class="red-card-box">2</span></td><td class="possession   ">57.7</td><td class="passSuccess   ">87.7</td><td class="aerialWonPerGame   ">11.8</td><td class="  sorted "><span class="stat-value rating">6.86</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/63/Show/Espagne-Atletico-Madrid">3. Atletico Madrid</a></td> <td class="goal   ">67</td><td class="shotsPerGame   ">12.1</td><td class="aaa"><span class="yellow-card-box">100</span><span class="red-card-box">0</span></td><td class="possession   ">51.8</td><td class="passSuccess   ">83.1</td><td class="aerialWonPerGame   ">14.4</td><td class="  sorted "><span class="stat-value rating">6.84</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/67/Show/Espagne-Sevilla">4. Sevilla</a></td> <td class="goal   ">53</td><td class="shotsPerGame   ">12.1</td><td class="aaa"><span class="yellow-card-box">75</span><span class="red-card-box">2</span></td><td class="possession   ">58.7</td><td class="passSuccess   ">86.2</td><td class="aerialWonPerGame   ">16.6</td><td class="  sorted "><span class="stat-value rating">6.70</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/839/Show/Espagne-Villarreal">5. Villarreal</a></td> <td class="goal   ">60</td><td class="shotsPerGame   ">10.7</td><td class="aaa"><span class="yellow-card-box">65</span><span class="red-card-box">5</span></td><td class="possession   ">54.3</td><td class="passSuccess   ">84.4</td><td class="aerialWonPerGame   ">13</td><td class="  sorted "><span class="stat-value rating">6.66</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/68/Show/Espagne-Real-Sociedad">6. Real Sociedad</a></td> <td class="goal   ">59</td><td class="shotsPerGame   ">11.3</td><td class="aaa"><span class="yellow-card-box">81</span><span class="red-card-box">1</span></td><td class="possession   ">53.7</td><td class="passSuccess   ">80.8</td><td class="aerialWonPerGame   ">17.9</td><td class="  sorted "><span class="stat-value rating">6.65</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/54/Show/Espagne-Real-Betis">7. Real Betis</a></td> <td class="goal   ">50</td><td class="shotsPerGame   ">11.7</td><td class="aaa"><span class="yellow-card-box">87</span><span class="red-card-box">8</span></td><td class="possession   ">52.9</td><td class="passSuccess   ">82.0</td><td class="aerialWonPerGame   ">16.4</td><td class="  sorted "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/55/Show/Espagne-Valencia">8. Valencia</a></td> <td class="goal   ">50</td><td class="shotsPerGame   ">10.3</td><td class="aaa"><span class="yellow-card-box">77</span><span class="red-card-box">5</span></td><td class="possession   ">47.9</td><td class="passSuccess   ">79.4</td><td class="aerialWonPerGame   ">16.3</td><td class="  sorted "><span class="stat-value rating">6.60</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/62/Show/Espagne-Celta-Vigo">9. Celta Vigo</a></td> <td class="goal   ">55</td><td class="shotsPerGame   ">9.4</td><td class="aaa"><span class="yellow-card-box">104</span><span class="red-card-box">5</span></td><td class="possession   ">52.0</td><td class="passSuccess   ">79.9</td><td class="aerialWonPerGame   ">16.5</td><td class="  sorted "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/5006/Show/Espagne-SD-Huesca">10. SD Huesca</a></td> <td class="goal   ">34</td><td class="shotsPerGame   ">10.7</td><td class="aaa"><span class="yellow-card-box">68</span><span class="red-card-box">2</span></td><td class="possession   ">48.7</td><td class="passSuccess   ">79.8</td><td class="aerialWonPerGame   ">15.7</td><td class="  sorted "><span class="stat-value rating">6.55</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/53/Show/Espagne-Athletic-Club">11. Athletic Club</a></td> <td class="goal   ">46</td><td class="shotsPerGame   ">10.6</td><td class="aaa"><span class="yellow-card-box">81</span><span class="red-card-box">3</span></td><td class="possession   ">49.4</td><td class="passSuccess   ">78.5</td><td class="aerialWonPerGame   ">17.9</td><td class="  sorted "><span class="stat-value rating">6.54</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/131/Show/Espagne-Osasuna">12. Osasuna</a></td> <td class="goal   ">37</td><td class="shotsPerGame   ">9.8</td><td class="aaa"><span class="yellow-card-box">74</span><span class="red-card-box">5</span></td><td class="possession   ">44.6</td><td class="passSuccess   ">70.2</td><td class="aerialWonPerGame   ">26.8</td><td class="  sorted "><span class="stat-value rating">6.54</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/824/Show/Espagne-Eibar">13. Eibar</a></td> <td class="goal   ">29</td><td class="shotsPerGame   ">11.9</td><td class="aaa"><span class="yellow-card-box">67</span><span class="red-card-box">3</span></td><td class="possession   ">49.2</td><td class="passSuccess   ">72.6</td><td class="aerialWonPerGame   ">24.4</td><td class="  sorted "><span class="stat-value rating">6.53</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/60/Show/Espagne-Deportivo-Alaves">14. Deportivo Alaves</a></td> <td class="goal   ">36</td><td class="shotsPerGame   ">9.1</td><td class="aaa"><span class="yellow-card-box">87</span><span class="red-card-box">8</span></td><td class="possession   ">44.6</td><td class="passSuccess   ">72.9</td><td class="aerialWonPerGame   ">22.6</td><td class="  sorted "><span class="stat-value rating">6.50</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/832/Show/Espagne-Levante">15. Levante</a></td> <td class="goal   ">46</td><td class="shotsPerGame   ">10.1</td><td class="aaa"><span class="yellow-card-box">70</span><span class="red-card-box">1</span></td><td class="possession   ">51.5</td><td class="passSuccess   ">80.1</td><td class="aerialWonPerGame   ">12.2</td><td class="  sorted "><span class="stat-value rating">6.50</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/819/Show/Espagne-Getafe">16. Getafe</a></td> <td class="goal   ">28</td><td class="shotsPerGame   ">9.5</td><td class="aaa"><span class="yellow-card-box">117</span><span class="red-card-box">7</span></td><td class="possession   ">8.9</td><td class="passSuccess   ">66.5</td><td class="aerialWonPerGame   ">22.8</td><td class="  sorted "><span class="stat-value rating">6.49</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/833/Show/Espagne-Elche">17. Elche</a></td> <td class="goal   ">34</td><td class="shotsPerGame   ">7.1</td><td class="aaa"><span class="yellow-card-box">95</span><span class="red-card-box">3</span></td><td class="possession   ">48.1</td><td class="passSuccess   ">81.5</td><td class="aerialWonPerGame   ">13.2</td><td class="  sorted "><span class="stat-value rating">6.48</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/925/Show/Espagne-Granada">18. Granada</a></td> <td class="goal   ">47</td><td class="shotsPerGame   ">9.4</td><td class="aaa"><span class="yellow-card-box">96</span><span class="red-card-box">6</span></td><td class="possession   ">43.4</td><td class="passSuccess   ">70.0</td><td class="aerialWonPerGame   ">18.3</td><td class="  sorted "><span class="stat-value rating">6.47</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1354/Show/Espagne-Cadiz">19. Cadiz</a></td> <td class="goal   ">36</td><td class="shotsPerGame   ">8</td><td class="aaa"><span class="yellow-card-box">77</span><span class="red-card-box">3</span></td><td class="possession   ">38.5</td><td class="passSuccess   ">68.8</td><td class="aerialWonPerGame   ">18.5</td><td class="  sorted "><span class="stat-value rating">6.47</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/58/Show/Espagne-Real-Valladolid">20. Real Valladolid</a></td> <td class="goal   ">34</td><td class="shotsPerGame   ">9.7</td><td class="aaa"><span class="yellow-card-box">93</span><span class="red-card-box">4</span></td><td class="possession   ">46.2</td><td class="passSuccess   ">74.8</td><td class="aerialWonPerGame   ">17.1</td><td class="  sorted "><span class="stat-value rating">6.46</span></td></tr></tbody></table></div></div>
"""
soup = BeautifulSoup(html_content, "html.parser")

data = []
headers = ['Équipe', 'Buts', 'Tirs pm', 'Discipline', 'Possession%', 'PassesRéussies%', 'AériensGagnés', 'Note']

table_body = soup.find("tbody", {"id": "top-team-stats-summary-content"})
rows = table_body.find_all("tr")

with open('football_stats_modified_20-21.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Équipe', 'Buts', 'Tirs pm', 'Discipline', 'Possession%', 'PassesRéussies%', 'AériensGagnés', 'Note'])

    for row in rows:
        cells = row.find_all("td")
        team_name = cells[0].text.strip().split(". ")[1]
        goals = cells[1].text.strip()
        shots_per_game = cells[2].text.strip()
        discipline = cells[3].text.strip()
        possession = cells[4].text.strip()
        passes_success = cells[5].text.strip()
        aerials_won = cells[6].text.strip()
        rating = cells[7].text.strip()
        writer.writerow([team_name, goals, shots_per_game, discipline, possession, passes_success, aerials_won, rating])

print("Les données ont été enregistrées dans football_stats_modified_20-21.csv avec succès !")

# Lire le fichier CSV dans un DataFrame
df = pd.read_csv("football_stats_modified_20-21.csv")

# Supprimer les colonnes "Note" et "Discipline"
df.drop(columns=["Note", "Discipline"], inplace=True)

# Ajouter une colonne "Journée" avec la valeur "26"
df.insert(0, "Journée", 38)

# Supprimer la colonne "Position" si elle existe déjà
if "Position" in df.columns:
    df.drop(columns=["Position"], inplace=True)

# Insérer une nouvelle colonne "Position" à l'index 1 (après la colonne "Journée")
df.insert(1, "Position", range(1, len(df) + 1))

# Convertir les types des colonnes "Journée" et "Buts"
df['Journée'] = df['Journée'].astype(int)
df['Buts'] = df['Buts'].astype(int)

# Enregistrer le DataFrame dans un fichier CSV (avec les modifications)
df.to_csv("football_stats_modified_20-21.csv", index=False)

print("Les données ont été extraites et enregistrées dans football_stats_modified_20-21.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_General_20-21_Liga'

# Supprimer la collection si elle existe
if collection_name in db.list_collection_names():
    db.drop_collection(collection_name)
    print("La collection existe. Elle a été supprimée.")

# Recréer la collection
collection = db[collection_name]


# Convertir le DataFrame en dictionnaire pour l'insertion dans MongoDB
data_dict = df.to_dict(orient='records')

# Insérer les données dans la collection MongoDB
collection.insert_many(data_dict)

print("Les données ont été insérées dans la collection MongoDB avec succès.")