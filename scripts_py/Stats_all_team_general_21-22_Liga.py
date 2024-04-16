from bs4 import BeautifulSoup
import csv
from pymongo import MongoClient
import pandas as pd

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/52/Show/Espagne-Real-Madrid">1. Real Madrid</a></td> <td class="goal   ">80</td><td class="shotsPerGame   ">17.3</td><td class="aaa"><span class="yellow-card-box">76</span><span class="red-card-box">0</span></td><td class="possession   ">60.1</td><td class="passSuccess   ">89.0</td><td class="aerialWonPerGame   ">10.5</td><td class="  sorted "><span class="stat-value rating">6.90</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/65/Show/Espagne-Barcelona">2. Barcelona</a></td> <td class="goal   ">68</td><td class="shotsPerGame   ">13.6</td><td class="aaa"><span class="yellow-card-box">91</span><span class="red-card-box">5</span></td><td class="possession   ">64.7</td><td class="passSuccess   ">88.3</td><td class="aerialWonPerGame   ">12.2</td><td class="  sorted "><span class="stat-value rating">6.77</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/54/Show/Espagne-Real-Betis">3. Real Betis</a></td> <td class="goal   ">62</td><td class="shotsPerGame   ">13.6</td><td class="aaa"><span class="yellow-card-box">85</span><span class="red-card-box">6</span></td><td class="possession   ">53.8</td><td class="passSuccess   ">82.5</td><td class="aerialWonPerGame   ">13.4</td><td class="  sorted "><span class="stat-value rating">6.72</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/839/Show/Espagne-Villarreal">4. Villarreal</a></td> <td class="goal   ">63</td><td class="shotsPerGame   ">12</td><td class="aaa"><span class="yellow-card-box">79</span><span class="red-card-box">1</span></td><td class="possession   ">56.8</td><td class="passSuccess   ">83.6</td><td class="aerialWonPerGame   ">13</td><td class="  sorted "><span class="stat-value rating">6.72</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/63/Show/Espagne-Atletico-Madrid">5. Atletico Madrid</a></td> <td class="goal   ">65</td><td class="shotsPerGame   ">12.2</td><td class="aaa"><span class="yellow-card-box">106</span><span class="red-card-box">7</span></td><td class="possession   ">50.2</td><td class="passSuccess   ">81.1</td><td class="aerialWonPerGame   ">15</td><td class="  sorted "><span class="stat-value rating">6.68</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/67/Show/Espagne-Sevilla">6. Sevilla</a></td> <td class="goal   ">53</td><td class="shotsPerGame   ">11.4</td><td class="aaa"><span class="yellow-card-box">94</span><span class="red-card-box">4</span></td><td class="possession   ">59.9</td><td class="passSuccess   ">84.7</td><td class="aerialWonPerGame   ">15.7</td><td class="  sorted "><span class="stat-value rating">6.65</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/68/Show/Espagne-Real-Sociedad">7. Real Sociedad</a></td> <td class="goal   ">40</td><td class="shotsPerGame   ">11.6</td><td class="aaa"><span class="yellow-card-box">76</span><span class="red-card-box">4</span></td><td class="possession   ">54.6</td><td class="passSuccess   ">81.8</td><td class="aerialWonPerGame   ">18.1</td><td class="  sorted "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/62/Show/Espagne-Celta-Vigo">8. Celta Vigo</a></td> <td class="goal   ">43</td><td class="shotsPerGame   ">10.8</td><td class="aaa"><span class="yellow-card-box">85</span><span class="red-card-box">3</span></td><td class="possession   ">55.4</td><td class="passSuccess   ">79.8</td><td class="aerialWonPerGame   ">16</td><td class="  sorted "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/53/Show/Espagne-Athletic-Club">9. Athletic Club</a></td> <td class="goal   ">43</td><td class="shotsPerGame   ">12.3</td><td class="aaa"><span class="yellow-card-box">84</span><span class="red-card-box">4</span></td><td class="possession   ">47.5</td><td class="passSuccess   ">77.1</td><td class="aerialWonPerGame   ">15.7</td><td class="  sorted "><span class="stat-value rating">6.59</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/64/Show/Espagne-Rayo-Vallecano">10. Rayo Vallecano</a></td> <td class="goal   ">39</td><td class="shotsPerGame   ">13.3</td><td class="aaa"><span class="yellow-card-box">100</span><span class="red-card-box">3</span></td><td class="possession   ">49.4</td><td class="passSuccess   ">77.6</td><td class="aerialWonPerGame   ">17.6</td><td class="  sorted "><span class="stat-value rating">6.56</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/131/Show/Espagne-Osasuna">11. Osasuna</a></td> <td class="goal   ">37</td><td class="shotsPerGame   ">11.4</td><td class="aaa"><span class="yellow-card-box">83</span><span class="red-card-box">2</span></td><td class="possession   ">46.1</td><td class="passSuccess   ">74.4</td><td class="aerialWonPerGame   ">21.7</td><td class="  sorted "><span class="stat-value rating">6.55</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/819/Show/Espagne-Getafe">12. Getafe</a></td> <td class="goal   ">33</td><td class="shotsPerGame   ">10.8</td><td class="aaa"><span class="yellow-card-box">119</span><span class="red-card-box">8</span></td><td class="possession   ">40.4</td><td class="passSuccess   ">72.3</td><td class="aerialWonPerGame   ">19.3</td><td class="  sorted "><span class="stat-value rating">6.55</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/55/Show/Espagne-Valencia">13. Valencia</a></td> <td class="goal   ">48</td><td class="shotsPerGame   ">10.6</td><td class="aaa"><span class="yellow-card-box">123</span><span class="red-card-box">8</span></td><td class="possession   ">43.3</td><td class="passSuccess   ">71.9</td><td class="aerialWonPerGame   ">15.1</td><td class="  sorted "><span class="stat-value rating">6.54</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1354/Show/Espagne-Cadiz">14. Cadiz</a></td> <td class="goal   ">35</td><td class="shotsPerGame   ">10.8</td><td class="aaa"><span class="yellow-card-box">100</span><span class="red-card-box">3</span></td><td class="possession   ">40.5</td><td class="passSuccess   ">73.6</td><td class="aerialWonPerGame   ">16.1</td><td class="  sorted "><span class="stat-value rating">6.53</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/70/Show/Espagne-Espanyol">15. Espanyol</a></td> <td class="goal   ">40</td><td class="shotsPerGame   ">10.7</td><td class="aaa"><span class="yellow-card-box">93</span><span class="red-card-box">7</span></td><td class="possession   ">46.7</td><td class="passSuccess   ">81.1</td><td class="aerialWonPerGame   ">12.1</td><td class="  sorted "><span class="stat-value rating">6.52</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/925/Show/Espagne-Granada">16. Granada</a></td> <td class="goal   ">44</td><td class="shotsPerGame   ">11.2</td><td class="aaa"><span class="yellow-card-box">103</span><span class="red-card-box">4</span></td><td class="possession   ">43.9</td><td class="passSuccess   ">72.9</td><td class="aerialWonPerGame   ">16.2</td><td class="  sorted "><span class="stat-value rating">6.52</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/833/Show/Espagne-Elche">17. Elche</a></td> <td class="goal   ">40</td><td class="shotsPerGame   ">8.8</td><td class="aaa"><span class="yellow-card-box">103</span><span class="red-card-box">6</span></td><td class="possession   ">48.0</td><td class="passSuccess   ">79.2</td><td class="aerialWonPerGame   ">14.3</td><td class="  sorted "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/51/Show/Espagne-Mallorca">18. Mallorca</a></td> <td class="goal   ">36</td><td class="shotsPerGame   ">11.8</td><td class="aaa"><span class="yellow-card-box">105</span><span class="red-card-box">7</span></td><td class="possession   ">44.6</td><td class="passSuccess   ">76.0</td><td class="aerialWonPerGame   ">17.4</td><td class="  sorted "><span class="stat-value rating">6.48</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/60/Show/Espagne-Deportivo-Alaves">19. Deportivo Alaves</a></td> <td class="goal   ">31</td><td class="shotsPerGame   ">10</td><td class="aaa"><span class="yellow-card-box">87</span><span class="red-card-box">4</span></td><td class="possession   ">41.3</td><td class="passSuccess   ">70.3</td><td class="aerialWonPerGame   ">23.1</td><td class="  sorted "><span class="stat-value rating">6.46</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/832/Show/Espagne-Levante">20. Levante</a></td> <td class="goal   ">51</td><td class="shotsPerGame   ">11.7</td><td class="aaa"><span class="yellow-card-box">105</span><span class="red-card-box">6</span></td><td class="possession   ">46.6</td><td class="passSuccess   ">78.3</td><td class="aerialWonPerGame   ">11.9</td><td class="  sorted "><span class="stat-value rating">6.46</span></td></tr></tbody></table></div></div>
"""
soup = BeautifulSoup(html_content, "html.parser")

data = []
headers = ['Équipe', 'Buts', 'Tirs pm', 'Discipline', 'Possession%', 'PassesRéussies%', 'AériensGagnés', 'Note']

table_body = soup.find("tbody", {"id": "top-team-stats-summary-content"})
rows = table_body.find_all("tr")

with open('football_stats_modified_21-22.csv', mode='w', newline='', encoding='utf-8') as file:
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

print("Les données ont été enregistrées dans football_stats_modified_21-22.csv avec succès !")

# Lire le fichier CSV dans un DataFrame
df = pd.read_csv("football_stats_modified_21-22.csv")

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
df.to_csv("football_stats_modified_21-22.csv", index=False)

print("Les données ont été extraites et enregistrées dans football_stats_liga.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_General_21-22_Liga'

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