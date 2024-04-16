from bs4 import BeautifulSoup
import csv
from pymongo import MongoClient
import pandas as pd

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/52/Show/Espagne-Real-Madrid">1. Real Madrid</a></td> <td class="goal   ">64</td><td class="shotsPerGame   ">16.4</td><td class="aaa"><span class="yellow-card-box">52</span><span class="red-card-box">4</span></td><td class="possession   ">59.6</td><td class="passSuccess   ">90.1</td><td class="aerialWonPerGame   ">8.7</td><td class="  sorted "><span class="stat-value rating">6.89</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/65/Show/Espagne-Barcelona">2. Barcelona</a></td> <td class="goal   ">60</td><td class="shotsPerGame   ">15.7</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">2</span></td><td class="possession   ">64.8</td><td class="passSuccess   ">88.3</td><td class="aerialWonPerGame   ">13</td><td class="  sorted "><span class="stat-value rating">6.83</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2783/Show/Espagne-Girona">3. Girona</a></td> <td class="goal   ">59</td><td class="shotsPerGame   ">12.7</td><td class="aaa"><span class="yellow-card-box">60</span><span class="red-card-box">1</span></td><td class="possession   ">57.0</td><td class="passSuccess   ">87.4</td><td class="aerialWonPerGame   ">10.6</td><td class="  sorted "><span class="stat-value rating">6.74</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/53/Show/Espagne-Athletic-Club">4. Athletic Club</a></td> <td class="goal   ">50</td><td class="shotsPerGame   ">12.7</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">3</span></td><td class="possession   ">48.9</td><td class="passSuccess   ">78.8</td><td class="aerialWonPerGame   ">15.3</td><td class="  sorted "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/63/Show/Espagne-Atletico-Madrid">5. Atletico Madrid</a></td> <td class="goal   ">54</td><td class="shotsPerGame   ">12.6</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">4</span></td><td class="possession   ">51.2</td><td class="passSuccess   ">84.6</td><td class="aerialWonPerGame   ">11.8</td><td class="  sorted "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/54/Show/Espagne-Real-Betis">6. Real Betis</a></td> <td class="goal   ">34</td><td class="shotsPerGame   ">13.2</td><td class="aaa"><span class="yellow-card-box">67</span><span class="red-card-box">3</span></td><td class="possession   ">50.5</td><td class="passSuccess   ">82.9</td><td class="aerialWonPerGame   ">13</td><td class="  sorted "><span class="stat-value rating">6.66</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/68/Show/Espagne-Real-Sociedad">7. Real Sociedad</a></td> <td class="goal   ">42</td><td class="shotsPerGame   ">12.7</td><td class="aaa"><span class="yellow-card-box">77</span><span class="red-card-box">1</span></td><td class="possession   ">56.7</td><td class="passSuccess   ">81.8</td><td class="aerialWonPerGame   ">21.7</td><td class="  sorted "><span class="stat-value rating">6.66</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/839/Show/Espagne-Villarreal">8. Villarreal</a></td> <td class="goal   ">47</td><td class="shotsPerGame   ">11.8</td><td class="aaa"><span class="yellow-card-box">81</span><span class="red-card-box">5</span></td><td class="possession   ">50.1</td><td class="passSuccess   ">83.7</td><td class="aerialWonPerGame   ">14.1</td><td class="  sorted "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/67/Show/Espagne-Sevilla">9. Sevilla</a></td> <td class="goal   ">36</td><td class="shotsPerGame   ">13.5</td><td class="aaa"><span class="yellow-card-box">79</span><span class="red-card-box">4</span></td><td class="possession   ">51.2</td><td class="passSuccess   ">81.3</td><td class="aerialWonPerGame   ">17.1</td><td class="  sorted "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/62/Show/Espagne-Celta-Vigo">10. Celta Vigo</a></td> <td class="goal   ">32</td><td class="shotsPerGame   ">12.6</td><td class="aaa"><span class="yellow-card-box">47</span><span class="red-card-box">5</span></td><td class="possession   ">44.0</td><td class="passSuccess   ">78.2</td><td class="aerialWonPerGame   ">15.3</td><td class="  sorted "><span class="stat-value rating">6.57</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/838/Show/Espagne-Las-Palmas">11. Las Palmas</a></td> <td class="goal   ">29</td><td class="shotsPerGame   ">10.7</td><td class="aaa"><span class="yellow-card-box">67</span><span class="red-card-box">3</span></td><td class="possession   ">61.0</td><td class="passSuccess   ">85.5</td><td class="aerialWonPerGame   ">11.3</td><td class="  sorted "><span class="stat-value rating">6.57</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/55/Show/Espagne-Valencia">12. Valencia</a></td> <td class="goal   ">32</td><td class="shotsPerGame   ">9.8</td><td class="aaa"><span class="yellow-card-box">49</span><span class="red-card-box">4</span></td><td class="possession   ">43.8</td><td class="passSuccess   ">77.2</td><td class="aerialWonPerGame   ">12.8</td><td class="  sorted "><span class="stat-value rating">6.56</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/51/Show/Espagne-Mallorca">13. Mallorca</a></td> <td class="goal   ">25</td><td class="shotsPerGame   ">11.1</td><td class="aaa"><span class="yellow-card-box">80</span><span class="red-card-box">4</span></td><td class="possession   ">43.4</td><td class="passSuccess   ">74.4</td><td class="aerialWonPerGame   ">21.6</td><td class="  sorted "><span class="stat-value rating">6.55</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/819/Show/Espagne-Getafe">14. Getafe</a></td> <td class="goal   ">37</td><td class="shotsPerGame   ">11.6</td><td class="aaa"><span class="yellow-card-box">101</span><span class="red-card-box">9</span></td><td class="possession   ">42.5</td><td class="passSuccess   ">71.0</td><td class="aerialWonPerGame   ">19</td><td class="  sorted "><span class="stat-value rating">6.54</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/60/Show/Espagne-Deportivo-Alaves">15. Deportivo Alaves</a></td> <td class="goal   ">26</td><td class="shotsPerGame   ">12.4</td><td class="aaa"><span class="yellow-card-box">59</span><span class="red-card-box">1</span></td><td class="possession   ">41.2</td><td class="passSuccess   ">74.3</td><td class="aerialWonPerGame   ">15</td><td class="  sorted "><span class="stat-value rating">6.50</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/131/Show/Espagne-Osasuna">16. Osasuna</a></td> <td class="goal   ">33</td><td class="shotsPerGame   ">11.3</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">4</span></td><td class="possession   ">47.3</td><td class="passSuccess   ">76.8</td><td class="aerialWonPerGame   ">20.6</td><td class="  sorted "><span class="stat-value rating">6.50</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/64/Show/Espagne-Rayo-Vallecano">17. Rayo Vallecano</a></td> <td class="goal   ">25</td><td class="shotsPerGame   ">12</td><td class="aaa"><span class="yellow-card-box">77</span><span class="red-card-box">4</span></td><td class="possession   ">48.8</td><td class="passSuccess   ">77.6</td><td class="aerialWonPerGame   ">12.9</td><td class="  sorted "><span class="stat-value rating">6.47</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1354/Show/Espagne-Cadiz">18. Cadiz</a></td> <td class="goal   ">21</td><td class="shotsPerGame   ">10</td><td class="aaa"><span class="yellow-card-box">86</span><span class="red-card-box">6</span></td><td class="possession   ">42.1</td><td class="passSuccess   ">72.9</td><td class="aerialWonPerGame   ">17.4</td><td class="  sorted "><span class="stat-value rating">6.45</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1799/Show/Espagne-Almeria">19. Almeria</a></td> <td class="goal   ">28</td><td class="shotsPerGame   ">12</td><td class="aaa"><span class="yellow-card-box">67</span><span class="red-card-box">4</span></td><td class="possession   ">45.5</td><td class="passSuccess   ">78.2</td><td class="aerialWonPerGame   ">15.8</td><td class="  sorted "><span class="stat-value rating">6.45</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/925/Show/Espagne-Granada">20. Granada</a></td> <td class="goal   ">30</td><td class="shotsPerGame   ">10.8</td><td class="aaa"><span class="yellow-card-box">72</span><span class="red-card-box">3</span></td><td class="possession   ">45.6</td><td class="passSuccess   ">77.9</td><td class="aerialWonPerGame   ">12.5</td><td class="  sorted "><span class="stat-value rating">6.40</span></td></tr></tbody></table></div></div>
"""
soup = BeautifulSoup(html_content, "html.parser")

data = []
headers = ['Équipe', 'Buts', 'Tirs pm', 'Discipline', 'Possession%', 'PassesRéussies%', 'AériensGagnés', 'Note']

table_body = soup.find("tbody", {"id": "top-team-stats-summary-content"})
rows = table_body.find_all("tr")

with open('football_stats_liga.csv', mode='w', newline='', encoding='utf-8') as file:
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

print("Les données ont été enregistrées dans football_stats.csv avec succès !")

# Lire le fichier CSV dans un DataFrame
df = pd.read_csv("football_stats_liga.csv")

# Supprimer les colonnes "Note" et "Discipline"
df.drop(columns=["Note", "Discipline"], inplace=True)

# Ajouter une colonne "Journée" avec la valeur "26"
df.insert(0, "Journée", 29)

# Supprimer la colonne "Position" si elle existe déjà
if "Position" in df.columns:
    df.drop(columns=["Position"], inplace=True)

# Insérer une nouvelle colonne "Position" à l'index 1 (après la colonne "Journée")
df.insert(1, "Position", range(1, len(df) + 1))

# Convertir les types des colonnes "Journée" et "Buts"
df['Journée'] = df['Journée'].astype(int)
df['Buts'] = df['Buts'].astype(int)

# Enregistrer le DataFrame dans un fichier CSV (avec les modifications)
df.to_csv("football_stats_modified.csv", index=False)

print("Les données ont été extraites et enregistrées dans football_stats_liga.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_General_Liga'

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