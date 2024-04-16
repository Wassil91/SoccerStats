from bs4 import BeautifulSoup
import csv
from pymongo import MongoClient
import pandas as pd

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/75/Show/Italie-Inter">1. Inter</a></td> <td class="goal   ">71</td><td class="shotsPerGame   ">15.4</td><td class="aaa"><span class="yellow-card-box">40</span><span class="red-card-box">0</span></td><td class="possession   ">55.5</td><td class="passSuccess   ">87.1</td><td class="aerialWonPerGame   ">14.5</td><td class="  sorted "><span class="stat-value rating">6.88</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/300/Show/Italie-Atalanta">2. Atalanta</a></td> <td class="goal   ">54</td><td class="shotsPerGame   ">13.9</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">1</span></td><td class="possession   ">49.9</td><td class="passSuccess   ">82.1</td><td class="aerialWonPerGame   ">16.3</td><td class="  sorted "><span class="stat-value rating">6.74</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/80/Show/Italie-AC-Milan">3. AC Milan</a></td> <td class="goal   ">55</td><td class="shotsPerGame   ">13.9</td><td class="aaa"><span class="yellow-card-box">65</span><span class="red-card-box">6</span></td><td class="possession   ">56.0</td><td class="passSuccess   ">86.8</td><td class="aerialWonPerGame   ">10.9</td><td class="  sorted "><span class="stat-value rating">6.70</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/87/Show/Italie-Juventus">4. Juventus</a></td> <td class="goal   ">44</td><td class="shotsPerGame   ">14.3</td><td class="aaa"><span class="yellow-card-box">68</span><span class="red-card-box">2</span></td><td class="possession   ">47.9</td><td class="passSuccess   ">83.7</td><td class="aerialWonPerGame   ">13.5</td><td class="  sorted "><span class="stat-value rating">6.69</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/71/Show/Italie-Bologna">5. Bologna</a></td> <td class="goal   ">42</td><td class="shotsPerGame   ">12.8</td><td class="aaa"><span class="yellow-card-box">67</span><span class="red-card-box">1</span></td><td class="possession   ">57.8</td><td class="passSuccess   ">86.1</td><td class="aerialWonPerGame   ">11.7</td><td class="  sorted "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/84/Show/Italie-Roma">6. Roma</a></td> <td class="goal   ">55</td><td class="shotsPerGame   ">11.9</td><td class="aaa"><span class="yellow-card-box">74</span><span class="red-card-box">2</span></td><td class="possession   ">54.3</td><td class="passSuccess   ">85.1</td><td class="aerialWonPerGame   ">13.3</td><td class="  sorted "><span class="stat-value rating">6.64</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/276/Show/Italie-Napoli">7. Napoli</a></td> <td class="goal   ">44</td><td class="shotsPerGame   ">16.9</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">4</span></td><td class="possession   ">61.4</td><td class="passSuccess   ">87.2</td><td class="aerialWonPerGame   ">13</td><td class="  sorted "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/73/Show/Italie-Fiorentina">8. Fiorentina</a></td> <td class="goal   ">41</td><td class="shotsPerGame   ">13.5</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">0</span></td><td class="possession   ">56.5</td><td class="passSuccess   ">83.1</td><td class="aerialWonPerGame   ">16.2</td><td class="  sorted "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/72/Show/Italie-Torino">9. Torino</a></td> <td class="goal   ">29</td><td class="shotsPerGame   ">11.5</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">1</span></td><td class="possession   ">51.9</td><td class="passSuccess   ">81.8</td><td class="aerialWonPerGame   ">16.1</td><td class="  sorted "><span class="stat-value rating">6.58</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/77/Show/Italie-Lazio">10. Lazio</a></td> <td class="goal   ">37</td><td class="shotsPerGame   ">11.5</td><td class="aaa"><span class="yellow-card-box">74</span><span class="red-card-box">5</span></td><td class="possession   ">52.2</td><td class="passSuccess   ">84.8</td><td class="aerialWonPerGame   ">10.4</td><td class="  sorted "><span class="stat-value rating">6.56</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/269/Show/Italie-Monza">11. Monza</a></td> <td class="goal   ">32</td><td class="shotsPerGame   ">12.3</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">4</span></td><td class="possession   ">53.7</td><td class="passSuccess   ">85.7</td><td class="aerialWonPerGame   ">11.2</td><td class="  sorted "><span class="stat-value rating">6.55</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/278/Show/Italie-Genoa">12. Genoa</a></td> <td class="goal   ">32</td><td class="shotsPerGame   ">10.8</td><td class="aaa"><span class="yellow-card-box">66</span><span class="red-card-box">3</span></td><td class="possession   ">43.8</td><td class="passSuccess   ">77.9</td><td class="aerialWonPerGame   ">16.4</td><td class="  sorted "><span class="stat-value rating">6.55</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/86/Show/Italie-Udinese">13. Udinese</a></td> <td class="goal   ">28</td><td class="shotsPerGame   ">12.6</td><td class="aaa"><span class="yellow-card-box">69</span><span class="red-card-box">5</span></td><td class="possession   ">39.9</td><td class="passSuccess   ">78.1</td><td class="aerialWonPerGame   ">14.9</td><td class="  sorted "><span class="stat-value rating">6.52</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/76/Show/Italie-Verona">14. Verona</a></td> <td class="goal   ">26</td><td class="shotsPerGame   ">11.3</td><td class="aaa"><span class="yellow-card-box">64</span><span class="red-card-box">5</span></td><td class="possession   ">44.3</td><td class="passSuccess   ">75.4</td><td class="aerialWonPerGame   ">19.8</td><td class="  sorted "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/79/Show/Italie-Lecce">15. Lecce</a></td> <td class="goal   ">26</td><td class="shotsPerGame   ">13.2</td><td class="aaa"><span class="yellow-card-box">72</span><span class="red-card-box">4</span></td><td class="possession   ">43.5</td><td class="passSuccess   ">78.1</td><td class="aerialWonPerGame   ">14</td><td class="  sorted "><span class="stat-value rating">6.46</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/272/Show/Italie-Empoli">16. Empoli</a></td> <td class="goal   ">22</td><td class="shotsPerGame   ">11.6</td><td class="aaa"><span class="yellow-card-box">66</span><span class="red-card-box">0</span></td><td class="possession   ">44.4</td><td class="passSuccess   ">79.0</td><td class="aerialWonPerGame   ">12.9</td><td class="  sorted "><span class="stat-value rating">6.46</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2732/Show/Italie-Frosinone">17. Frosinone</a></td> <td class="goal   ">38</td><td class="shotsPerGame   ">11.7</td><td class="aaa"><span class="yellow-card-box">55</span><span class="red-card-box">2</span></td><td class="possession   ">51.4</td><td class="passSuccess   ">80.6</td><td class="aerialWonPerGame   ">14.1</td><td class="  sorted "><span class="stat-value rating">6.45</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/78/Show/Italie-Cagliari">18. Cagliari</a></td> <td class="goal   ">29</td><td class="shotsPerGame   ">11.9</td><td class="aaa"><span class="yellow-card-box">54</span><span class="red-card-box">4</span></td><td class="possession   ">42.3</td><td class="passSuccess   ">76.6</td><td class="aerialWonPerGame   ">15</td><td class="  sorted "><span class="stat-value rating">6.43</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2889/Show/Italie-Sassuolo">19. Sassuolo</a></td> <td class="goal   ">33</td><td class="shotsPerGame   ">12.8</td><td class="aaa"><span class="yellow-card-box">51</span><span class="red-card-box">3</span></td><td class="possession   ">43.9</td><td class="passSuccess   ">79.4</td><td class="aerialWonPerGame   ">11.2</td><td class="  sorted "><span class="stat-value rating">6.42</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/143/Show/Italie-Salernitana">20. Salernitana</a></td> <td class="goal   ">23</td><td class="shotsPerGame   ">10.8</td><td class="aaa"><span class="yellow-card-box">67</span><span class="red-card-box">3</span></td><td class="possession   ">45.6</td><td class="passSuccess   ">79.3</td><td class="aerialWonPerGame   ">13.9</td><td class="  sorted "><span class="stat-value rating">6.36</span></td></tr></tbody></table></div></div>
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
collection_name = 'Stats_All_Team_General_serieA'

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