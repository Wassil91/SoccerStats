from bs4 import BeautifulSoup
import csv
from pymongo import MongoClient
import pandas as pd

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/37/Show/Allemagne-Bayern-Munich">1. Bayern Munich</a></td> <td class="goal   ">99</td><td class="shotsPerGame   ">17.1</td><td class="aaa"><span class="yellow-card-box">44</span><span class="red-card-box">3</span></td><td class="possession   ">58.1</td><td class="passSuccess   ">85.5</td><td class="aerialWonPerGame   ">12.9</td><td class="  sorted "><span class="stat-value rating">6.95</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/44/Show/Allemagne-Borussia-Dortmund">2. Borussia Dortmund</a></td> <td class="goal   ">75</td><td class="shotsPerGame   ">14.6</td><td class="aaa"><span class="yellow-card-box">43</span><span class="red-card-box">1</span></td><td class="possession   ">57.5</td><td class="passSuccess   ">85.5</td><td class="aerialWonPerGame   ">12.8</td><td class="  sorted "><span class="stat-value rating">6.84</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/33/Show/Allemagne-Wolfsburg">3. Wolfsburg</a></td> <td class="goal   ">61</td><td class="shotsPerGame   ">14.1</td><td class="aaa"><span class="yellow-card-box">56</span><span class="red-card-box">3</span></td><td class="possession   ">51.0</td><td class="passSuccess   ">78.0</td><td class="aerialWonPerGame   ">16.9</td><td class="  sorted "><span class="stat-value rating">6.80</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/7614/Show/Allemagne-RB-Leipzig">4. RB Leipzig</a></td> <td class="goal   ">60</td><td class="shotsPerGame   ">16</td><td class="aaa"><span class="yellow-card-box">57</span><span class="red-card-box">0</span></td><td class="possession   ">57.3</td><td class="passSuccess   ">83.2</td><td class="aerialWonPerGame   ">18.6</td><td class="  sorted "><span class="stat-value rating">6.78</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/36/Show/Allemagne-Bayer-Leverkusen">5. Bayer Leverkusen</a></td> <td class="goal   ">53</td><td class="shotsPerGame   ">13</td><td class="aaa"><span class="yellow-card-box">58</span><span class="red-card-box">0</span></td><td class="possession   ">57.3</td><td class="passSuccess   ">84.4</td><td class="aerialWonPerGame   ">13.1</td><td class="  sorted "><span class="stat-value rating">6.73</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/45/Show/Allemagne-Eintracht-Frankfurt">6. Eintracht Frankfurt</a></td> <td class="goal   ">69</td><td class="shotsPerGame   ">13.2</td><td class="aaa"><span class="yellow-card-box">80</span><span class="red-card-box">1</span></td><td class="possession   ">24.8</td><td class="passSuccess   ">79.6</td><td class="aerialWonPerGame   ">17.9</td><td class="  sorted "><span class="stat-value rating">6.73</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/134/Show/Allemagne-Borussia-M-Gladbach">7. Borussia M.Gladbach</a></td> <td class="goal   ">64</td><td class="shotsPerGame   ">13.4</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">2</span></td><td class="possession   ">51.5</td><td class="passSuccess   ">82.0</td><td class="aerialWonPerGame   ">15.3</td><td class="  sorted "><span class="stat-value rating">6.70</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/41/Show/Allemagne-VfB-Stuttgart">8. VfB Stuttgart</a></td> <td class="goal   ">56</td><td class="shotsPerGame   ">13.4</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">2</span></td><td class="possession   ">27.4</td><td class="passSuccess   ">81.1</td><td class="aerialWonPerGame   ">16.3</td><td class="  sorted "><span class="stat-value rating">6.68</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1211/Show/Allemagne-Hoffenheim">9. Hoffenheim</a></td> <td class="goal   ">52</td><td class="shotsPerGame   ">12.6</td><td class="aaa"><span class="yellow-card-box">65</span><span class="red-card-box">4</span></td><td class="possession   ">50.8</td><td class="passSuccess   ">80.7</td><td class="aerialWonPerGame   ">15.7</td><td class="  sorted "><span class="stat-value rating">6.63</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/796/Show/Allemagne-Union-Berlin">10. Union Berlin</a></td> <td class="goal   ">50</td><td class="shotsPerGame   ">11.7</td><td class="aaa"><span class="yellow-card-box">55</span><span class="red-card-box">2</span></td><td class="possession   ">20.9</td><td class="passSuccess   ">76.2</td><td class="aerialWonPerGame   ">17.6</td><td class="  sorted "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/50/Show/Allemagne-Freiburg">11. Freiburg</a></td> <td class="goal   ">52</td><td class="shotsPerGame   ">11.4</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">0</span></td><td class="possession   ">47.5</td><td class="passSuccess   ">78.1</td><td class="aerialWonPerGame   ">17.5</td><td class="  sorted "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/47/Show/Allemagne-Hertha-Berlin">12. Hertha Berlin</a></td> <td class="goal   ">41</td><td class="shotsPerGame   ">11.3</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">3</span></td><td class="possession   ">49.8</td><td class="passSuccess   ">79.5</td><td class="aerialWonPerGame   ">15.8</td><td class="  sorted "><span class="stat-value rating">6.58</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/219/Show/Allemagne-Mainz-05">13. Mainz 05</a></td> <td class="goal   ">39</td><td class="shotsPerGame   ">11.1</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">1</span></td><td class="possession   ">42.7</td><td class="passSuccess   ">71.3</td><td class="aerialWonPerGame   ">18.3</td><td class="  sorted "><span class="stat-value rating">6.57</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1730/Show/Allemagne-Augsburg">14. Augsburg</a></td> <td class="goal   ">36</td><td class="shotsPerGame   ">9.9</td><td class="aaa"><span class="yellow-card-box">65</span><span class="red-card-box">4</span></td><td class="possession   ">44.0</td><td class="passSuccess   ">73.6</td><td class="aerialWonPerGame   ">16.8</td><td class="  sorted "><span class="stat-value rating">6.56</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/40/Show/Allemagne-Arminia-Bielefeld">15. Arminia Bielefeld</a></td> <td class="goal   ">26</td><td class="shotsPerGame   ">9.8</td><td class="aaa"><span class="yellow-card-box">52</span><span class="red-card-box">1</span></td><td class="possession   ">44.1</td><td class="passSuccess   ">74.6</td><td class="aerialWonPerGame   ">22.2</td><td class="  sorted "><span class="stat-value rating">6.53</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/42/Show/Allemagne-Werder-Bremen">16. Werder Bremen</a></td> <td class="goal   ">36</td><td class="shotsPerGame   ">10.6</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">3</span></td><td class="possession   ">18.4</td><td class="passSuccess   ">76.2</td><td class="aerialWonPerGame   ">18.3</td><td class="  sorted "><span class="stat-value rating">6.52</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/282/Show/Allemagne-FC-Koln">17. FC Koln</a></td> <td class="goal   ">34</td><td class="shotsPerGame   ">10.6</td><td class="aaa"><span class="yellow-card-box">62</span><span class="red-card-box">1</span></td><td class="possession   ">47.1</td><td class="passSuccess   ">77.3</td><td class="aerialWonPerGame   ">18.5</td><td class="  sorted "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/39/Show/Allemagne-Schalke-04">18. Schalke 04</a></td> <td class="goal   ">25</td><td class="shotsPerGame   ">8.9</td><td class="aaa"><span class="yellow-card-box">70</span><span class="red-card-box">2</span></td><td class="possession   ">46.2</td><td class="passSuccess   ">76.5</td><td class="aerialWonPerGame   ">15.6</td><td class="  sorted "><span class="stat-value rating">6.41</span></td></tr></tbody></table></div></div>
"""

soup = BeautifulSoup(html_content, "html.parser")

data = []
headers = ['Équipe', 'Buts', 'Tirs pm', 'Discipline', 'Possession%', 'PassesRéussies%', 'AériensGagnés', 'Note']

table_body = soup.find("tbody", {"id": "top-team-stats-summary-content"})
rows = table_body.find_all("tr")

with open('football_stats_2021.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(headers)

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

print("Les données ont été enregistrées dans football_stats_2021.csv avec succès !")

# Lire le fichier CSV dans un DataFrame
df = pd.read_csv("football_stats_2021.csv")

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
df.to_csv("football_stats_2021.csv", index=False)

print("Les données ont été extraites et enregistrées dans football_stats_modified.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_General_20-21_bundes'

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