from bs4 import BeautifulSoup
import csv
from pymongo import MongoClient
import pandas as pd

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/37/Show/Allemagne-Bayern-Munich">1. Bayern Munich</a></td> <td class="goal   ">92</td><td class="shotsPerGame   ">18.5</td><td class="aaa"><span class="yellow-card-box">45</span><span class="red-card-box">3</span></td><td class="possession   ">64.3</td><td class="passSuccess   ">87.1</td><td class="aerialWonPerGame   ">13.9</td><td class="  sorted "><span class="stat-value rating">6.94</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/44/Show/Allemagne-Borussia-Dortmund">2. Borussia Dortmund</a></td> <td class="goal   ">83</td><td class="shotsPerGame   ">16.6</td><td class="aaa"><span class="yellow-card-box">66</span><span class="red-card-box">0</span></td><td class="possession   ">58.3</td><td class="passSuccess   ">84.4</td><td class="aerialWonPerGame   ">12.6</td><td class="  sorted "><span class="stat-value rating">6.88</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/7614/Show/Allemagne-RB-Leipzig">3. RB Leipzig</a></td> <td class="goal   ">64</td><td class="shotsPerGame   ">14.8</td><td class="aaa"><span class="yellow-card-box">67</span><span class="red-card-box">2</span></td><td class="possession   ">58.4</td><td class="passSuccess   ">83.6</td><td class="aerialWonPerGame   ">13</td><td class="  sorted "><span class="stat-value rating">6.72</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/50/Show/Allemagne-Freiburg">4. Freiburg</a></td> <td class="goal   ">51</td><td class="shotsPerGame   ">12.4</td><td class="aaa"><span class="yellow-card-box">60</span><span class="red-card-box">2</span></td><td class="possession   ">48.0</td><td class="passSuccess   ">76.9</td><td class="aerialWonPerGame   ">22</td><td class="  sorted "><span class="stat-value rating">6.67</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/33/Show/Allemagne-Wolfsburg">5. Wolfsburg</a></td> <td class="goal   ">57</td><td class="shotsPerGame   ">12</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">0</span></td><td class="possession   ">50.5</td><td class="passSuccess   ">79.4</td><td class="aerialWonPerGame   ">15.6</td><td class="  sorted "><span class="stat-value rating">6.65</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/36/Show/Allemagne-Bayer-Leverkusen">6. Bayer Leverkusen</a></td> <td class="goal   ">57</td><td class="shotsPerGame   ">12.9</td><td class="aaa"><span class="yellow-card-box">70</span><span class="red-card-box">7</span></td><td class="possession   ">51.8</td><td class="passSuccess   ">81.9</td><td class="aerialWonPerGame   ">12.7</td><td class="  sorted "><span class="stat-value rating">6.64</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/134/Show/Allemagne-Borussia-M-Gladbach">7. Borussia M.Gladbach</a></td> <td class="goal   ">52</td><td class="shotsPerGame   ">12</td><td class="aaa"><span class="yellow-card-box">60</span><span class="red-card-box">3</span></td><td class="possession   ">54.0</td><td class="passSuccess   ">83.4</td><td class="aerialWonPerGame   ">12.6</td><td class="  sorted "><span class="stat-value rating">6.63</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/796/Show/Allemagne-Union-Berlin">8. Union Berlin</a></td> <td class="goal   ">51</td><td class="shotsPerGame   ">11.2</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">2</span></td><td class="possession   ">42.9</td><td class="passSuccess   ">73.6</td><td class="aerialWonPerGame   ">22.7</td><td class="  sorted "><span class="stat-value rating">6.63</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/219/Show/Allemagne-Mainz-05">9. Mainz 05</a></td> <td class="goal   ">54</td><td class="shotsPerGame   ">12.4</td><td class="aaa"><span class="yellow-card-box">69</span><span class="red-card-box">1</span></td><td class="possession   ">43.9</td><td class="passSuccess   ">71.3</td><td class="aerialWonPerGame   ">21.9</td><td class="  sorted "><span class="stat-value rating">6.61</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/45/Show/Allemagne-Eintracht-Frankfurt">10. Eintracht Frankfurt</a></td> <td class="goal   ">58</td><td class="shotsPerGame   ">11.9</td><td class="aaa"><span class="yellow-card-box">70</span><span class="red-card-box">1</span></td><td class="possession   ">52.5</td><td class="passSuccess   ">79.5</td><td class="aerialWonPerGame   ">13.8</td><td class="  sorted "><span class="stat-value rating">6.60</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/42/Show/Allemagne-Werder-Bremen">11. Werder Bremen</a></td> <td class="goal   ">51</td><td class="shotsPerGame   ">10.9</td><td class="aaa"><span class="yellow-card-box">76</span><span class="red-card-box">1</span></td><td class="possession   ">49.5</td><td class="passSuccess   ">76.3</td><td class="aerialWonPerGame   ">18.7</td><td class="  sorted "><span class="stat-value rating">6.58</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/41/Show/Allemagne-VfB-Stuttgart">12. VfB Stuttgart</a></td> <td class="goal   ">45</td><td class="shotsPerGame   ">13.6</td><td class="aaa"><span class="yellow-card-box">64</span><span class="red-card-box">5</span></td><td class="possession   ">50.1</td><td class="passSuccess   ">80.9</td><td class="aerialWonPerGame   ">16.2</td><td class="  sorted "><span class="stat-value rating">6.57</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/282/Show/Allemagne-FC-Koln">13. FC Koln</a></td> <td class="goal   ">49</td><td class="shotsPerGame   ">12.3</td><td class="aaa"><span class="yellow-card-box">69</span><span class="red-card-box">3</span></td><td class="possession   ">49.4</td><td class="passSuccess   ">77.8</td><td class="aerialWonPerGame   ">19.5</td><td class="  sorted "><span class="stat-value rating">6.57</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1211/Show/Allemagne-Hoffenheim">14. Hoffenheim</a></td> <td class="goal   ">48</td><td class="shotsPerGame   ">12</td><td class="aaa"><span class="yellow-card-box">74</span><span class="red-card-box">5</span></td><td class="possession   ">48.0</td><td class="passSuccess   ">77.1</td><td class="aerialWonPerGame   ">16.3</td><td class="  sorted "><span class="stat-value rating">6.55</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/47/Show/Allemagne-Hertha-Berlin">15. Hertha Berlin</a></td> <td class="goal   ">42</td><td class="shotsPerGame   ">11.3</td><td class="aaa"><span class="yellow-card-box">80</span><span class="red-card-box">1</span></td><td class="possession   ">41.3</td><td class="passSuccess   ">71.2</td><td class="aerialWonPerGame   ">18.8</td><td class="  sorted "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/109/Show/Allemagne-Bochum">16. Bochum</a></td> <td class="goal   ">40</td><td class="shotsPerGame   ">11.8</td><td class="aaa"><span class="yellow-card-box">64</span><span class="red-card-box">1</span></td><td class="possession   ">45.7</td><td class="passSuccess   ">69.9</td><td class="aerialWonPerGame   ">22.8</td><td class="  sorted "><span class="stat-value rating">6.50</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/39/Show/Allemagne-Schalke-04">17. Schalke 04</a></td> <td class="goal   ">35</td><td class="shotsPerGame   ">12.5</td><td class="aaa"><span class="yellow-card-box">68</span><span class="red-card-box">1</span></td><td class="possession   ">45.2</td><td class="passSuccess   ">71.7</td><td class="aerialWonPerGame   ">20.5</td><td class="  sorted "><span class="stat-value rating">6.48</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1730/Show/Allemagne-Augsburg">18. Augsburg</a></td> <td class="goal   ">42</td><td class="shotsPerGame   ">10.6</td><td class="aaa"><span class="yellow-card-box">92</span><span class="red-card-box">5</span></td><td class="possession   ">40.9</td><td class="passSuccess   ">68.1</td><td class="aerialWonPerGame   ">18.9</td><td class="  sorted "><span class="stat-value rating">6.48</span></td></tr></tbody></table></div></div>
"""
soup = BeautifulSoup(html_content, "html.parser")

data = []
headers = ['Équipe', 'Buts', 'Tirs pm', 'Discipline', 'Possession%', 'PassesRéussies%', 'AériensGagnés', 'Note']

table_body = soup.find("tbody", {"id": "top-team-stats-summary-content"})
rows = table_body.find_all("tr")

with open('football_stats.csv', mode='w', newline='', encoding='utf-8') as file:
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
df = pd.read_csv("football_stats.csv")

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
df.to_csv("football_stats_modified.csv", index=False)

print("Les données ont été extraites et enregistrées dans football_stats_modified.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_General_22-23_bundes'

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