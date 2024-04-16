from bs4 import BeautifulSoup
import csv
from pymongo import MongoClient
import pandas as pd

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/304/Show/France-Paris-Saint-Germain">1. Paris Saint-Germain</a></td> <td class="goal   ">86</td><td class="shotsPerGame   ">15</td><td class="aaa"><span class="yellow-card-box">73</span><span class="red-card-box">7</span></td><td class="possession   ">60.1</td><td class="passSuccess   ">89.5</td><td class="aerialWonPerGame   ">9.5</td><td class="  sorted "><span class="stat-value rating">6.88</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/607/Show/France-Lille">2. Lille</a></td> <td class="goal   ">64</td><td class="shotsPerGame   ">12.8</td><td class="aaa"><span class="yellow-card-box">67</span><span class="red-card-box">2</span></td><td class="possession   ">52.6</td><td class="passSuccess   ">83.5</td><td class="aerialWonPerGame   ">15.8</td><td class="  sorted "><span class="stat-value rating">6.82</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/228/Show/France-Lyon">3. Lyon</a></td> <td class="goal   ">81</td><td class="shotsPerGame   ">16.1</td><td class="aaa"><span class="yellow-card-box">60</span><span class="red-card-box">10</span></td><td class="possession   ">53.6</td><td class="passSuccess   ">84.7</td><td class="aerialWonPerGame   ">14.3</td><td class="  sorted "><span class="stat-value rating">6.80</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/248/Show/France-Monaco">4. Monaco</a></td> <td class="goal   ">76</td><td class="shotsPerGame   ">12.8</td><td class="aaa"><span class="yellow-card-box">74</span><span class="red-card-box">7</span></td><td class="possession   ">54.2</td><td class="passSuccess   ">82.7</td><td class="aerialWonPerGame   ">16.5</td><td class="  sorted "><span class="stat-value rating">6.73</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/313/Show/France-Rennes">5. Rennes</a></td> <td class="goal   ">52</td><td class="shotsPerGame   ">13.5</td><td class="aaa"><span class="yellow-card-box">80</span><span class="red-card-box">5</span></td><td class="possession   ">56.8</td><td class="passSuccess   ">85.6</td><td class="aerialWonPerGame   ">16.9</td><td class="  sorted "><span class="stat-value rating">6.69</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/314/Show/France-Metz">6. Metz</a></td> <td class="goal   ">44</td><td class="shotsPerGame   ">11.5</td><td class="aaa"><span class="yellow-card-box">82</span><span class="red-card-box">4</span></td><td class="possession   ">46.9</td><td class="passSuccess   ">79.8</td><td class="aerialWonPerGame   ">16.1</td><td class="  sorted "><span class="stat-value rating">6.66</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/309/Show/France-Lens">7. Lens</a></td> <td class="goal   ">55</td><td class="shotsPerGame   ">11.7</td><td class="aaa"><span class="yellow-card-box">84</span><span class="red-card-box">7</span></td><td class="possession   ">51.1</td><td class="passSuccess   ">81.8</td><td class="aerialWonPerGame   ">17.4</td><td class="  sorted "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2332/Show/France-Brest">8. Brest</a></td> <td class="goal   ">50</td><td class="shotsPerGame   ">11.8</td><td class="aaa"><span class="yellow-card-box">60</span><span class="red-card-box">4</span></td><td class="possession   ">49.4</td><td class="passSuccess   ">81.3</td><td class="aerialWonPerGame   ">18.6</td><td class="  sorted "><span class="stat-value rating">6.64</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/249/Show/France-Marseille">9. Marseille</a></td> <td class="goal   ">54</td><td class="shotsPerGame   ">10</td><td class="aaa"><span class="yellow-card-box">94</span><span class="red-card-box">9</span></td><td class="possession   ">19.6</td><td class="passSuccess   ">82.0</td><td class="aerialWonPerGame   ">14.9</td><td class="  sorted "><span class="stat-value rating">6.63</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/311/Show/France-Montpellier">10. Montpellier</a></td> <td class="goal   ">60</td><td class="shotsPerGame   ">12.2</td><td class="aaa"><span class="yellow-card-box">65</span><span class="red-card-box">7</span></td><td class="possession   ">46.4</td><td class="passSuccess   ">78.8</td><td class="aerialWonPerGame   ">17.9</td><td class="  sorted "><span class="stat-value rating">6.63</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/613/Show/France-Nice">11. Nice</a></td> <td class="goal   ">50</td><td class="shotsPerGame   ">10.8</td><td class="aaa"><span class="yellow-card-box">69</span><span class="red-card-box">4</span></td><td class="possession   ">53.4</td><td class="passSuccess   ">85.6</td><td class="aerialWonPerGame   ">10.5</td><td class="  sorted "><span class="stat-value rating">6.63</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/302/Show/France-Nantes">12. Nantes</a></td> <td class="goal   ">47</td><td class="shotsPerGame   ">10.8</td><td class="aaa"><span class="yellow-card-box">68</span><span class="red-card-box">4</span></td><td class="possession   ">21.9</td><td class="passSuccess   ">77.0</td><td class="aerialWonPerGame   ">18.1</td><td class="  sorted "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/148/Show/France-Strasbourg">13. Strasbourg</a></td> <td class="goal   ">49</td><td class="shotsPerGame   ">11.3</td><td class="aaa"><span class="yellow-card-box">56</span><span class="red-card-box">3</span></td><td class="possession   ">46.5</td><td class="passSuccess   ">78.3</td><td class="aerialWonPerGame   ">18.3</td><td class="  sorted "><span class="stat-value rating">6.61</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/950/Show/France-Reims">14. Reims</a></td> <td class="goal   ">42</td><td class="shotsPerGame   ">9.6</td><td class="aaa"><span class="yellow-card-box">75</span><span class="red-card-box">8</span></td><td class="possession   ">22.6</td><td class="passSuccess   ">80.7</td><td class="aerialWonPerGame   ">13.4</td><td class="  sorted "><span class="stat-value rating">6.60</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/145/Show/France-Saint-Etienne">15. Saint-Etienne</a></td> <td class="goal   ">42</td><td class="shotsPerGame   ">11.6</td><td class="aaa"><span class="yellow-card-box">76</span><span class="red-card-box">2</span></td><td class="possession   ">49.0</td><td class="passSuccess   ">79.3</td><td class="aerialWonPerGame   ">16.4</td><td class="  sorted "><span class="stat-value rating">6.59</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/315/Show/France-Bordeaux">16. Bordeaux</a></td> <td class="goal   ">42</td><td class="shotsPerGame   ">11.1</td><td class="aaa"><span class="yellow-card-box">74</span><span class="red-card-box">4</span></td><td class="possession   ">25.2</td><td class="passSuccess   ">83.3</td><td class="aerialWonPerGame   ">15.4</td><td class="  sorted "><span class="stat-value rating">6.59</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/146/Show/France-Lorient">17. Lorient</a></td> <td class="goal   ">50</td><td class="shotsPerGame   ">11.2</td><td class="aaa"><span class="yellow-card-box">67</span><span class="red-card-box">3</span></td><td class="possession   ">45.9</td><td class="passSuccess   ">78.8</td><td class="aerialWonPerGame   ">13.7</td><td class="  sorted "><span class="stat-value rating">6.54</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/614/Show/France-Angers">18. Angers</a></td> <td class="goal   ">40</td><td class="shotsPerGame   ">10.7</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">2</span></td><td class="possession   ">47.1</td><td class="passSuccess   ">81.3</td><td class="aerialWonPerGame   ">13.2</td><td class="  sorted "><span class="stat-value rating">6.53</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/245/Show/France-Nimes">19. Nimes</a></td> <td class="goal   ">40</td><td class="shotsPerGame   ">10.3</td><td class="aaa"><span class="yellow-card-box">57</span><span class="red-card-box">5</span></td><td class="possession   ">45.8</td><td class="passSuccess   ">77.5</td><td class="aerialWonPerGame   ">14</td><td class="  sorted "><span class="stat-value rating">6.52</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1364/Show/France-Dijon">20. Dijon</a></td> <td class="goal   ">25</td><td class="shotsPerGame   ">9.2</td><td class="aaa"><span class="yellow-card-box">75</span><span class="red-card-box">5</span></td><td class="possession   ">46.9</td><td class="passSuccess   ">80.0</td><td class="aerialWonPerGame   ">14.3</td><td class="  sorted "><span class="stat-value rating">6.42</span></td></tr></tbody></table></div></div>
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
collection_name = 'Stats_All_Team_General_20-21_L1'

# Supprimer la collection si elle existe
if collection_name in db.list_collection_names():
    db.drop_collection(collection_name)
    print("La collection existe. Elle a été supprimée.")

# Recréer la collection
collection = db[collection_name]

# Remplacer "Paris Saint-Germain" par "Paris SG" dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Paris Saint-Germain", "Paris SG")

# Remplacer "Clermont Foot" par "Clermont F." dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Clermont Foot", "Clermont F.")

# Convertir le DataFrame en dictionnaire pour l'insertion dans MongoDB
data_dict = df.to_dict(orient='records')

# Insérer les données dans la collection MongoDB
collection.insert_many(data_dict)

print("Les données ont été insérées dans la collection MongoDB avec succès.")