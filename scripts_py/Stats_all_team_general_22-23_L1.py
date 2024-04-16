from bs4 import BeautifulSoup
import csv
from pymongo import MongoClient
import pandas as pd

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/304/Show/France-Paris-Saint-Germain">1. Paris Saint-Germain</a></td> <td class="goal   ">89</td><td class="shotsPerGame   ">15</td><td class="aaa"><span class="yellow-card-box">53</span><span class="red-card-box">5</span></td><td class="possession   ">60.9</td><td class="passSuccess   ">90.5</td><td class="aerialWonPerGame   ">6.6</td><td class="  sorted "><span class="stat-value rating">6.88</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/228/Show/France-Lyon">2. Lyon</a></td> <td class="goal   ">65</td><td class="shotsPerGame   ">13.7</td><td class="aaa"><span class="yellow-card-box">59</span><span class="red-card-box">3</span></td><td class="possession   ">58.0</td><td class="passSuccess   ">85.1</td><td class="aerialWonPerGame   ">12</td><td class="  sorted "><span class="stat-value rating">6.77</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/249/Show/France-Marseille">3. Marseille</a></td> <td class="goal   ">67</td><td class="shotsPerGame   ">14.6</td><td class="aaa"><span class="yellow-card-box">62</span><span class="red-card-box">4</span></td><td class="possession   ">56.9</td><td class="passSuccess   ">83.2</td><td class="aerialWonPerGame   ">13.1</td><td class="  sorted "><span class="stat-value rating">6.75</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/309/Show/France-Lens">4. Lens</a></td> <td class="goal   ">68</td><td class="shotsPerGame   ">13.9</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">3</span></td><td class="possession   ">55.8</td><td class="passSuccess   ">86.4</td><td class="aerialWonPerGame   ">10.8</td><td class="  sorted "><span class="stat-value rating">6.75</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/313/Show/France-Rennes">5. Rennes</a></td> <td class="goal   ">69</td><td class="shotsPerGame   ">13.9</td><td class="aaa"><span class="yellow-card-box">54</span><span class="red-card-box">6</span></td><td class="possession   ">55.8</td><td class="passSuccess   ">84.7</td><td class="aerialWonPerGame   ">14.2</td><td class="  sorted "><span class="stat-value rating">6.73</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/607/Show/France-Lille">6. Lille</a></td> <td class="goal   ">65</td><td class="shotsPerGame   ">14.7</td><td class="aaa"><span class="yellow-card-box">83</span><span class="red-card-box">1</span></td><td class="possession   ">60.9</td><td class="passSuccess   ">86.1</td><td class="aerialWonPerGame   ">11.7</td><td class="  sorted "><span class="stat-value rating">6.73</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/613/Show/France-Nice">7. Nice</a></td> <td class="goal   ">48</td><td class="shotsPerGame   ">13.5</td><td class="aaa"><span class="yellow-card-box">51</span><span class="red-card-box">3</span></td><td class="possession   ">51.5</td><td class="passSuccess   ">85.4</td><td class="aerialWonPerGame   ">10.3</td><td class="  sorted "><span class="stat-value rating">6.73</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/248/Show/France-Monaco">8. Monaco</a></td> <td class="goal   ">70</td><td class="shotsPerGame   ">12.6</td><td class="aaa"><span class="yellow-card-box">58</span><span class="red-card-box">5</span></td><td class="possession   ">47.8</td><td class="passSuccess   ">80.5</td><td class="aerialWonPerGame   ">13.6</td><td class="  sorted "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/146/Show/France-Lorient">9. Lorient</a></td> <td class="goal   ">52</td><td class="shotsPerGame   ">10.6</td><td class="aaa"><span class="yellow-card-box">42</span><span class="red-card-box">4</span></td><td class="possession   ">46.4</td><td class="passSuccess   ">83.5</td><td class="aerialWonPerGame   ">10.9</td><td class="  sorted "><span class="stat-value rating">6.66</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/311/Show/France-Montpellier">10. Montpellier</a></td> <td class="goal   ">65</td><td class="shotsPerGame   ">11.6</td><td class="aaa"><span class="yellow-card-box">66</span><span class="red-card-box">10</span></td><td class="possession   ">45.4</td><td class="passSuccess   ">78.6</td><td class="aerialWonPerGame   ">12.7</td><td class="  sorted "><span class="stat-value rating">6.64</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/950/Show/France-Reims">11. Reims</a></td> <td class="goal   ">45</td><td class="shotsPerGame   ">13.8</td><td class="aaa"><span class="yellow-card-box">67</span><span class="red-card-box">9</span></td><td class="possession   ">48.4</td><td class="passSuccess   ">80.0</td><td class="aerialWonPerGame   ">12.6</td><td class="  sorted "><span class="stat-value rating">6.63</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/148/Show/France-Strasbourg">12. Strasbourg</a></td> <td class="goal   ">51</td><td class="shotsPerGame   ">11.8</td><td class="aaa"><span class="yellow-card-box">68</span><span class="red-card-box">7</span></td><td class="possession   ">45.6</td><td class="passSuccess   ">79.0</td><td class="aerialWonPerGame   ">18</td><td class="  sorted "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2332/Show/France-Brest">13. Brest</a></td> <td class="goal   ">44</td><td class="shotsPerGame   ">11.2</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">2</span></td><td class="possession   ">44.6</td><td class="passSuccess   ">78.0</td><td class="aerialWonPerGame   ">16.8</td><td class="  sorted "><span class="stat-value rating">6.62</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/246/Show/France-Toulouse">14. Toulouse</a></td> <td class="goal   ">51</td><td class="shotsPerGame   ">12</td><td class="aaa"><span class="yellow-card-box">56</span><span class="red-card-box">3</span></td><td class="possession   ">51.1</td><td class="passSuccess   ">82.0</td><td class="aerialWonPerGame   ">12.9</td><td class="  sorted "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/302/Show/France-Nantes">15. Nantes</a></td> <td class="goal   ">37</td><td class="shotsPerGame   ">11.8</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">4</span></td><td class="possession   ">45.5</td><td class="passSuccess   ">80.0</td><td class="aerialWonPerGame   ">15.7</td><td class="  sorted "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/941/Show/France-Clermont-Foot">16. Clermont Foot</a></td> <td class="goal   ">45</td><td class="shotsPerGame   ">10.6</td><td class="aaa"><span class="yellow-card-box">68</span><span class="red-card-box">5</span></td><td class="possession   ">48.1</td><td class="passSuccess   ">81.1</td><td class="aerialWonPerGame   ">10.8</td><td class="  sorted "><span class="stat-value rating">6.57</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/229/Show/France-Troyes">17. Troyes</a></td> <td class="goal   ">45</td><td class="shotsPerGame   ">10.4</td><td class="aaa"><span class="yellow-card-box">55</span><span class="red-card-box">5</span></td><td class="possession   ">42.1</td><td class="passSuccess   ">80.2</td><td class="aerialWonPerGame   ">12.6</td><td class="  sorted "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/308/Show/France-Auxerre">18. Auxerre</a></td> <td class="goal   ">35</td><td class="shotsPerGame   ">11.3</td><td class="aaa"><span class="yellow-card-box">46</span><span class="red-card-box">6</span></td><td class="possession   ">43.0</td><td class="passSuccess   ">79.6</td><td class="aerialWonPerGame   ">13</td><td class="  sorted "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/614/Show/France-Angers">19. Angers</a></td> <td class="goal   ">33</td><td class="shotsPerGame   ">9.8</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">5</span></td><td class="possession   ">46.7</td><td class="passSuccess   ">82.8</td><td class="aerialWonPerGame   ">11.6</td><td class="  sorted "><span class="stat-value rating">6.43</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/610/Show/France-AC-Ajaccio">20. AC Ajaccio</a></td> <td class="goal   ">23</td><td class="shotsPerGame   ">8.4</td><td class="aaa"><span class="yellow-card-box">82</span><span class="red-card-box">10</span></td><td class="possession   ">42.5</td><td class="passSuccess   ">75.5</td><td class="aerialWonPerGame   ">16.2</td><td class="  sorted "><span class="stat-value rating">6.34</span></td></tr></tbody></table></div></div>
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
collection_name = 'Stats_All_Team_General_22-23_L1'

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