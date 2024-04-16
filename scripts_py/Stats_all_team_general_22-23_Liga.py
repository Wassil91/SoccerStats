from bs4 import BeautifulSoup
import csv
from pymongo import MongoClient
import pandas as pd

html_content = """
<div id="stage-team-stats-summary" class="statistics-table-tab" style="">
            <div id="statistics-team-mini-filter-summary" class="statistics-table-filter"><div class="clear"></div><div class="grid-toolbar"><dl id="field" class="listbox left"><dd><a class="option  selected " data-value="Overall" data-backbone-model-attribute="field">Général</a></dd><dd><a class="option " data-value="Home" data-backbone-model-attribute="field">Domicile</a></dd><dd><a class="option " data-value="Away" data-backbone-model-attribute="field">Extérieur</a></dd></dl></div></div>
            <div id="statistics-team-table-summary-loading" class="loading-wrapper" style="display: none;"><div class="loading-spinner-container"> <div class="loading-spinner"><div class="spinner-container loading-container1"><div class="circle1"></div><div class="circle2"></div><div class="circle3"></div><div class="circle4"></div></div><div class="spinner-container loading-container2"><div class="circle1"></div><div class="circle2"></div><div class="circle3"></div><div class="circle4"></div></div><div class="spinner-container loading-container3"><div class="circle1"></div><div class="circle2"></div><div class="circle3"></div><div class="circle4"></div></div></div><div class="loading-spinner-container-shade"></div></div></div>
            <div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/52/Show/Espagne-Real-Madrid">1. Real Madrid</a></td> <td class="goal   ">75</td><td class="shotsPerGame   ">17</td><td class="aaa"><span class="yellow-card-box">68</span><span class="red-card-box">3</span></td><td class="possession   ">61.2</td><td class="passSuccess   ">90.0</td><td class="aerialWonPerGame   ">8.9</td><td class="  sorted "><span class="stat-value rating">6.86</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/65/Show/Espagne-Barcelona">2. Barcelona</a></td> <td class="goal   ">70</td><td class="shotsPerGame   ">15.1</td><td class="aaa"><span class="yellow-card-box">78</span><span class="red-card-box">6</span></td><td class="possession   ">64.8</td><td class="passSuccess   ">88.1</td><td class="aerialWonPerGame   ">12.3</td><td class="  sorted "><span class="stat-value rating">6.85</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/63/Show/Espagne-Atletico-Madrid">3. Atletico Madrid</a></td> <td class="goal   ">70</td><td class="shotsPerGame   ">14.2</td><td class="aaa"><span class="yellow-card-box">86</span><span class="red-card-box">8</span></td><td class="possession   ">50.4</td><td class="passSuccess   ">84.2</td><td class="aerialWonPerGame   ">11.9</td><td class="  sorted "><span class="stat-value rating">6.75</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/839/Show/Espagne-Villarreal">4. Villarreal</a></td> <td class="goal   ">59</td><td class="shotsPerGame   ">13.3</td><td class="aaa"><span class="yellow-card-box">86</span><span class="red-card-box">4</span></td><td class="possession   ">56.7</td><td class="passSuccess   ">85.4</td><td class="aerialWonPerGame   ">9.5</td><td class="  sorted "><span class="stat-value rating">6.68</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/68/Show/Espagne-Real-Sociedad">5. Real Sociedad</a></td> <td class="goal   ">51</td><td class="shotsPerGame   ">12.3</td><td class="aaa"><span class="yellow-card-box">91</span><span class="red-card-box">2</span></td><td class="possession   ">54.6</td><td class="passSuccess   ">81.9</td><td class="aerialWonPerGame   ">17.6</td><td class="  sorted "><span class="stat-value rating">6.65</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/54/Show/Espagne-Real-Betis">6. Real Betis</a></td> <td class="goal   ">46</td><td class="shotsPerGame   ">11.1</td><td class="aaa"><span class="yellow-card-box">77</span><span class="red-card-box">15</span></td><td class="possession   ">50.5</td><td class="passSuccess   ">82.4</td><td class="aerialWonPerGame   ">12.1</td><td class="  sorted "><span class="stat-value rating">6.63</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/53/Show/Espagne-Athletic-Club">7. Athletic Club</a></td> <td class="goal   ">47</td><td class="shotsPerGame   ">14.4</td><td class="aaa"><span class="yellow-card-box">80</span><span class="red-card-box">5</span></td><td class="possession   ">51.5</td><td class="passSuccess   ">80.1</td><td class="aerialWonPerGame   ">14.2</td><td class="  sorted "><span class="stat-value rating">6.59</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/51/Show/Espagne-Mallorca">8. Mallorca</a></td> <td class="goal   ">37</td><td class="shotsPerGame   ">8.7</td><td class="aaa"><span class="yellow-card-box">121</span><span class="red-card-box">5</span></td><td class="possession   ">40.4</td><td class="passSuccess   ">75.5</td><td class="aerialWonPerGame   ">17.8</td><td class="  sorted "><span class="stat-value rating">6.56</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/62/Show/Espagne-Celta-Vigo">9. Celta Vigo</a></td> <td class="goal   ">43</td><td class="shotsPerGame   ">12.4</td><td class="aaa"><span class="yellow-card-box">80</span><span class="red-card-box">6</span></td><td class="possession   ">50.1</td><td class="passSuccess   ">81.8</td><td class="aerialWonPerGame   ">12.1</td><td class="  sorted "><span class="stat-value rating">6.56</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2783/Show/Espagne-Girona">10. Girona</a></td> <td class="goal   ">58</td><td class="shotsPerGame   ">12</td><td class="aaa"><span class="yellow-card-box">88</span><span class="red-card-box">3</span></td><td class="possession   ">50.9</td><td class="passSuccess   ">83.4</td><td class="aerialWonPerGame   ">11.4</td><td class="  sorted "><span class="stat-value rating">6.55</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/55/Show/Espagne-Valencia">11. Valencia</a></td> <td class="goal   ">42</td><td class="shotsPerGame   ">12.8</td><td class="aaa"><span class="yellow-card-box">86</span><span class="red-card-box">8</span></td><td class="possession   ">51.8</td><td class="passSuccess   ">80.6</td><td class="aerialWonPerGame   ">13</td><td class="  sorted "><span class="stat-value rating">6.54</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/131/Show/Espagne-Osasuna">12. Osasuna</a></td> <td class="goal   ">37</td><td class="shotsPerGame   ">11.7</td><td class="aaa"><span class="yellow-card-box">91</span><span class="red-card-box">7</span></td><td class="possession   ">48.1</td><td class="passSuccess   ">78.0</td><td class="aerialWonPerGame   ">16.2</td><td class="  sorted "><span class="stat-value rating">6.54</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/67/Show/Espagne-Sevilla">13. Sevilla</a></td> <td class="goal   ">47</td><td class="shotsPerGame   ">12.3</td><td class="aaa"><span class="yellow-card-box">112</span><span class="red-card-box">13</span></td><td class="possession   ">52.7</td><td class="passSuccess   ">83.0</td><td class="aerialWonPerGame   ">13.1</td><td class="  sorted "><span class="stat-value rating">6.53</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/70/Show/Espagne-Espanyol">14. Espanyol</a></td> <td class="goal   ">52</td><td class="shotsPerGame   ">11.1</td><td class="aaa"><span class="yellow-card-box">91</span><span class="red-card-box">10</span></td><td class="possession   ">42.6</td><td class="passSuccess   ">76.5</td><td class="aerialWonPerGame   ">17.1</td><td class="  sorted "><span class="stat-value rating">6.53</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/64/Show/Espagne-Rayo-Vallecano">15. Rayo Vallecano</a></td> <td class="goal   ">45</td><td class="shotsPerGame   ">13.6</td><td class="aaa"><span class="yellow-card-box">87</span><span class="red-card-box">6</span></td><td class="possession   ">50.8</td><td class="passSuccess   ">78.8</td><td class="aerialWonPerGame   ">11.5</td><td class="  sorted "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/58/Show/Espagne-Real-Valladolid">16. Real Valladolid</a></td> <td class="goal   ">33</td><td class="shotsPerGame   ">11.8</td><td class="aaa"><span class="yellow-card-box">89</span><span class="red-card-box">6</span></td><td class="possession   ">48.5</td><td class="passSuccess   ">79.1</td><td class="aerialWonPerGame   ">12.5</td><td class="  sorted "><span class="stat-value rating">6.50</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1799/Show/Espagne-Almeria">17. Almeria</a></td> <td class="goal   ">49</td><td class="shotsPerGame   ">11.6</td><td class="aaa"><span class="yellow-card-box">94</span><span class="red-card-box">4</span></td><td class="possession   ">44.9</td><td class="passSuccess   ">78.3</td><td class="aerialWonPerGame   ">12.8</td><td class="  sorted "><span class="stat-value rating">6.50</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/819/Show/Espagne-Getafe">18. Getafe</a></td> <td class="goal   ">34</td><td class="shotsPerGame   ">9.8</td><td class="aaa"><span class="yellow-card-box">116</span><span class="red-card-box">9</span></td><td class="possession   ">39.2</td><td class="passSuccess   ">70.6</td><td class="aerialWonPerGame   ">19</td><td class="  sorted "><span class="stat-value rating">6.49</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1354/Show/Espagne-Cadiz">19. Cadiz</a></td> <td class="goal   ">30</td><td class="shotsPerGame   ">10.4</td><td class="aaa"><span class="yellow-card-box">107</span><span class="red-card-box">7</span></td><td class="possession   ">41.3</td><td class="passSuccess   ">74.1</td><td class="aerialWonPerGame   ">16.1</td><td class="  sorted "><span class="stat-value rating">6.49</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/833/Show/Espagne-Elche">20. Elche</a></td> <td class="goal   ">30</td><td class="shotsPerGame   ">10.7</td><td class="aaa"><span class="yellow-card-box">93</span><span class="red-card-box">12</span></td><td class="possession   ">44.9</td><td class="passSuccess   ">79.1</td><td class="aerialWonPerGame   ">12.8</td><td class="  sorted "><span class="stat-value rating">6.44</span></td></tr></tbody></table></div></div>
            <div id="statistics-team-table-summary-column-legend"><div class="table-column-legend info">  <div class="col12-lg-4 col12-m-4 col12-s-6 col12-xs-6"><strong>Buts</strong>: Total Buts</div><div class="col12-lg-4 col12-m-4 col12-s-6 col12-xs-6"><strong>Tirs pm</strong>: Tirs par match</div><div class="col12-lg-4 col12-m-4 col12-s-6 col12-xs-6"><strong>Discipline</strong>: Carton jaune</div>   <div class="col12-lg-4 col12-m-4 col12-s-6 col12-xs-6"><strong>Rou</strong>: Carton rouge</div><div class="col12-lg-4 col12-m-4 col12-s-6 col12-xs-6"><strong>Possession%</strong>: Pourcentage Possession</div><div class="col12-lg-4 col12-m-4 col12-s-6 col12-xs-6"><strong>PassesRéussies%</strong>: Taux de passes réussies</div>   <div class="col12-lg-4 col12-m-4 col12-s-6 col12-xs-6"><strong>AériensGagnés</strong>: Duels aériens gagnés par match</div></div></div>
        </div>
"""
soup = BeautifulSoup(html_content, "html.parser")

data = []
headers = ['Équipe', 'Buts', 'Tirs pm', 'Discipline', 'Possession%', 'PassesRéussies%', 'AériensGagnés', 'Note']

table_body = soup.find("tbody", {"id": "top-team-stats-summary-content"})
rows = table_body.find_all("tr")

with open('football_stats_22_23_liga.csv', mode='w', newline='', encoding='utf-8') as file:
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

print("Les données ont été enregistrées dans football_stats_22_23_liga.csv avec succès !")

# Lire le fichier CSV dans un DataFrame
df = pd.read_csv("football_stats_22_23_liga.csv")

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

print("Les données ont été extraites et enregistrées dans football_stats_liga.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_General_22-23_Liga'

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