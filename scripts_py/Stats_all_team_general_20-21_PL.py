from bs4 import BeautifulSoup
import csv
from pymongo import MongoClient
import pandas as pd

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/167/Show/Angleterre-Manchester-City">1. Manchester City</a></td> <td class="goal   ">80</td><td class="shotsPerGame   ">15.4</td><td class="aaa"><span class="yellow-card-box">46</span><span class="red-card-box">2</span></td><td class="possession   ">63.7</td><td class="passSuccess   ">89.3</td><td class="aerialWonPerGame   ">12.6</td><td class="  sorted "><span class="stat-value rating">6.99</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/32/Show/Angleterre-Manchester-United">2. Manchester United</a></td> <td class="goal   ">73</td><td class="shotsPerGame   ">13.8</td><td class="aaa"><span class="yellow-card-box">64</span><span class="red-card-box">1</span></td><td class="possession   ">55.6</td><td class="passSuccess   ">84.8</td><td class="aerialWonPerGame   ">14.5</td><td class="  sorted "><span class="stat-value rating">6.85</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/24/Show/Angleterre-Aston-Villa">3. Aston Villa</a></td> <td class="goal   ">55</td><td class="shotsPerGame   ">13.7</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">4</span></td><td class="possession   ">47.8</td><td class="passSuccess   ">78.6</td><td class="aerialWonPerGame   ">19.4</td><td class="  sorted "><span class="stat-value rating">6.84</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/15/Show/Angleterre-Chelsea">4. Chelsea</a></td> <td class="goal   ">58</td><td class="shotsPerGame   ">14.6</td><td class="aaa"><span class="yellow-card-box">49</span><span class="red-card-box">3</span></td><td class="possession   ">61.2</td><td class="passSuccess   ">87.0</td><td class="aerialWonPerGame   ">15.2</td><td class="  sorted "><span class="stat-value rating">6.83</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/26/Show/Angleterre-Liverpool">5. Liverpool</a></td> <td class="goal   ">68</td><td class="shotsPerGame   ">16</td><td class="aaa"><span class="yellow-card-box">40</span><span class="red-card-box">0</span></td><td class="possession   ">62.4</td><td class="passSuccess   ">85.7</td><td class="aerialWonPerGame   ">14.3</td><td class="  sorted "><span class="stat-value rating">6.82</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/30/Show/Angleterre-Tottenham">6. Tottenham</a></td> <td class="goal   ">68</td><td class="shotsPerGame   ">11.7</td><td class="aaa"><span class="yellow-card-box">53</span><span class="red-card-box">2</span></td><td class="possession   ">51.4</td><td class="passSuccess   ">81.8</td><td class="aerialWonPerGame   ">16.4</td><td class="  sorted "><span class="stat-value rating">6.81</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/14/Show/Angleterre-Leicester">7. Leicester</a></td> <td class="goal   ">68</td><td class="shotsPerGame   ">12.8</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">0</span></td><td class="possession   ">54.4</td><td class="passSuccess   ">82.1</td><td class="aerialWonPerGame   ">16.2</td><td class="  sorted "><span class="stat-value rating">6.80</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/19/Show/Angleterre-Leeds">8. Leeds</a></td> <td class="goal   ">62</td><td class="shotsPerGame   ">13.7</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">1</span></td><td class="possession   ">57.8</td><td class="passSuccess   ">80.8</td><td class="aerialWonPerGame   ">14.5</td><td class="  sorted "><span class="stat-value rating">6.77</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/29/Show/Angleterre-West-Ham">9. West Ham</a></td> <td class="goal   ">62</td><td class="shotsPerGame   ">12.3</td><td class="aaa"><span class="yellow-card-box">48</span><span class="red-card-box">3</span></td><td class="possession   ">42.5</td><td class="passSuccess   ">77.8</td><td class="aerialWonPerGame   ">19.9</td><td class="  sorted "><span class="stat-value rating">6.77</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/31/Show/Angleterre-Everton">10. Everton</a></td> <td class="goal   ">47</td><td class="shotsPerGame   ">10.5</td><td class="aaa"><span class="yellow-card-box">59</span><span class="red-card-box">2</span></td><td class="possession   ">46.5</td><td class="passSuccess   ">81.4</td><td class="aerialWonPerGame   ">17.7</td><td class="  sorted "><span class="stat-value rating">6.73</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/13/Show/Angleterre-Arsenal">11. Arsenal</a></td> <td class="goal   ">55</td><td class="shotsPerGame   ">12.1</td><td class="aaa"><span class="yellow-card-box">47</span><span class="red-card-box">5</span></td><td class="possession   ">53.4</td><td class="passSuccess   ">85.0</td><td class="aerialWonPerGame   ">13.5</td><td class="  sorted "><span class="stat-value rating">6.69</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/161/Show/Angleterre-Wolves">12. Wolves</a></td> <td class="goal   ">35</td><td class="shotsPerGame   ">12</td><td class="aaa"><span class="yellow-card-box">52</span><span class="red-card-box">1</span></td><td class="possession   ">49.2</td><td class="passSuccess   ">83.1</td><td class="aerialWonPerGame   ">15.2</td><td class="  sorted "><span class="stat-value rating">6.66</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/211/Show/Angleterre-Brighton">13. Brighton</a></td> <td class="goal   ">40</td><td class="shotsPerGame   ">12.8</td><td class="aaa"><span class="yellow-card-box">45</span><span class="red-card-box">6</span></td><td class="possession   ">50.8</td><td class="passSuccess   ">81.3</td><td class="aerialWonPerGame   ">14.2</td><td class="  sorted "><span class="stat-value rating">6.65</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/184/Show/Angleterre-Burnley">14. Burnley</a></td> <td class="goal   ">33</td><td class="shotsPerGame   ">10.1</td><td class="aaa"><span class="yellow-card-box">48</span><span class="red-card-box">0</span></td><td class="possession   ">41.5</td><td class="passSuccess   ">71.6</td><td class="aerialWonPerGame   ">23.4</td><td class="  sorted "><span class="stat-value rating">6.64</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/170/Show/Angleterre-Fulham">15. Fulham</a></td> <td class="goal   ">27</td><td class="shotsPerGame   ">11.6</td><td class="aaa"><span class="yellow-card-box">67</span><span class="red-card-box">3</span></td><td class="possession   ">49.5</td><td class="passSuccess   ">81.2</td><td class="aerialWonPerGame   ">17.2</td><td class="  sorted "><span class="stat-value rating">6.63</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/18/Show/Angleterre-Southampton">16. Southampton</a></td> <td class="goal   ">47</td><td class="shotsPerGame   ">11.2</td><td class="aaa"><span class="yellow-card-box">52</span><span class="red-card-box">3</span></td><td class="possession   ">51.7</td><td class="passSuccess   ">79.3</td><td class="aerialWonPerGame   ">14.1</td><td class="  sorted "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/23/Show/Angleterre-Newcastle">17. Newcastle</a></td> <td class="goal   ">46</td><td class="shotsPerGame   ">10.4</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">3</span></td><td class="possession   ">38.0</td><td class="passSuccess   ">76.0</td><td class="aerialWonPerGame   ">17.1</td><td class="  sorted "><span class="stat-value rating">6.62</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/162/Show/Angleterre-Crystal-Palace">18. Crystal Palace</a></td> <td class="goal   ">41</td><td class="shotsPerGame   ">9.2</td><td class="aaa"><span class="yellow-card-box">54</span><span class="red-card-box">2</span></td><td class="possession   ">39.9</td><td class="passSuccess   ">76.1</td><td class="aerialWonPerGame   ">18.3</td><td class="  sorted "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/175/Show/Angleterre-West-Bromwich-Albion">19. West Bromwich Albion</a></td> <td class="goal   ">35</td><td class="shotsPerGame   ">8.9</td><td class="aaa"><span class="yellow-card-box">51</span><span class="red-card-box">4</span></td><td class="possession   ">37.0</td><td class="passSuccess   ">72.2</td><td class="aerialWonPerGame   ">19.1</td><td class="  sorted "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/163/Show/Angleterre-Sheffield-United">20. Sheffield United</a></td> <td class="goal   ">20</td><td class="shotsPerGame   ">8.5</td><td class="aaa"><span class="yellow-card-box">73</span><span class="red-card-box">3</span></td><td class="possession   ">41.0</td><td class="passSuccess   ">76.9</td><td class="aerialWonPerGame   ">19.1</td><td class="  sorted "><span class="stat-value rating">6.46</span></td></tr></tbody></table></div></div>
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
collection_name = 'Stats_All_Team_General_20-21_PL'

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