from bs4 import BeautifulSoup
import csv
from pymongo import MongoClient
import pandas as pd

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/167/Show/Angleterre-Manchester-City">1. Manchester City</a></td> <td class="goal   ">63</td><td class="shotsPerGame   ">17.8</td><td class="aaa"><span class="yellow-card-box">46</span><span class="red-card-box">2</span></td><td class="possession   ">65.5</td><td class="passSuccess   ">90.0</td><td class="aerialWonPerGame   ">9.4</td><td class="  sorted "><span class="stat-value rating">6.93</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/26/Show/Angleterre-Liverpool">2. Liverpool</a></td> <td class="goal   ">67</td><td class="shotsPerGame   ">19.7</td><td class="aaa"><span class="yellow-card-box">54</span><span class="red-card-box">5</span></td><td class="possession   ">59.9</td><td class="passSuccess   ">85.2</td><td class="aerialWonPerGame   ">15.9</td><td class="  sorted "><span class="stat-value rating">6.87</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/13/Show/Angleterre-Arsenal">3. Arsenal</a></td> <td class="goal   ">70</td><td class="shotsPerGame   ">16.7</td><td class="aaa"><span class="yellow-card-box">43</span><span class="red-card-box">2</span></td><td class="possession   ">60.7</td><td class="passSuccess   ">87.0</td><td class="aerialWonPerGame   ">13.6</td><td class="  sorted "><span class="stat-value rating">6.84</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/30/Show/Angleterre-Tottenham">4. Tottenham</a></td> <td class="goal   ">61</td><td class="shotsPerGame   ">15.5</td><td class="aaa"><span class="yellow-card-box">65</span><span class="red-card-box">4</span></td><td class="possession   ">61.5</td><td class="passSuccess   ">87.2</td><td class="aerialWonPerGame   ">10</td><td class="  sorted "><span class="stat-value rating">6.79</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/23/Show/Angleterre-Newcastle">5. Newcastle</a></td> <td class="goal   ">63</td><td class="shotsPerGame   ">13.9</td><td class="aaa"><span class="yellow-card-box">60</span><span class="red-card-box">1</span></td><td class="possession   ">53.0</td><td class="passSuccess   ">83.2</td><td class="aerialWonPerGame   ">11.9</td><td class="  sorted "><span class="stat-value rating">6.74</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/29/Show/Angleterre-West-Ham">6. West Ham</a></td> <td class="goal   ">49</td><td class="shotsPerGame   ">11.9</td><td class="aaa"><span class="yellow-card-box">65</span><span class="red-card-box">3</span></td><td class="possession   ">40.9</td><td class="passSuccess   ">78.6</td><td class="aerialWonPerGame   ">15.9</td><td class="  sorted "><span class="stat-value rating">6.74</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/24/Show/Angleterre-Aston-Villa">7. Aston Villa</a></td> <td class="goal   ">62</td><td class="shotsPerGame   ">14.4</td><td class="aaa"><span class="yellow-card-box">79</span><span class="red-card-box">2</span></td><td class="possession   ">54.7</td><td class="passSuccess   ">85.8</td><td class="aerialWonPerGame   ">9.3</td><td class="  sorted "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/15/Show/Angleterre-Chelsea">8. Chelsea</a></td> <td class="goal   ">49</td><td class="shotsPerGame   ">13.9</td><td class="aaa"><span class="yellow-card-box">84</span><span class="red-card-box">3</span></td><td class="possession   ">58.9</td><td class="passSuccess   ">87.2</td><td class="aerialWonPerGame   ">11.8</td><td class="  sorted "><span class="stat-value rating">6.70</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/32/Show/Angleterre-Manchester-United">9. Manchester United</a></td> <td class="goal   ">40</td><td class="shotsPerGame   ">14.2</td><td class="aaa"><span class="yellow-card-box">67</span><span class="red-card-box">1</span></td><td class="possession   ">50.1</td><td class="passSuccess   ">82.6</td><td class="aerialWonPerGame   ">11.7</td><td class="  sorted "><span class="stat-value rating">6.69</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/31/Show/Angleterre-Everton">10. Everton</a></td> <td class="goal   ">30</td><td class="shotsPerGame   ">13.9</td><td class="aaa"><span class="yellow-card-box">59</span><span class="red-card-box">1</span></td><td class="possession   ">39.9</td><td class="passSuccess   ">75.5</td><td class="aerialWonPerGame   ">18.3</td><td class="  sorted "><span class="stat-value rating">6.66</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/161/Show/Angleterre-Wolves">11. Wolves</a></td> <td class="goal   ">42</td><td class="shotsPerGame   ">11.8</td><td class="aaa"><span class="yellow-card-box">72</span><span class="red-card-box">3</span></td><td class="possession   ">47.7</td><td class="passSuccess   ">81.8</td><td class="aerialWonPerGame   ">12.1</td><td class="  sorted "><span class="stat-value rating">6.66</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/162/Show/Angleterre-Crystal-Palace">12. Crystal Palace</a></td> <td class="goal   ">34</td><td class="shotsPerGame   ">11.7</td><td class="aaa"><span class="yellow-card-box">53</span><span class="red-card-box">1</span></td><td class="possession   ">41.1</td><td class="passSuccess   ">78.9</td><td class="aerialWonPerGame   ">15.2</td><td class="  sorted "><span class="stat-value rating">6.64</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/170/Show/Angleterre-Fulham">13. Fulham</a></td> <td class="goal   ">46</td><td class="shotsPerGame   ">13.3</td><td class="aaa"><span class="yellow-card-box">66</span><span class="red-card-box">3</span></td><td class="possession   ">50.3</td><td class="passSuccess   ">82.7</td><td class="aerialWonPerGame   ">13.3</td><td class="  sorted "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/183/Show/Angleterre-Bournemouth">14. Bournemouth</a></td> <td class="goal   ">43</td><td class="shotsPerGame   ">14.2</td><td class="aaa"><span class="yellow-card-box">58</span><span class="red-card-box">2</span></td><td class="possession   ">44.5</td><td class="passSuccess   ">77.1</td><td class="aerialWonPerGame   ">16</td><td class="  sorted "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/211/Show/Angleterre-Brighton">15. Brighton</a></td> <td class="goal   ">51</td><td class="shotsPerGame   ">14.8</td><td class="aaa"><span class="yellow-card-box">74</span><span class="red-card-box">3</span></td><td class="possession   ">62.3</td><td class="passSuccess   ">89.1</td><td class="aerialWonPerGame   ">11.4</td><td class="  sorted "><span class="stat-value rating">6.62</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/189/Show/Angleterre-Brentford">16. Brentford</a></td> <td class="goal   ">42</td><td class="shotsPerGame   ">13.1</td><td class="aaa"><span class="yellow-card-box">72</span><span class="red-card-box">2</span></td><td class="possession   ">44.4</td><td class="passSuccess   ">76.4</td><td class="aerialWonPerGame   ">17.3</td><td class="  sorted "><span class="stat-value rating">6.59</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/95/Show/Angleterre-Luton">17. Luton</a></td> <td class="goal   ">43</td><td class="shotsPerGame   ">11.7</td><td class="aaa"><span class="yellow-card-box">59</span><span class="red-card-box">0</span></td><td class="possession   ">41.3</td><td class="passSuccess   ">75.1</td><td class="aerialWonPerGame   ">17.2</td><td class="  sorted "><span class="stat-value rating">6.58</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/174/Show/Angleterre-Nottingham-Forest">18. Nottingham Forest</a></td> <td class="goal   ">36</td><td class="shotsPerGame   ">11.2</td><td class="aaa"><span class="yellow-card-box">66</span><span class="red-card-box">3</span></td><td class="possession   ">40.4</td><td class="passSuccess   ">78.2</td><td class="aerialWonPerGame   ">16</td><td class="  sorted "><span class="stat-value rating">6.57</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/184/Show/Angleterre-Burnley">19. Burnley</a></td> <td class="goal   ">31</td><td class="shotsPerGame   ">10.9</td><td class="aaa"><span class="yellow-card-box">58</span><span class="red-card-box">6</span></td><td class="possession   ">45.6</td><td class="passSuccess   ">79.0</td><td class="aerialWonPerGame   ">15.8</td><td class="  sorted "><span class="stat-value rating">6.44</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/163/Show/Angleterre-Sheffield-United">20. Sheffield United</a></td> <td class="goal   ">27</td><td class="shotsPerGame   ">9</td><td class="aaa"><span class="yellow-card-box">83</span><span class="red-card-box">4</span></td><td class="possession   ">34.5</td><td class="passSuccess   ">71.3</td><td class="aerialWonPerGame   ">16.7</td><td class="  sorted "><span class="stat-value rating">6.36</span></td></tr></tbody></table></div></div>
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
collection_name = 'Stats_All_Team_General_PL'

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