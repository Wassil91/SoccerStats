from bs4 import BeautifulSoup
import csv
from pymongo import MongoClient
import pandas as pd

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/167/Show/Angleterre-Manchester-City">1. Manchester City</a></td> <td class="goal   ">99</td><td class="shotsPerGame   ">18.8</td><td class="aaa"><span class="yellow-card-box">42</span><span class="red-card-box">1</span></td><td class="possession   ">68.2</td><td class="passSuccess   ">89.7</td><td class="aerialWonPerGame   ">12.7</td><td class="  sorted "><span class="stat-value rating">7.12</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/26/Show/Angleterre-Liverpool">2. Liverpool</a></td> <td class="goal   ">94</td><td class="shotsPerGame   ">19.2</td><td class="aaa"><span class="yellow-card-box">50</span><span class="red-card-box">1</span></td><td class="possession   ">63.1</td><td class="passSuccess   ">84.9</td><td class="aerialWonPerGame   ">15.1</td><td class="  sorted "><span class="stat-value rating">7.05</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/15/Show/Angleterre-Chelsea">3. Chelsea</a></td> <td class="goal   ">76</td><td class="shotsPerGame   ">15.6</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">1</span></td><td class="possession   ">62.2</td><td class="passSuccess   ">87.1</td><td class="aerialWonPerGame   ">14.3</td><td class="  sorted "><span class="stat-value rating">6.92</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/30/Show/Angleterre-Tottenham">4. Tottenham</a></td> <td class="goal   ">69</td><td class="shotsPerGame   ">12.8</td><td class="aaa"><span class="yellow-card-box">66</span><span class="red-card-box">1</span></td><td class="possession   ">51.6</td><td class="passSuccess   ">84.9</td><td class="aerialWonPerGame   ">14.7</td><td class="  sorted "><span class="stat-value rating">6.87</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/29/Show/Angleterre-West-Ham">5. West Ham</a></td> <td class="goal   ">60</td><td class="shotsPerGame   ">11.8</td><td class="aaa"><span class="yellow-card-box">47</span><span class="red-card-box">3</span></td><td class="possession   ">47.4</td><td class="passSuccess   ">80.6</td><td class="aerialWonPerGame   ">16.9</td><td class="  sorted "><span class="stat-value rating">6.75</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/13/Show/Angleterre-Arsenal">6. Arsenal</a></td> <td class="goal   ">61</td><td class="shotsPerGame   ">15.5</td><td class="aaa"><span class="yellow-card-box">60</span><span class="red-card-box">4</span></td><td class="possession   ">52.6</td><td class="passSuccess   ">83.4</td><td class="aerialWonPerGame   ">12.4</td><td class="  sorted "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/32/Show/Angleterre-Manchester-United">7. Manchester United</a></td> <td class="goal   ">57</td><td class="shotsPerGame   ">13.4</td><td class="aaa"><span class="yellow-card-box">75</span><span class="red-card-box">2</span></td><td class="possession   ">52.1</td><td class="passSuccess   ">82.8</td><td class="aerialWonPerGame   ">14.7</td><td class="  sorted "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/162/Show/Angleterre-Crystal-Palace">8. Crystal Palace</a></td> <td class="goal   ">50</td><td class="shotsPerGame   ">10.8</td><td class="aaa"><span class="yellow-card-box">67</span><span class="red-card-box">1</span></td><td class="possession   ">50.8</td><td class="passSuccess   ">80.3</td><td class="aerialWonPerGame   ">16.1</td><td class="  sorted "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/14/Show/Angleterre-Leicester">9. Leicester</a></td> <td class="goal   ">62</td><td class="shotsPerGame   ">11.4</td><td class="aaa"><span class="yellow-card-box">55</span><span class="red-card-box">1</span></td><td class="possession   ">51.8</td><td class="passSuccess   ">81.8</td><td class="aerialWonPerGame   ">14</td><td class="  sorted "><span class="stat-value rating">6.70</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/211/Show/Angleterre-Brighton">10. Brighton</a></td> <td class="goal   ">42</td><td class="shotsPerGame   ">12.9</td><td class="aaa"><span class="yellow-card-box">73</span><span class="red-card-box">2</span></td><td class="possession   ">54.3</td><td class="passSuccess   ">81.7</td><td class="aerialWonPerGame   ">15.1</td><td class="  sorted "><span class="stat-value rating">6.70</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/161/Show/Angleterre-Wolves">11. Wolves</a></td> <td class="goal   ">38</td><td class="shotsPerGame   ">10.6</td><td class="aaa"><span class="yellow-card-box">59</span><span class="red-card-box">2</span></td><td class="possession   ">49.3</td><td class="passSuccess   ">81.3</td><td class="aerialWonPerGame   ">12.2</td><td class="  sorted "><span class="stat-value rating">6.69</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/24/Show/Angleterre-Aston-Villa">12. Aston Villa</a></td> <td class="goal   ">52</td><td class="shotsPerGame   ">12.2</td><td class="aaa"><span class="yellow-card-box">79</span><span class="red-card-box">2</span></td><td class="possession   ">46.3</td><td class="passSuccess   ">79.7</td><td class="aerialWonPerGame   ">14.3</td><td class="  sorted "><span class="stat-value rating">6.68</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/184/Show/Angleterre-Burnley">13. Burnley</a></td> <td class="goal   ">34</td><td class="shotsPerGame   ">10.7</td><td class="aaa"><span class="yellow-card-box">68</span><span class="red-card-box">2</span></td><td class="possession   ">39.1</td><td class="passSuccess   ">69.2</td><td class="aerialWonPerGame   ">21.9</td><td class="  sorted "><span class="stat-value rating">6.65</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/189/Show/Angleterre-Brentford">14. Brentford</a></td> <td class="goal   ">48</td><td class="shotsPerGame   ">11.6</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">3</span></td><td class="possession   ">44.3</td><td class="passSuccess   ">73.7</td><td class="aerialWonPerGame   ">19</td><td class="  sorted "><span class="stat-value rating">6.65</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/18/Show/Angleterre-Southampton">15. Southampton</a></td> <td class="goal   ">43</td><td class="shotsPerGame   ">12.7</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">2</span></td><td class="possession   ">47.4</td><td class="passSuccess   ">76.6</td><td class="aerialWonPerGame   ">17.8</td><td class="  sorted "><span class="stat-value rating">6.63</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/23/Show/Angleterre-Newcastle">16. Newcastle</a></td> <td class="goal   ">43</td><td class="shotsPerGame   ">11.7</td><td class="aaa"><span class="yellow-card-box">76</span><span class="red-card-box">1</span></td><td class="possession   ">39.4</td><td class="passSuccess   ">74.5</td><td class="aerialWonPerGame   ">17.5</td><td class="  sorted "><span class="stat-value rating">6.60</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/31/Show/Angleterre-Everton">17. Everton</a></td> <td class="goal   ">43</td><td class="shotsPerGame   ">11.5</td><td class="aaa"><span class="yellow-card-box">78</span><span class="red-card-box">6</span></td><td class="possession   ">39.1</td><td class="passSuccess   ">73.3</td><td class="aerialWonPerGame   ">16.8</td><td class="  sorted "><span class="stat-value rating">6.59</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/19/Show/Angleterre-Leeds">18. Leeds</a></td> <td class="goal   ">42</td><td class="shotsPerGame   ">12.8</td><td class="aaa"><span class="yellow-card-box">101</span><span class="red-card-box">3</span></td><td class="possession   ">51.9</td><td class="passSuccess   ">78.0</td><td class="aerialWonPerGame   ">12.6</td><td class="  sorted "><span class="stat-value rating">6.53</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/27/Show/Angleterre-Watford">19. Watford</a></td> <td class="goal   ">34</td><td class="shotsPerGame   ">10.5</td><td class="aaa"><span class="yellow-card-box">57</span><span class="red-card-box">3</span></td><td class="possession   ">39.7</td><td class="passSuccess   ">72.9</td><td class="aerialWonPerGame   ">18.9</td><td class="  sorted "><span class="stat-value rating">6.53</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/168/Show/Angleterre-Norwich">20. Norwich</a></td> <td class="goal   ">23</td><td class="shotsPerGame   ">9.8</td><td class="aaa"><span class="yellow-card-box">55</span><span class="red-card-box">1</span></td><td class="possession   ">42.3</td><td class="passSuccess   ">77.5</td><td class="aerialWonPerGame   ">13</td><td class="  sorted "><span class="stat-value rating">6.42</span></td></tr></tbody></table></div></div>
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
collection_name = 'Stats_All_Team_General_21-22_PL'

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