from bs4 import BeautifulSoup
import csv
from pymongo import MongoClient
import pandas as pd

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/167/Show/Angleterre-Manchester-City">1. Manchester City</a></td> <td class="goal   ">94</td><td class="shotsPerGame   ">15.8</td><td class="aaa"><span class="yellow-card-box">44</span><span class="red-card-box">1</span></td><td class="possession   ">65.2</td><td class="passSuccess   ">89.2</td><td class="aerialWonPerGame   ">11.6</td><td class="  sorted "><span class="stat-value rating">6.90</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/13/Show/Angleterre-Arsenal">2. Arsenal</a></td> <td class="goal   ">88</td><td class="shotsPerGame   ">15.6</td><td class="aaa"><span class="yellow-card-box">52</span><span class="red-card-box">0</span></td><td class="possession   ">59.7</td><td class="passSuccess   ">85.4</td><td class="aerialWonPerGame   ">12.9</td><td class="  sorted "><span class="stat-value rating">6.81</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/23/Show/Angleterre-Newcastle">3. Newcastle</a></td> <td class="goal   ">68</td><td class="shotsPerGame   ">15</td><td class="aaa"><span class="yellow-card-box">62</span><span class="red-card-box">1</span></td><td class="possession   ">52.2</td><td class="passSuccess   ">79.8</td><td class="aerialWonPerGame   ">14.7</td><td class="  sorted "><span class="stat-value rating">6.79</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/32/Show/Angleterre-Manchester-United">4. Manchester United</a></td> <td class="goal   ">58</td><td class="shotsPerGame   ">15.6</td><td class="aaa"><span class="yellow-card-box">78</span><span class="red-card-box">2</span></td><td class="possession   ">53.8</td><td class="passSuccess   ">82.3</td><td class="aerialWonPerGame   ">12.3</td><td class="  sorted "><span class="stat-value rating">6.77</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/26/Show/Angleterre-Liverpool">5. Liverpool</a></td> <td class="goal   ">75</td><td class="shotsPerGame   ">15.9</td><td class="aaa"><span class="yellow-card-box">57</span><span class="red-card-box">1</span></td><td class="possession   ">60.6</td><td class="passSuccess   ">84.2</td><td class="aerialWonPerGame   ">12.7</td><td class="  sorted "><span class="stat-value rating">6.75</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/211/Show/Angleterre-Brighton">6. Brighton</a></td> <td class="goal   ">72</td><td class="shotsPerGame   ">16.1</td><td class="aaa"><span class="yellow-card-box">59</span><span class="red-card-box">0</span></td><td class="possession   ">60.5</td><td class="passSuccess   ">85.9</td><td class="aerialWonPerGame   ">11.7</td><td class="  sorted "><span class="stat-value rating">6.72</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/30/Show/Angleterre-Tottenham">7. Tottenham</a></td> <td class="goal   ">70</td><td class="shotsPerGame   ">13.6</td><td class="aaa"><span class="yellow-card-box">75</span><span class="red-card-box">3</span></td><td class="possession   ">49.8</td><td class="passSuccess   ">83.4</td><td class="aerialWonPerGame   ">14.6</td><td class="  sorted "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/189/Show/Angleterre-Brentford">8. Brentford</a></td> <td class="goal   ">58</td><td class="shotsPerGame   ">10.7</td><td class="aaa"><span class="yellow-card-box">55</span><span class="red-card-box">1</span></td><td class="possession   ">43.3</td><td class="passSuccess   ">74.8</td><td class="aerialWonPerGame   ">17.6</td><td class="  sorted "><span class="stat-value rating">6.66</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/24/Show/Angleterre-Aston-Villa">9. Aston Villa</a></td> <td class="goal   ">51</td><td class="shotsPerGame   ">11.3</td><td class="aaa"><span class="yellow-card-box">80</span><span class="red-card-box">1</span></td><td class="possession   ">49.2</td><td class="passSuccess   ">81.1</td><td class="aerialWonPerGame   ">11.7</td><td class="  sorted "><span class="stat-value rating">6.64</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/15/Show/Angleterre-Chelsea">10. Chelsea</a></td> <td class="goal   ">38</td><td class="shotsPerGame   ">12.7</td><td class="aaa"><span class="yellow-card-box">77</span><span class="red-card-box">3</span></td><td class="possession   ">58.8</td><td class="passSuccess   ">85.8</td><td class="aerialWonPerGame   ">13.1</td><td class="  sorted "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/31/Show/Angleterre-Everton">11. Everton</a></td> <td class="goal   ">34</td><td class="shotsPerGame   ">11.3</td><td class="aaa"><span class="yellow-card-box">79</span><span class="red-card-box">2</span></td><td class="possession   ">42.5</td><td class="passSuccess   ">77.0</td><td class="aerialWonPerGame   ">15.3</td><td class="  sorted "><span class="stat-value rating">6.62</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/162/Show/Angleterre-Crystal-Palace">12. Crystal Palace</a></td> <td class="goal   ">40</td><td class="shotsPerGame   ">11.2</td><td class="aaa"><span class="yellow-card-box">79</span><span class="red-card-box">3</span></td><td class="possession   ">45.8</td><td class="passSuccess   ">79.3</td><td class="aerialWonPerGame   ">13.1</td><td class="  sorted "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/170/Show/Angleterre-Fulham">13. Fulham</a></td> <td class="goal   ">55</td><td class="shotsPerGame   ">11.3</td><td class="aaa"><span class="yellow-card-box">80</span><span class="red-card-box">1</span></td><td class="possession   ">48.6</td><td class="passSuccess   ">80.0</td><td class="aerialWonPerGame   ">13.5</td><td class="  sorted "><span class="stat-value rating">6.61</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/29/Show/Angleterre-West-Ham">14. West Ham</a></td> <td class="goal   ">42</td><td class="shotsPerGame   ">12.5</td><td class="aaa"><span class="yellow-card-box">44</span><span class="red-card-box">0</span></td><td class="possession   ">41.4</td><td class="passSuccess   ">78.2</td><td class="aerialWonPerGame   ">16.4</td><td class="  sorted "><span class="stat-value rating">6.60</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/14/Show/Angleterre-Leicester">15. Leicester</a></td> <td class="goal   ">51</td><td class="shotsPerGame   ">11</td><td class="aaa"><span class="yellow-card-box">59</span><span class="red-card-box">2</span></td><td class="possession   ">47.7</td><td class="passSuccess   ">80.3</td><td class="aerialWonPerGame   ">13.5</td><td class="  sorted "><span class="stat-value rating">6.57</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/183/Show/Angleterre-Bournemouth">16. Bournemouth</a></td> <td class="goal   ">37</td><td class="shotsPerGame   ">9.4</td><td class="aaa"><span class="yellow-card-box">69</span><span class="red-card-box">0</span></td><td class="possession   ">40.0</td><td class="passSuccess   ">77.5</td><td class="aerialWonPerGame   ">13.8</td><td class="  sorted "><span class="stat-value rating">6.53</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/161/Show/Angleterre-Wolves">17. Wolves</a></td> <td class="goal   ">31</td><td class="shotsPerGame   ">10.8</td><td class="aaa"><span class="yellow-card-box">84</span><span class="red-card-box">6</span></td><td class="possession   ">50.1</td><td class="passSuccess   ">81.3</td><td class="aerialWonPerGame   ">12.3</td><td class="  sorted "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/18/Show/Angleterre-Southampton">18. Southampton</a></td> <td class="goal   ">36</td><td class="shotsPerGame   ">11</td><td class="aaa"><span class="yellow-card-box">73</span><span class="red-card-box">0</span></td><td class="possession   ">44.1</td><td class="passSuccess   ">77.3</td><td class="aerialWonPerGame   ">14.6</td><td class="  sorted "><span class="stat-value rating">6.50</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/19/Show/Angleterre-Leeds">19. Leeds</a></td> <td class="goal   ">48</td><td class="shotsPerGame   ">12.2</td><td class="aaa"><span class="yellow-card-box">84</span><span class="red-card-box">3</span></td><td class="possession   ">46.3</td><td class="passSuccess   ">74.9</td><td class="aerialWonPerGame   ">15.5</td><td class="  sorted "><span class="stat-value rating">6.50</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/174/Show/Angleterre-Nottingham-Forest">20. Nottingham Forest</a></td> <td class="goal   ">38</td><td class="shotsPerGame   ">9.7</td><td class="aaa"><span class="yellow-card-box">84</span><span class="red-card-box">0</span></td><td class="possession   ">37.2</td><td class="passSuccess   ">72.3</td><td class="aerialWonPerGame   ">14.8</td><td class="  sorted "><span class="stat-value rating">6.49</span></td></tr></tbody></table></div></div>
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
collection_name = 'Stats_All_Team_General_22-23_PL'

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