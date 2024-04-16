from bs4 import BeautifulSoup
import csv
from pymongo import MongoClient
import pandas as pd

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/75/Show/Italie-Inter">1. Inter</a></td> <td class="goal   ">84</td><td class="shotsPerGame   ">17.8</td><td class="aaa"><span class="yellow-card-box">71</span><span class="red-card-box">1</span></td><td class="possession   ">56.8</td><td class="passSuccess   ">86.3</td><td class="aerialWonPerGame   ">14.6</td><td class="  sorted "><span class="stat-value rating">6.83</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/80/Show/Italie-AC-Milan">2. AC Milan</a></td> <td class="goal   ">69</td><td class="shotsPerGame   ">15.8</td><td class="aaa"><span class="yellow-card-box">73</span><span class="red-card-box">3</span></td><td class="possession   ">54.2</td><td class="passSuccess   ">83.3</td><td class="aerialWonPerGame   ">13.7</td><td class="  sorted "><span class="stat-value rating">6.81</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/276/Show/Italie-Napoli">3. Napoli</a></td> <td class="goal   ">74</td><td class="shotsPerGame   ">15.2</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">2</span></td><td class="possession   ">58.8</td><td class="passSuccess   ">86.9</td><td class="aerialWonPerGame   ">10.7</td><td class="  sorted "><span class="stat-value rating">6.78</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/87/Show/Italie-Juventus">4. Juventus</a></td> <td class="goal   ">57</td><td class="shotsPerGame   ">13.8</td><td class="aaa"><span class="yellow-card-box">75</span><span class="red-card-box">2</span></td><td class="possession   ">51.7</td><td class="passSuccess   ">84.7</td><td class="aerialWonPerGame   ">14.7</td><td class="  sorted "><span class="stat-value rating">6.72</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/300/Show/Italie-Atalanta">5. Atalanta</a></td> <td class="goal   ">65</td><td class="shotsPerGame   ">15.9</td><td class="aaa"><span class="yellow-card-box">85</span><span class="red-card-box">2</span></td><td class="possession   ">55.0</td><td class="passSuccess   ">81.9</td><td class="aerialWonPerGame   ">18.2</td><td class="  sorted "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/84/Show/Italie-Roma">6. Roma</a></td> <td class="goal   ">59</td><td class="shotsPerGame   ">15.8</td><td class="aaa"><span class="yellow-card-box">99</span><span class="red-card-box">8</span></td><td class="possession   ">51.2</td><td class="passSuccess   ">82.9</td><td class="aerialWonPerGame   ">13.9</td><td class="  sorted "><span class="stat-value rating">6.70</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/77/Show/Italie-Lazio">7. Lazio</a></td> <td class="goal   ">77</td><td class="shotsPerGame   ">11.9</td><td class="aaa"><span class="yellow-card-box">82</span><span class="red-card-box">5</span></td><td class="possession   ">55.4</td><td class="passSuccess   ">87.0</td><td class="aerialWonPerGame   ">11</td><td class="  sorted "><span class="stat-value rating">6.69</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/72/Show/Italie-Torino">8. Torino</a></td> <td class="goal   ">46</td><td class="shotsPerGame   ">12.5</td><td class="aaa"><span class="yellow-card-box">85</span><span class="red-card-box">3</span></td><td class="possession   ">53.7</td><td class="passSuccess   ">79.0</td><td class="aerialWonPerGame   ">20.6</td><td class="  sorted "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/76/Show/Italie-Verona">9. Verona</a></td> <td class="goal   ">65</td><td class="shotsPerGame   ">12.2</td><td class="aaa"><span class="yellow-card-box">90</span><span class="red-card-box">7</span></td><td class="possession   ">50.5</td><td class="passSuccess   ">76.7</td><td class="aerialWonPerGame   ">18.2</td><td class="  sorted "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/86/Show/Italie-Udinese">10. Udinese</a></td> <td class="goal   ">61</td><td class="shotsPerGame   ">13.4</td><td class="aaa"><span class="yellow-card-box">90</span><span class="red-card-box">6</span></td><td class="possession   ">42.1</td><td class="passSuccess   ">78.7</td><td class="aerialWonPerGame   ">12.1</td><td class="  sorted "><span class="stat-value rating">6.59</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2889/Show/Italie-Sassuolo">11. Sassuolo</a></td> <td class="goal   ">64</td><td class="shotsPerGame   ">15.2</td><td class="aaa"><span class="yellow-card-box">88</span><span class="red-card-box">4</span></td><td class="possession   ">55.2</td><td class="passSuccess   ">85.3</td><td class="aerialWonPerGame   ">9.2</td><td class="  sorted "><span class="stat-value rating">6.59</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/71/Show/Italie-Bologna">12. Bologna</a></td> <td class="goal   ">44</td><td class="shotsPerGame   ">11.6</td><td class="aaa"><span class="yellow-card-box">84</span><span class="red-card-box">7</span></td><td class="possession   ">50.3</td><td class="passSuccess   ">81.0</td><td class="aerialWonPerGame   ">13.3</td><td class="  sorted "><span class="stat-value rating">6.58</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/73/Show/Italie-Fiorentina">13. Fiorentina</a></td> <td class="goal   ">59</td><td class="shotsPerGame   ">13.5</td><td class="aaa"><span class="yellow-card-box">78</span><span class="red-card-box">8</span></td><td class="possession   ">58.0</td><td class="passSuccess   ">85.4</td><td class="aerialWonPerGame   ">12.5</td><td class="  sorted "><span class="stat-value rating">6.57</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/271/Show/Italie-Sampdoria">14. Sampdoria</a></td> <td class="goal   ">46</td><td class="shotsPerGame   ">10.3</td><td class="aaa"><span class="yellow-card-box">92</span><span class="red-card-box">4</span></td><td class="possession   ">45.7</td><td class="passSuccess   ">77.8</td><td class="aerialWonPerGame   ">16.3</td><td class="  sorted "><span class="stat-value rating">6.54</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/272/Show/Italie-Empoli">15. Empoli</a></td> <td class="goal   ">50</td><td class="shotsPerGame   ">13.1</td><td class="aaa"><span class="yellow-card-box">81</span><span class="red-card-box">5</span></td><td class="possession   ">46.9</td><td class="passSuccess   ">78.9</td><td class="aerialWonPerGame   ">11.4</td><td class="  sorted "><span class="stat-value rating">6.52</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1501/Show/Italie-Spezia">16. Spezia</a></td> <td class="goal   ">41</td><td class="shotsPerGame   ">10.2</td><td class="aaa"><span class="yellow-card-box">87</span><span class="red-card-box">4</span></td><td class="possession   ">42.4</td><td class="passSuccess   ">76.7</td><td class="aerialWonPerGame   ">14.2</td><td class="  sorted "><span class="stat-value rating">6.49</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/78/Show/Italie-Cagliari">17. Cagliari</a></td> <td class="goal   ">34</td><td class="shotsPerGame   ">11.2</td><td class="aaa"><span class="yellow-card-box">90</span><span class="red-card-box">5</span></td><td class="possession   ">43.9</td><td class="passSuccess   ">75.2</td><td class="aerialWonPerGame   ">18.6</td><td class="  sorted "><span class="stat-value rating">6.48</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/278/Show/Italie-Genoa">18. Genoa</a></td> <td class="goal   ">27</td><td class="shotsPerGame   ">10.6</td><td class="aaa"><span class="yellow-card-box">88</span><span class="red-card-box">3</span></td><td class="possession   ">43.3</td><td class="passSuccess   ">73.7</td><td class="aerialWonPerGame   ">16.8</td><td class="  sorted "><span class="stat-value rating">6.48</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/85/Show/Italie-Venezia">19. Venezia</a></td> <td class="goal   ">34</td><td class="shotsPerGame   ">9.3</td><td class="aaa"><span class="yellow-card-box">106</span><span class="red-card-box">9</span></td><td class="possession   ">41.8</td><td class="passSuccess   ">77.5</td><td class="aerialWonPerGame   ">15</td><td class="  sorted "><span class="stat-value rating">6.45</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/143/Show/Italie-Salernitana">20. Salernitana</a></td> <td class="goal   ">33</td><td class="shotsPerGame   ">11.3</td><td class="aaa"><span class="yellow-card-box">88</span><span class="red-card-box">5</span></td><td class="possession   ">40.2</td><td class="passSuccess   ">76.5</td><td class="aerialWonPerGame   ">17.5</td><td class="  sorted "><span class="stat-value rating">6.43</span></td></tr></tbody></table></div></div>
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
collection_name = 'Stats_All_Team_General_21-22_serieA'

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