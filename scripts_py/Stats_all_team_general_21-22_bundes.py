import csv
import pandas as pd
from bs4 import BeautifulSoup
from pymongo import MongoClient

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/37/Show/Allemagne-Bayern-Munich">1. Bayern Munich</a></td> <td class="goal   ">97</td><td class="shotsPerGame   ">19.8</td><td class="aaa"><span class="yellow-card-box">36</span><span class="red-card-box">2</span></td><td class="possession   ">64.8</td><td class="passSuccess   ">86.0</td><td class="aerialWonPerGame   ">12.5</td><td class="  sorted "><span class="stat-value rating">6.98</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/36/Show/Allemagne-Bayer-Leverkusen">2. Bayer Leverkusen</a></td> <td class="goal   ">80</td><td class="shotsPerGame   ">13.5</td><td class="aaa"><span class="yellow-card-box">64</span><span class="red-card-box">2</span></td><td class="possession   ">53.7</td><td class="passSuccess   ">81.8</td><td class="aerialWonPerGame   ">13.1</td><td class="  sorted "><span class="stat-value rating">6.77</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/44/Show/Allemagne-Borussia-Dortmund">3. Borussia Dortmund</a></td> <td class="goal   ">85</td><td class="shotsPerGame   ">13.3</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">1</span></td><td class="possession   ">59.4</td><td class="passSuccess   ">84.0</td><td class="aerialWonPerGame   ">12.5</td><td class="  sorted "><span class="stat-value rating">6.77</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/7614/Show/Allemagne-RB-Leipzig">4. RB Leipzig</a></td> <td class="goal   ">72</td><td class="shotsPerGame   ">12.9</td><td class="aaa"><span class="yellow-card-box">49</span><span class="red-card-box">0</span></td><td class="possession   ">56.5</td><td class="passSuccess   ">83.1</td><td class="aerialWonPerGame   ">14.9</td><td class="  sorted "><span class="stat-value rating">6.77</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/50/Show/Allemagne-Freiburg">5. Freiburg</a></td> <td class="goal   ">58</td><td class="shotsPerGame   ">13.6</td><td class="aaa"><span class="yellow-card-box">34</span><span class="red-card-box">0</span></td><td class="possession   ">48.6</td><td class="passSuccess   ">76.2</td><td class="aerialWonPerGame   ">19.9</td><td class="  sorted "><span class="stat-value rating">6.68</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/134/Show/Allemagne-Borussia-M-Gladbach">6. Borussia M.Gladbach</a></td> <td class="goal   ">52</td><td class="shotsPerGame   ">14.8</td><td class="aaa"><span class="yellow-card-box">66</span><span class="red-card-box">1</span></td><td class="possession   ">54.1</td><td class="passSuccess   ">82.0</td><td class="aerialWonPerGame   ">14.3</td><td class="  sorted "><span class="stat-value rating">6.67</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/282/Show/Allemagne-FC-Koln">7. FC Koln</a></td> <td class="goal   ">52</td><td class="shotsPerGame   ">13.8</td><td class="aaa"><span class="yellow-card-box">66</span><span class="red-card-box">1</span></td><td class="possession   ">54.8</td><td class="passSuccess   ">77.4</td><td class="aerialWonPerGame   ">17.8</td><td class="  sorted "><span class="stat-value rating">6.65</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/219/Show/Allemagne-Mainz-05">8. Mainz 05</a></td> <td class="goal   ">50</td><td class="shotsPerGame   ">13.8</td><td class="aaa"><span class="yellow-card-box">58</span><span class="red-card-box">4</span></td><td class="possession   ">46.0</td><td class="passSuccess   ">74.1</td><td class="aerialWonPerGame   ">18</td><td class="  sorted "><span class="stat-value rating">6.64</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/33/Show/Allemagne-Wolfsburg">9. Wolfsburg</a></td> <td class="goal   ">43</td><td class="shotsPerGame   ">12.4</td><td class="aaa"><span class="yellow-card-box">58</span><span class="red-card-box">3</span></td><td class="possession   ">50.2</td><td class="passSuccess   ">78.6</td><td class="aerialWonPerGame   ">18.4</td><td class="  sorted "><span class="stat-value rating">6.63</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/41/Show/Allemagne-VfB-Stuttgart">10. VfB Stuttgart</a></td> <td class="goal   ">41</td><td class="shotsPerGame   ">13.3</td><td class="aaa"><span class="yellow-card-box">62</span><span class="red-card-box">2</span></td><td class="possession   ">50.4</td><td class="passSuccess   ">80.7</td><td class="aerialWonPerGame   ">17.3</td><td class="  sorted "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/796/Show/Allemagne-Union-Berlin">11. Union Berlin</a></td> <td class="goal   ">50</td><td class="shotsPerGame   ">12.1</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">1</span></td><td class="possession   ">43.3</td><td class="passSuccess   ">73.6</td><td class="aerialWonPerGame   ">17.4</td><td class="  sorted "><span class="stat-value rating">6.61</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/45/Show/Allemagne-Eintracht-Frankfurt">12. Eintracht Frankfurt</a></td> <td class="goal   ">45</td><td class="shotsPerGame   ">13.2</td><td class="aaa"><span class="yellow-card-box">59</span><span class="red-card-box">1</span></td><td class="possession   ">49.4</td><td class="passSuccess   ">76.2</td><td class="aerialWonPerGame   ">16.4</td><td class="  sorted "><span class="stat-value rating">6.60</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1211/Show/Allemagne-Hoffenheim">13. Hoffenheim</a></td> <td class="goal   ">58</td><td class="shotsPerGame   ">13.3</td><td class="aaa"><span class="yellow-card-box">75</span><span class="red-card-box">0</span></td><td class="possession   ">53.2</td><td class="passSuccess   ">80.7</td><td class="aerialWonPerGame   ">14.5</td><td class="  sorted "><span class="stat-value rating">6.59</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/109/Show/Allemagne-Bochum">14. Bochum</a></td> <td class="goal   ">38</td><td class="shotsPerGame   ">12.1</td><td class="aaa"><span class="yellow-card-box">53</span><span class="red-card-box">2</span></td><td class="possession   ">44.5</td><td class="passSuccess   ">72.1</td><td class="aerialWonPerGame   ">18.7</td><td class="  sorted "><span class="stat-value rating">6.58</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1730/Show/Allemagne-Augsburg">15. Augsburg</a></td> <td class="goal   ">39</td><td class="shotsPerGame   ">10.8</td><td class="aaa"><span class="yellow-card-box">74</span><span class="red-card-box">0</span></td><td class="possession   ">40.6</td><td class="passSuccess   ">72.0</td><td class="aerialWonPerGame   ">16.6</td><td class="  sorted "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/40/Show/Allemagne-Arminia-Bielefeld">16. Arminia Bielefeld</a></td> <td class="goal   ">27</td><td class="shotsPerGame   ">10.7</td><td class="aaa"><span class="yellow-card-box">53</span><span class="red-card-box">2</span></td><td class="possession   ">39.9</td><td class="passSuccess   ">71.7</td><td class="aerialWonPerGame   ">19.6</td><td class="  sorted "><span class="stat-value rating">6.50</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/47/Show/Allemagne-Hertha-Berlin">17. Hertha Berlin</a></td> <td class="goal   ">37</td><td class="shotsPerGame   ">10.8</td><td class="aaa"><span class="yellow-card-box">62</span><span class="red-card-box">2</span></td><td class="possession   ">43.2</td><td class="passSuccess   ">74.7</td><td class="aerialWonPerGame   ">16.6</td><td class="  sorted "><span class="stat-value rating">6.48</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/89/Show/Allemagne-Greuther-Fuerth">18. Greuther Fuerth</a></td> <td class="goal   ">28</td><td class="shotsPerGame   ">9.2</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">0</span></td><td class="possession   ">43.0</td><td class="passSuccess   ">74.8</td><td class="aerialWonPerGame   ">14.1</td><td class="  sorted "><span class="stat-value rating">6.38</span></td></tr></tbody></table></div></div>
"""
# Parser le contenu HTML
# Parser le contenu HTML
soup = BeautifulSoup(html_content, "html.parser")

# Extraire les données de la table HTML
table_body = soup.find("tbody", {"id": "top-team-stats-summary-content"})
rows = table_body.find_all("tr")

data = []
headers = ['Équipe', 'Buts', 'Tirs pm', 'Discipline', 'Possession%', 'PassesRéussies%', 'AériensGagnés', 'Note']

for row in rows:
    cells = row.find_all("td")
    team_name = cells[0].text.strip().split(". ")[1]  # Extraction du nom sans le numéro de classement
    goals = cells[1].text.strip()
    shots_per_game = cells[2].text.strip()
    discipline = cells[3].text.strip().split()[0]  # Only extracting yellow cards
    possession = cells[4].text.strip()
    passes_success = cells[5].text.strip()
    aerials_won = cells[6].text.strip()
    rating = cells[7].text.strip()
    data.append([team_name, goals, shots_per_game, discipline, possession, passes_success, aerials_won, rating])

# Enregistrer les données dans un fichier CSV
with open('team_stats_summary.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(headers)
    writer.writerows(data)

print("Les données ont été enregistrées dans team_stats_summary.csv.")

# Créer un DataFrame pandas
df = pd.DataFrame(data, columns=headers)

# Supprimer les colonnes "Note" et "Discipline"
df.drop(columns=["Note", "Discipline"], inplace=True)

# Ajouter une colonne "Journée" avec la valeur "26"
df.insert(0, "Journée", "38")

# Supprimer la colonne "Position" si elle existe déjà
if "Position" in df.columns:
    df.drop(columns=["Position"], inplace=True)

# Insérer une nouvelle colonne "Position" à l'index 1 (après la colonne "Journée")
df.insert(1, "Position", range(1, len(df) + 1))


# Convertir les types des colonnes "Journée" et "Buts"
df['Journée'] = df['Journée'].astype(int)
df['Buts'] = df['Buts'].astype(int)

# Enregistrer le DataFrame dans un fichier CSV (avec les modifications)
df.to_csv("team_stats_summary_modified.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_summary_modified.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_General_21-22_bundes'

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