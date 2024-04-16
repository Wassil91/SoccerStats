from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/37/Show/Allemagne-Bayern-Munich">1. Bayern Munich</a></td> <td class="goal   ">78</td><td class="shotsPerGame   ">20</td><td class="aaa"><span class="yellow-card-box">42</span><span class="red-card-box">2</span></td><td class="possession   ">62.0</td><td class="passSuccess   ">88.7</td><td class="aerialWonPerGame   ">12.5</td><td class="  sorted "><span class="stat-value rating">6.98</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/36/Show/Allemagne-Bayer-Leverkusen">2. Bayer Leverkusen</a></td> <td class="goal   ">68</td><td class="shotsPerGame   ">18.3</td><td class="aaa"><span class="yellow-card-box">47</span><span class="red-card-box">0</span></td><td class="possession   ">63.3</td><td class="passSuccess   ">88.9</td><td class="aerialWonPerGame   ">10.4</td><td class="  sorted "><span class="stat-value rating">6.97</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/41/Show/Allemagne-VfB-Stuttgart">3. VfB Stuttgart</a></td> <td class="goal   ">60</td><td class="shotsPerGame   ">15.7</td><td class="aaa"><span class="yellow-card-box">38</span><span class="red-card-box">1</span></td><td class="possession   ">59.9</td><td class="passSuccess   ">86.3</td><td class="aerialWonPerGame   ">14.3</td><td class="  sorted "><span class="stat-value rating">6.83</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/44/Show/Allemagne-Borussia-Dortmund">4. Borussia Dortmund</a></td> <td class="goal   ">55</td><td class="shotsPerGame   ">15.3</td><td class="aaa"><span class="yellow-card-box">43</span><span class="red-card-box">3</span></td><td class="possession   ">57.8</td><td class="passSuccess   ">85.0</td><td class="aerialWonPerGame   ">12.3</td><td class="  sorted "><span class="stat-value rating">6.78</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/7614/Show/Allemagne-RB-Leipzig">5. RB Leipzig</a></td> <td class="goal   ">60</td><td class="shotsPerGame   ">16.4</td><td class="aaa"><span class="yellow-card-box">47</span><span class="red-card-box">0</span></td><td class="possession   ">57.0</td><td class="passSuccess   ">84.6</td><td class="aerialWonPerGame   ">13.4</td><td class="  sorted "><span class="stat-value rating">6.78</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/134/Show/Allemagne-Borussia-M-Gladbach">6. Borussia M.Gladbach</a></td> <td class="goal   ">46</td><td class="shotsPerGame   ">13.6</td><td class="aaa"><span class="yellow-card-box">53</span><span class="red-card-box">2</span></td><td class="possession   ">47.1</td><td class="passSuccess   ">81.5</td><td class="aerialWonPerGame   ">14.7</td><td class="  sorted "><span class="stat-value rating">6.62</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/45/Show/Allemagne-Eintracht-Frankfurt">7. Eintracht Frankfurt</a></td> <td class="goal   ">42</td><td class="shotsPerGame   ">10.9</td><td class="aaa"><span class="yellow-card-box">48</span><span class="red-card-box">4</span></td><td class="possession   ">51.7</td><td class="passSuccess   ">80.9</td><td class="aerialWonPerGame   ">14.3</td><td class="  sorted "><span class="stat-value rating">6.61</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1211/Show/Allemagne-Hoffenheim">8. Hoffenheim</a></td> <td class="goal   ">45</td><td class="shotsPerGame   ">13.1</td><td class="aaa"><span class="yellow-card-box">67</span><span class="red-card-box">5</span></td><td class="possession   ">48.0</td><td class="passSuccess   ">79.9</td><td class="aerialWonPerGame   ">15.3</td><td class="  sorted "><span class="stat-value rating">6.60</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/50/Show/Allemagne-Freiburg">9. Freiburg</a></td> <td class="goal   ">39</td><td class="shotsPerGame   ">11.9</td><td class="aaa"><span class="yellow-card-box">45</span><span class="red-card-box">3</span></td><td class="possession   ">45.4</td><td class="passSuccess   ">78.8</td><td class="aerialWonPerGame   ">18.6</td><td class="  sorted "><span class="stat-value rating">6.59</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/42/Show/Allemagne-Werder-Bremen">10. Werder Bremen</a></td> <td class="goal   ">35</td><td class="shotsPerGame   ">11.7</td><td class="aaa"><span class="yellow-card-box">64</span><span class="red-card-box">1</span></td><td class="possession   ">47.3</td><td class="passSuccess   ">79.3</td><td class="aerialWonPerGame   ">16.7</td><td class="  sorted "><span class="stat-value rating">6.58</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1730/Show/Allemagne-Augsburg">11. Augsburg</a></td> <td class="goal   ">43</td><td class="shotsPerGame   ">12.5</td><td class="aaa"><span class="yellow-card-box">57</span><span class="red-card-box">3</span></td><td class="possession   ">43.5</td><td class="passSuccess   ">76.4</td><td class="aerialWonPerGame   ">17.9</td><td class="  sorted "><span class="stat-value rating">6.58</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/4852/Show/Allemagne-FC-Heidenheim">12. FC Heidenheim</a></td> <td class="goal   ">35</td><td class="shotsPerGame   ">11.7</td><td class="aaa"><span class="yellow-card-box">43</span><span class="red-card-box">0</span></td><td class="possession   ">42.2</td><td class="passSuccess   ">73.1</td><td class="aerialWonPerGame   ">21</td><td class="  sorted "><span class="stat-value rating">6.55</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/33/Show/Allemagne-Wolfsburg">13. Wolfsburg</a></td> <td class="goal   ">33</td><td class="shotsPerGame   ">12.3</td><td class="aaa"><span class="yellow-card-box">66</span><span class="red-card-box">5</span></td><td class="possession   ">46.6</td><td class="passSuccess   ">79.6</td><td class="aerialWonPerGame   ">15.4</td><td class="  sorted "><span class="stat-value rating">6.53</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/219/Show/Allemagne-Mainz-05">14. Mainz 05</a></td> <td class="goal   ">22</td><td class="shotsPerGame   ">13.7</td><td class="aaa"><span class="yellow-card-box">79</span><span class="red-card-box">2</span></td><td class="possession   ">45.2</td><td class="passSuccess   ">74.7</td><td class="aerialWonPerGame   ">20.1</td><td class="  sorted "><span class="stat-value rating">6.52</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/109/Show/Allemagne-Bochum">15. Bochum</a></td> <td class="goal   ">30</td><td class="shotsPerGame   ">14.5</td><td class="aaa"><span class="yellow-card-box">80</span><span class="red-card-box">1</span></td><td class="possession   ">44.8</td><td class="passSuccess   ">70.1</td><td class="aerialWonPerGame   ">23.7</td><td class="  sorted "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/796/Show/Allemagne-Union-Berlin">16. Union Berlin</a></td> <td class="goal   ">25</td><td class="shotsPerGame   ">11.7</td><td class="aaa"><span class="yellow-card-box">48</span><span class="red-card-box">6</span></td><td class="possession   ">42.3</td><td class="passSuccess   ">76.6</td><td class="aerialWonPerGame   ">18</td><td class="  sorted "><span class="stat-value rating">6.49</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/282/Show/Allemagne-FC-Koln">17. FC Koln</a></td> <td class="goal   ">20</td><td class="shotsPerGame   ">11.9</td><td class="aaa"><span class="yellow-card-box">45</span><span class="red-card-box">4</span></td><td class="possession   ">43.7</td><td class="passSuccess   ">79.0</td><td class="aerialWonPerGame   ">15.5</td><td class="  sorted "><span class="stat-value rating">6.47</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/1147/Show/Allemagne-Darmstadt">18. Darmstadt</a></td> <td class="goal   ">26</td><td class="shotsPerGame   ">12.3</td><td class="aaa"><span class="yellow-card-box">66</span><span class="red-card-box">4</span></td><td class="possession   ">46.3</td><td class="passSuccess   ">78.7</td><td class="aerialWonPerGame   ">15.3</td><td class="  sorted "><span class="stat-value rating">6.40</span></td></tr></tbody></table></div></div>
"""

# Parse the HTML content
soup = BeautifulSoup(html_content, "html.parser")

table_body = soup.find("tbody", {"id": "top-team-stats-summary-content"})
rows = table_body.find_all("tr")

# Extraction des données dans une liste de listes
data = []
headers = ['Équipe', 'Buts', 'Tirs pm', 'Discipline', 'Possession%', 'PassesRéussies%', 'AériensGagnés', 'Note']

for row in rows:
    cells = row.find_all("td")
    team_name = cells[0].text.strip()
    goals = cells[1].text.strip()
    shots_per_game = cells[2].text.strip()
    discipline = cells[3].text.strip()
    possession = cells[4].text.strip()
    passes_success = cells[5].text.strip()
    aerials_won = cells[6].text.strip()
    rating = cells[7].text.strip()
    data.append([team_name, goals, shots_per_game, discipline, possession, passes_success, aerials_won, rating])

# Création d'un DataFrame pandas
df = pd.DataFrame(data, columns=headers)

# Supprimer les colonnes "Note" et "Discipline"
df.drop(columns=["Note", "Discipline"], inplace=True)

# Ajouter une colonne "Journée" avec la valeur "26"
df.insert(0, "Journée", "26")

# Supprimer la colonne "Position" si elle existe déjà
if "Position" in df.columns:
    df.drop(columns=["Position"], inplace=True)

# Insérer une nouvelle colonne "Position" à l'index 1 (après la colonne "Journée")
df.insert(1, "Position", range(1, len(df) + 1))

# Séparer la colonne "Équipe" pour obtenir seulement le nom de l'équipe
df["Équipe"] = df["Équipe"].str.split(". ", n=1).str[1]


# Convertir les types des colonnes "Journée" et "Buts"
df['Journée'] = df['Journée'].astype(int)
df['Buts'] = df['Buts'].astype(int)

# Enregistrer le DataFrame dans un fichier CSV
df.to_csv("team_stats_summary.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_summary.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_General_bundes'

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