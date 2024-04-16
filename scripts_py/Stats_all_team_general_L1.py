from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1">
    <div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table">
        <table class="grid with-centered-columns hover" id="top-team-stats-summary-grid">
            <thead>
                <tr>
                    <th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th>
                     <th class="global sortable goal   " data-stat-name="goal">Buts</th>
                     <th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th>
                     <th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th>
                     <th class="global sortable possession   " data-stat-name="possession">Possession%</th>
                     <th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th>
                     <th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th>
                     <th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr>
                    </thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text">
                        <a class="team-link" href="/Teams/304/Show/France-Paris-Saint-Germain">1. Paris Saint-Germain</a>
                    </td> 
                    <td class="goal   ">62</td><td class="shotsPerGame   ">15</td>
                    <td class="aaa"><span class="yellow-card-box">37</span>
                    <span class="red-card-box">2</span></td><td class="possession   ">65.6</td>
                    <td class="passSuccess   ">89.8</td><td class="aerialWonPerGame   ">8</td>
                    <td class="  sorted "><span class="stat-value rating">6.95</span></td>
                </tr>
                <tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/248/Show/France-Monaco">2. Monaco</a></td> <td class="goal   ">47</td><td class="shotsPerGame   ">15.1</td><td class="aaa"><span class="yellow-card-box">60</span><span class="red-card-box">5</span></td><td class="possession   ">53.7</td><td class="passSuccess   ">82.4</td><td class="aerialWonPerGame   ">12.3</td><td class="  sorted "><span class="stat-value rating">6.76</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/607/Show/France-Lille">3. Lille</a></td> <td class="goal   ">37</td><td class="shotsPerGame   ">13.4</td><td class="aaa"><span class="yellow-card-box">47</span><span class="red-card-box">2</span></td><td class="possession   ">57.3</td><td class="passSuccess   ">86.0</td><td class="aerialWonPerGame   ">10.6</td><td class="  sorted "><span class="stat-value rating">6.76</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2332/Show/France-Brest">4. Brest</a></td> <td class="goal   ">36</td><td class="shotsPerGame   ">14.2</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">3</span></td><td class="possession   ">53.3</td><td class="passSuccess   ">81.3</td><td class="aerialWonPerGame   ">17.6</td><td class="  sorted "><span class="stat-value rating">6.76</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/249/Show/France-Marseille">5. Marseille</a></td> <td class="goal   ">40</td><td class="shotsPerGame   ">13.7</td><td class="aaa"><span class="yellow-card-box">43</span><span class="red-card-box">4</span></td><td class="possession   ">54.1</td><td class="passSuccess   ">84.7</td><td class="aerialWonPerGame   ">11.3</td><td class="  sorted "><span class="stat-value rating">6.73</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/313/Show/France-Rennes">6. Rennes</a></td> <td class="goal   ">40</td><td class="shotsPerGame   ">13.7</td><td class="aaa"><span class="yellow-card-box">46</span><span class="red-card-box">3</span></td><td class="possession   ">50.8</td><td class="passSuccess   ">83.9</td><td class="aerialWonPerGame   ">11.3</td><td class="  sorted "><span class="stat-value rating">6.67</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/613/Show/France-Nice">7. Nice</a></td> <td class="goal   ">27</td><td class="shotsPerGame   ">13.7</td><td class="aaa"><span class="yellow-card-box">47</span><span class="red-card-box">3</span></td><td class="possession   ">53.4</td><td class="passSuccess   ">86.5</td><td class="aerialWonPerGame   ">11.6</td><td class="  sorted "><span class="stat-value rating">6.65</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/950/Show/France-Reims">8. Reims</a></td> <td class="goal   ">34</td><td class="shotsPerGame   ">12.5</td><td class="aaa"><span class="yellow-card-box">47</span><span class="red-card-box">1</span></td><td class="possession   ">51.9</td><td class="passSuccess   ">81.7</td><td class="aerialWonPerGame   ">12.2</td><td class="  sorted "><span class="stat-value rating">6.63</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/309/Show/France-Lens">9. Lens</a></td> <td class="goal   ">35</td><td class="shotsPerGame   ">13.9</td><td class="aaa"><span class="yellow-card-box">60</span><span class="red-card-box">4</span></td><td class="possession   ">52.1</td><td class="passSuccess   ">83.7</td><td class="aerialWonPerGame   ">11.5</td><td class="  sorted "><span class="stat-value rating">6.61</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/311/Show/France-Montpellier">10. Montpellier</a></td> <td class="goal   ">31</td><td class="shotsPerGame   ">13.6</td><td class="aaa"><span class="yellow-card-box">51</span><span class="red-card-box">2</span></td><td class="possession   ">43.7</td><td class="passSuccess   ">78.1</td><td class="aerialWonPerGame   ">13.1</td><td class="  sorted "><span class="stat-value rating">6.61</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/246/Show/France-Toulouse">11. Toulouse</a></td> <td class="goal   ">29</td><td class="shotsPerGame   ">11.8</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">0</span></td><td class="possession   ">48.4</td><td class="passSuccess   ">81.3</td><td class="aerialWonPerGame   ">15</td><td class="  sorted "><span class="stat-value rating">6.60</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/228/Show/France-Lyon">12. Lyon</a></td> <td class="goal   ">30</td><td class="shotsPerGame   ">12.7</td><td class="aaa"><span class="yellow-card-box">42</span><span class="red-card-box">5</span></td><td class="possession   ">51.3</td><td class="passSuccess   ">83.6</td><td class="aerialWonPerGame   ">11.5</td><td class="  sorted "><span class="stat-value rating">6.59</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/146/Show/France-Lorient">13. Lorient</a></td> <td class="goal   ">35</td><td class="shotsPerGame   ">9.5</td><td class="aaa"><span class="yellow-card-box">45</span><span class="red-card-box">1</span></td><td class="possession   ">45.5</td><td class="passSuccess   ">83.7</td><td class="aerialWonPerGame   ">12</td><td class="  sorted "><span class="stat-value rating">6.58</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/148/Show/France-Strasbourg">14. Strasbourg</a></td> <td class="goal   ">28</td><td class="shotsPerGame   ">11</td><td class="aaa"><span class="yellow-card-box">46</span><span class="red-card-box">2</span></td><td class="possession   ">42.3</td><td class="passSuccess   ">81.1</td><td class="aerialWonPerGame   ">11.5</td><td class="  sorted "><span class="stat-value rating">6.54</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/217/Show/France-Le-Havre">15. Le Havre</a></td> <td class="goal   ">26</td><td class="shotsPerGame   ">11.2</td><td class="aaa"><span class="yellow-card-box">45</span><span class="red-card-box">6</span></td><td class="possession   ">44.3</td><td class="passSuccess   ">80.0</td><td class="aerialWonPerGame   ">14.7</td><td class="  sorted "><span class="stat-value rating">6.53</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/314/Show/France-Metz">16. Metz</a></td> <td class="goal   ">23</td><td class="shotsPerGame   ">9.9</td><td class="aaa"><span class="yellow-card-box">41</span><span class="red-card-box">2</span></td><td class="possession   ">35.9</td><td class="passSuccess   ">76.8</td><td class="aerialWonPerGame   ">11.7</td><td class="  sorted "><span class="stat-value rating">6.52</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/302/Show/France-Nantes">17. Nantes</a></td> <td class="goal   ">24</td><td class="shotsPerGame   ">11.4</td><td class="aaa"><span class="yellow-card-box">49</span><span class="red-card-box">3</span></td><td class="possession   ">45.5</td><td class="passSuccess   ">82.1</td><td class="aerialWonPerGame   ">11.6</td><td class="  sorted "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/941/Show/France-Clermont-Foot">18. Clermont Foot</a></td> <td class="goal   ">19</td><td class="shotsPerGame   ">11.7</td><td class="aaa"><span class="yellow-card-box">47</span><span class="red-card-box">6</span></td><td class="possession   ">49.3</td><td class="passSuccess   ">81.9</td><td class="aerialWonPerGame   ">12.1</td><td class="  sorted "><span class="stat-value rating">6.48</span></td></tr></tbody></table></div></div>
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

# Remplacer "Paris Saint-Germain" par "Paris SG" dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Paris Saint-Germain", "Paris SG")

# Remplacer "Clermont Foot" par "Clermont F." dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Clermont Foot", "Clermont F.")

# Convertir les types des colonnes "Journée" et "Buts"
df['Journée'] = df['Journée'].astype(int)
df['Buts'] = df['Buts'].astype(int)

# Enregistrer le DataFrame dans un fichier CSV
df.to_csv("team_stats_summary.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_summary.csv.")

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

# Convertir le DataFrame en dictionnaire pour l'insertion dans MongoDB
data_dict = df.to_dict(orient='records')

# Insérer les données dans la collection MongoDB
collection.insert_many(data_dict)

print("Les données ont été insérées dans la collection MongoDB avec succès.")