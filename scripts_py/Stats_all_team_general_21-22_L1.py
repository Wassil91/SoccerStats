import csv
import pandas as pd
from bs4 import BeautifulSoup
from pymongo import MongoClient

html_content = """
<div id="statistics-team-table-summary" class="" data-fwsc="1"><div class="ml12-lg-2 ml12-m-3 ml12-s-4 ml12-xs-5 semi-attached-table"><table class="grid with-centered-columns hover" id="top-team-stats-summary-grid"><thead><tr><th class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs" data-default-sort-dir="asc" data-property="TeamName">Équipe</th> <th class="global sortable goal   " data-stat-name="goal">Buts</th><th class="global sortable shotsPerGame   " data-stat-name="shotsPerGame">Tirs pm</th><th class="global sortable yellowCard   " data-stat-name="yellowCard">Discipline</th><th class="global sortable possession   " data-stat-name="possession">Possession%</th><th class="global sortable passSuccess   " data-stat-name="passSuccess">PassesRéussies%</th><th class="global sortable aerialWonPerGame   " data-stat-name="aerialWonPerGame">AériensGagnés</th><th class="global sortable rating " data-property="Rating" data-stat-name="Rating">Note</th></tr></thead><tbody id="top-team-stats-summary-content" style=""><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/304/Show/France-Paris-Saint-Germain">1. Paris Saint-Germain</a></td> <td class="goal   ">90</td><td class="shotsPerGame   ">14.8</td><td class="aaa"><span class="yellow-card-box">78</span><span class="red-card-box">4</span></td><td class="possession   ">63.3</td><td class="passSuccess   ">90.6</td><td class="aerialWonPerGame   ">7</td><td class="  sorted "><span class="stat-value rating">6.90</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/228/Show/France-Lyon">2. Lyon</a></td> <td class="goal   ">66</td><td class="shotsPerGame   ">14.6</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">6</span></td><td class="possession   ">58.7</td><td class="passSuccess   ">86.0</td><td class="aerialWonPerGame   ">12.5</td><td class="  sorted "><span class="stat-value rating">6.78</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/313/Show/France-Rennes">3. Rennes</a></td> <td class="goal   ">82</td><td class="shotsPerGame   ">14.5</td><td class="aaa"><span class="yellow-card-box">54</span><span class="red-card-box">2</span></td><td class="possession   ">56.2</td><td class="passSuccess   ">84.4</td><td class="aerialWonPerGame   ">14.7</td><td class="  sorted "><span class="stat-value rating">6.78</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/148/Show/France-Strasbourg">4. Strasbourg</a></td> <td class="goal   ">60</td><td class="shotsPerGame   ">12.6</td><td class="aaa"><span class="yellow-card-box">70</span><span class="red-card-box">3</span></td><td class="possession   ">49.0</td><td class="passSuccess   ">80.5</td><td class="aerialWonPerGame   ">17.4</td><td class="  sorted "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/248/Show/France-Monaco">5. Monaco</a></td> <td class="goal   ">65</td><td class="shotsPerGame   ">11.9</td><td class="aaa"><span class="yellow-card-box">78</span><span class="red-card-box">4</span></td><td class="possession   ">53.9</td><td class="passSuccess   ">81.2</td><td class="aerialWonPerGame   ">14.4</td><td class="  sorted "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/249/Show/France-Marseille">6. Marseille</a></td> <td class="goal   ">63</td><td class="shotsPerGame   ">13.1</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">3</span></td><td class="possession   ">62.0</td><td class="passSuccess   ">88.2</td><td class="aerialWonPerGame   ">9.9</td><td class="  sorted "><span class="stat-value rating">6.71</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/613/Show/France-Nice">7. Nice</a></td> <td class="goal   ">52</td><td class="shotsPerGame   ">11.9</td><td class="aaa"><span class="yellow-card-box">77</span><span class="red-card-box">6</span></td><td class="possession   ">51.3</td><td class="passSuccess   ">83.6</td><td class="aerialWonPerGame   ">14.2</td><td class="  sorted "><span class="stat-value rating">6.71</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/309/Show/France-Lens">8. Lens</a></td> <td class="goal   ">62</td><td class="shotsPerGame   ">13.6</td><td class="aaa"><span class="yellow-card-box">71</span><span class="red-card-box">7</span></td><td class="possession   ">50.9</td><td class="passSuccess   ">84.2</td><td class="aerialWonPerGame   ">11.5</td><td class="  sorted "><span class="stat-value rating">6.67</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/302/Show/France-Nantes">9. Nantes</a></td> <td class="goal   ">55</td><td class="shotsPerGame   ">11.2</td><td class="aaa"><span class="yellow-card-box">76</span><span class="red-card-box">4</span></td><td class="possession   ">43.2</td><td class="passSuccess   ">78.6</td><td class="aerialWonPerGame   ">15.9</td><td class="  sorted "><span class="stat-value rating">6.65</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/2332/Show/France-Brest">10. Brest</a></td> <td class="goal   ">49</td><td class="shotsPerGame   ">10.6</td><td class="aaa"><span class="yellow-card-box">59</span><span class="red-card-box">2</span></td><td class="possession   ">43.1</td><td class="passSuccess   ">78.6</td><td class="aerialWonPerGame   ">16.5</td><td class="  sorted "><span class="stat-value rating">6.65</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/607/Show/France-Lille">11. Lille</a></td> <td class="goal   ">48</td><td class="shotsPerGame   ">12.3</td><td class="aaa"><span class="yellow-card-box">93</span><span class="red-card-box">6</span></td><td class="possession   ">50.7</td><td class="passSuccess   ">81.5</td><td class="aerialWonPerGame   ">14.1</td><td class="  sorted "><span class="stat-value rating">6.62</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/950/Show/France-Reims">12. Reims</a></td> <td class="goal   ">43</td><td class="shotsPerGame   ">10.4</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">7</span></td><td class="possession   ">41.7</td><td class="passSuccess   ">78.4</td><td class="aerialWonPerGame   ">11.9</td><td class="  sorted "><span class="stat-value rating">6.60</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/311/Show/France-Montpellier">13. Montpellier</a></td> <td class="goal   ">49</td><td class="shotsPerGame   ">11.5</td><td class="aaa"><span class="yellow-card-box">69</span><span class="red-card-box">9</span></td><td class="possession   ">47.9</td><td class="passSuccess   ">80.4</td><td class="aerialWonPerGame   ">11.8</td><td class="  sorted "><span class="stat-value rating">6.56</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/614/Show/France-Angers">14. Angers</a></td> <td class="goal   ">44</td><td class="shotsPerGame   ">10.3</td><td class="aaa"><span class="yellow-card-box">66</span><span class="red-card-box">4</span></td><td class="possession   ">48.4</td><td class="passSuccess   ">83.2</td><td class="aerialWonPerGame   ">11.1</td><td class="  sorted "><span class="stat-value rating">6.54</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/314/Show/France-Metz">15. Metz</a></td> <td class="goal   ">35</td><td class="shotsPerGame   ">9.8</td><td class="aaa"><span class="yellow-card-box">90</span><span class="red-card-box">9</span></td><td class="possession   ">42.7</td><td class="passSuccess   ">78.7</td><td class="aerialWonPerGame   ">14.2</td><td class="  sorted "><span class="stat-value rating">6.52</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/146/Show/France-Lorient">16. Lorient</a></td> <td class="goal   ">35</td><td class="shotsPerGame   ">11.7</td><td class="aaa"><span class="yellow-card-box">63</span><span class="red-card-box">4</span></td><td class="possession   ">44.1</td><td class="passSuccess   ">79.8</td><td class="aerialWonPerGame   ">11.6</td><td class="  sorted "><span class="stat-value rating">6.51</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/229/Show/France-Troyes">17. Troyes</a></td> <td class="goal   ">37</td><td class="shotsPerGame   ">10.2</td><td class="aaa"><span class="yellow-card-box">69</span><span class="red-card-box">6</span></td><td class="possession   ">44.0</td><td class="passSuccess   ">79.9</td><td class="aerialWonPerGame   ">11.2</td><td class="  sorted "><span class="stat-value rating">6.51</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/145/Show/France-Saint-Etienne">18. Saint-Etienne</a></td> <td class="goal   ">42</td><td class="shotsPerGame   ">11.6</td><td class="aaa"><span class="yellow-card-box">79</span><span class="red-card-box">5</span></td><td class="possession   ">48.4</td><td class="passSuccess   ">79.6</td><td class="aerialWonPerGame   ">12.5</td><td class="  sorted "><span class="stat-value rating">6.47</span></td></tr><tr><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/941/Show/France-Clermont-Foot">19. Clermont Foot</a></td> <td class="goal   ">38</td><td class="shotsPerGame   ">11.5</td><td class="aaa"><span class="yellow-card-box">61</span><span class="red-card-box">7</span></td><td class="possession   ">49.8</td><td class="passSuccess   ">82.2</td><td class="aerialWonPerGame   ">11.7</td><td class="  sorted "><span class="stat-value rating">6.46</span></td></tr><tr class="alt"><td class="col12-lg-2 col12-m-3 col12-s-4 col12-xs-5 grid-abs overflow-text"><a class="team-link" href="/Teams/315/Show/France-Bordeaux">20. Bordeaux</a></td> <td class="goal   ">52</td><td class="shotsPerGame   ">11.8</td><td class="aaa"><span class="yellow-card-box">88</span><span class="red-card-box">5</span></td><td class="possession   ">47.1</td><td class="passSuccess   ">79.8</td><td class="aerialWonPerGame   ">13.2</td><td class="  sorted "><span class="stat-value rating">6.44</span></td></tr></tbody></table></div></div>
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

# Remplacer "Paris Saint-Germain" par "Paris SG" dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Paris Saint-Germain", "Paris SG")

# Remplacer "Clermont Foot" par "Clermont F." dans la colonne "Équipe"
df["Équipe"] = df["Équipe"].replace("Clermont Foot", "Clermont F.")

# Convertir les types des colonnes "Journée" et "Buts"
df['Journée'] = df['Journée'].astype(int)
df['Buts'] = df['Buts'].astype(int)

# Enregistrer le DataFrame dans un fichier CSV (avec les modifications)
df.to_csv("team_stats_summary_modified.csv", index=False)

print("Les données ont été extraites et enregistrées dans team_stats_summary_modified.csv.")

# Connexion à la base de données MongoDB
client = MongoClient('mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1')
db = client['SoccerStats']
collection_name = 'Stats_All_Team_General_21-22_L1'

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