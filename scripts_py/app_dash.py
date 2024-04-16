# Importation des modules nécessaires
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash
from dash import dcc, html, callback_context
import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import pymongo
import dash_bootstrap_components as dbc
import dash_bootstrap_components as dbc

def load_data(collection_name):
        # Connexion à la base de données MongoDB
        client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
        db = client["SoccerStats"]
        collection = db[collection_name]

        # Récupération des données depuis MongoDB
        data = list(collection.find({}, {'_id': 0}))  # Exclure l'ID MongoDB
        df = pd.DataFrame(data)

            # Ajouter une colonne 'Championnat' avec le nom du championnat
        df['Championnat'] = collection_name.split('_')[-1]

        # Remplacer les valeurs vides dans la colonne 'Score' par '0-0'
        df['Score'].fillna('0-0', inplace=True)

        # Filtrer les lignes avec des scores incorrects
        df = df.dropna(subset=['Score'])

        # Utiliser str.extract() pour séparer les scores en buts à domicile et à l'extérieur
        df[['Buts_Domicile', 'Buts_Extérieur']] = df['Score'].str.extract(r'(\d+)-(\d+)')
        df[['Buts_Domicile', 'Buts_Extérieur']] = df[['Buts_Domicile', 'Buts_Extérieur']].astype(float)  # Convertir en float

        # Remplacer les chaînes vides par NaN
        df['Buts_Domicile'].replace('', pd.NA, inplace=True)
        df['Buts_Extérieur'].replace('', pd.NA, inplace=True)

        # Convertir les colonnes en float64 pour inclure NaN
        df['Buts_Domicile'] = df['Buts_Domicile'].astype('float64')
        df['Buts_Extérieur'] = df['Buts_Extérieur'].astype('float64')

        # Remplacer NaN par 0
        df['Buts_Domicile'].fillna(0, inplace=True)
        df['Buts_Extérieur'].fillna(0, inplace=True)

        # Convertir les colonnes en entiers
        df['Buts_Domicile'] = df['Buts_Domicile'].astype(int)
        df['Buts_Extérieur'] = df['Buts_Extérieur'].astype(int)

        # Convertir la colonne 'Journée' en numérique pour trier correctement
        df['Journée'] = pd.to_numeric(df['Journée'], errors='coerce')

        # Supprimer les lignes avec des valeurs nulles dans la colonne 'Journée'
        df = df.dropna(subset=['Journée'])

        # Trier le DataFrame par la colonne 'Journée'
        df = df.sort_values(by='Journée')

        return df

def calculate_summary_statistics(df):
        # Créer une DataFrame résumant le nombre total de buts marqués à domicile et à l'extérieur pour chaque équipe
        goals_summary = df.groupby('Equipe_Domicile').agg({'Buts_Domicile': 'sum', 'Buts_Extérieur': 'sum'}).reset_index()

        # Renommer les colonnes pour correspondre à ce que vous avez demandé
        goals_summary = goals_summary.rename(columns={'Buts_Domicile': 'Buts_Domicile_Equipe', 'Buts_Extérieur': 'Buts_Extérieur_Equipe'})

        # Ajouter une colonne pour les buts à l'extérieur de l'équipe visiteuse
        goals_summary['Buts_Extérieur_Visiteur'] = df.groupby('Equipe_Extérieur')['Buts_Extérieur'].sum().reset_index()['Buts_Extérieur']

        # Créer une DataFrame résumant le nombre total de buts encaissés à domicile et à l'extérieur pour chaque équipe
        goals_conceded_summary = df.groupby('Equipe_Domicile').agg({'Buts_Extérieur': 'sum', 'Buts_Domicile': 'sum'}).reset_index()

        # Renommer les colonnes pour correspondre à ce que vous avez demandé
        goals_conceded_summary = goals_conceded_summary.rename(columns={'Buts_Domicile': 'Buts_Encaissés_Domicile', 'Buts_Extérieur': 'Buts_Encaissés_Extérieur'})

        # Ajouter une colonne pour les buts encaissés à l'extérieur par l'équipe domicile
        goals_conceded_summary['Buts_Encaissés_Extérieur_Domicile'] = df.groupby('Equipe_Domicile')['Buts_Extérieur'].sum().reset_index()['Buts_Extérieur']

        # Ajouter une colonne pour les buts encaissés à domicile par l'équipe visiteuse
        goals_conceded_summary['Buts_Encaissés_Domicile_Visiteur'] = df.groupby('Equipe_Extérieur')['Buts_Domicile'].sum().reset_index()['Buts_Domicile']

        # Calculer le nombre total de buts par journée jouée
        def calculate_total_goals(row):
            goals = row['Score'].split('-')
            if len(goals) == 2 and all(map(lambda x: x.isdigit(), goals)):
                return int(goals[0]) + int(goals[1])
            else:
                return 0

        df['TotalButs'] = df.apply(calculate_total_goals, axis=1)
        goals_per_matchday = df.groupby('Journée')['TotalButs'].sum().reset_index().sort_values(by='Journée')

        return goals_summary, goals_conceded_summary, goals_per_matchday


colors = {
            'background': '#212121',  # Gris foncé
            'text': '#FFFFFF'  # Blanc
}
# Mise en place de l'application Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Définition des couleurs
colors = {
    'background': '#000000',  # Noir
    'text': '#FFFFFF'  # Blanc
}

# Fonction pour le rendu de la page d'accueil
# Fonction pour le rendu de la page d'accueil avec les graphiques Histogramme des scores
def render_home_page():
    # Charger les données de chaque championnat
    df_l1 = load_data("matchs_L1")
    df_liga = load_data("matchs_Liga")
    df_serieA = load_data("matchs_serieA")
    df_bundes = load_data("matchs_bundesliga")
    df_PL = load_data("matchs_PL")

    # Définir les couleurs pour chaque championnat
    l1_color = '#FF5733'  # Couleur pour les matchs de Ligue 1
    liga_color = '#FFA500'  # Couleur pour les matchs de Liga
    SerieA_color = '#B0F2B6'  # Couleur pour les matchs de SerieA
    Bundes_color = '#00FF00'  # Couleur pour les matchs de Bundesliga
    PL_color = '#FF5733'  # Couleur pour les matchs de PL

    # Concaténer les DataFrames de chaque championnat pour les deux groupes
    df_concatenated_group1 = pd.concat([df_l1, df_liga])
    df_concatenated_group2 = pd.concat([df_serieA, df_bundes, df_PL])

    # Créer les histogrammes pour chaque groupe
    histogram_figure_group1 = px.histogram(df_concatenated_group1, x='Score', title='Histogramme du nombre des scores',
                                           labels={'Score': 'Buts'}, color='Championnat')
    histogram_figure_group2 = px.histogram(df_concatenated_group2, x='Score', title='Histogramme du nombre des scores',
                                           labels={'Score': 'Buts'}, color='Championnat')

    # Mise à jour des couleurs pour chaque championnat dans les deux graphiques
    histogram_figure_group1.update_traces(marker_color=[l1_color, liga_color], selector={'name': 'Ligue 1'})
    histogram_figure_group2.update_traces(marker_color=[SerieA_color, Bundes_color, PL_color], selector={'name': ['SerieA', 'Bundes', 'PL']})

    # Mise en page des deux graphiques
    histogram_div_group1 = dcc.Graph(id='goals-histogram-group1', figure=histogram_figure_group1,
                                     style={'height': '400px', 'margin': '20px', 'borderRadius': '10px',
                                            'backgroundColor': '#333333', 'padding': '20px'})
    histogram_div_group2 = dcc.Graph(id='goals-histogram-group2', figure=histogram_figure_group2,
                                     style={'height': '400px', 'margin': '20px', 'borderRadius': '10px',
                                            'backgroundColor': '#333333', 'padding': '20px'})

    # Mise en page des deux graphiques avec les couleurs spécifiées
    histogram_figure_group1.update_layout({
        'plot_bgcolor': '#000000',  # Couleur de fond
        'paper_bgcolor': '#000000',  # Couleur du papier
        'font': {'color': '#FFFFFF'}  # Couleur du texte
    })

    histogram_figure_group2.update_layout({
        'plot_bgcolor': '#000000',  # Couleur de fond
        'paper_bgcolor': '#000000',  # Couleur du papier
        'font': {'color': '#FFFFFF'}  # Couleur du texte
    })

    # Retourner la mise en page de la page d'accueil avec les deux graphiques
    return html.Div([
        html.H1('Bienvenue sur notre Dashboard des championnats du big five Européen !',
                style={'color': colors['text'], 'margin-bottom': '80px', 'text-align': 'center'}),
        html.P("Vous trouverez des Dashboards actualisés à la saison actuelle ainsi que les précédentes saisons !",
               style={'color': colors['text'], 'text-align': 'center'}),
        html.P("Vous pourrez faire des comparaisons intéressantes entre les championnats et entre les saisons !",
               style={'color': colors['text'], 'text-align': 'center'}),
        html.P("Choisissez dans la navbar le championnat de votre choix ;)", style={'color': colors['text'], 'margin-top': '20px', 'text-align': 'center'}),
        html.P("Voici quelques graphiques representant le statut de tous les championnats de la saison actuelle", style={'color': colors['text'], 'margin-top': '20px', 'text-align': 'center'}),
        html.Div([
            html.Div([
                html.H2('Histogrammes du nombre des scores pour Ligue 1 et Liga',
                        style={'color': colors['text'], 'padding': '10px', 'text-align': 'center'}),
                histogram_div_group1
            ], style={'width': '100%', 'display': 'inline-block'}),
            html.Div([
                html.H2('Histogrammes du nombre des scores pour SerieA, Bundesliga et PL',
                        style={'color': colors['text'], 'padding': '10px', 'text-align': 'center'}),
                histogram_div_group2
            ], style={'width': '100%', 'display': 'inline-block'})
        ])
    ])








# Mise en page de la navbar
navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Accueil", href="/dashboard")),
        dbc.NavItem(dbc.NavLink("L1", href="/dashboard/L1")),
        dbc.NavItem(dbc.NavLink("Liga", href="/Liga")),  
        dbc.NavItem(dbc.NavLink("SerieA", href="/SerieA")),
        dbc.NavItem(dbc.NavLink("Bundes", href="/Bundes")),
        dbc.NavItem(dbc.NavLink("PL", href="/PL")),
    ],
    brand="Navigation",
    brand_href="#",
    color="dark",
    dark=True,
)


# Mise en page de l'application
app.layout = html.Div(style={'backgroundColor': colors['background'], 'height': '100vh'}, children=[
    # Barre de navigation
    navbar,
    # Composant dcc.Location pour suivre l'URL
    dcc.Location(id='url', refresh=False),
    # Contenu de la page d'accueil
    html.Div(id='page-content')
    
    
])

# Callback pour rendre la page d'accueil
@app.callback(
    dash.dependencies.Output('page-content', 'children'),
    [dash.dependencies.Input('url', 'pathname')]
)
def display_page(pathname):
    if pathname == '/':
        return render_home_page()
    else:
        return html.H1('404 - Page not found', style={'color': colors['text']})



# Point d'entrée de l'application
if __name__ == '__main__':
    app.run_server(debug=True)
