import dash
from dash import dcc, html
import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import pymongo
import dash_bootstrap_components as dbc



def load_dataserieA(collection_name):
    # Connexion à la base de données MongoDB
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
    db = client["SoccerStats"]
    collection = db[collection_name]

    # Récupération des données depuis MongoDB
    data = list(collection.find({}, {'_id': 0}))  # Exclure l'ID MongoDB
    df = pd.DataFrame(data)

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

def calculate_summary_statistics_serieA(df):
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

def dash_serieA():
    # Initialisation de l'application Dash avec le thème bootstrap
    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

    # Style pour le fond noir et les couleurs du texte
    colors = {
        'background': '#212121',  # Gris foncé
        'text': '#FFFFFF'  # Blanc
    }

    # Mise en page de l'application
    app.layout = html.Div(style={'backgroundColor': colors['background'], 'fontFamily': 'Arial'}, children=[
        # Barre de navigation
        dbc.NavbarSimple(
            children=[
                dbc.NavItem(dbc.NavLink("Accueil", href="/")),
                dbc.NavItem(dbc.NavLink("L1", href="/L1")),
                dbc.NavItem(dbc.NavLink("Liga", href="/Liga", id="nav-link-Liga")),  # Ajoutez l'ID ici
                dbc.NavItem(dbc.NavLink("SerieA", href="/SerieA", id="nav-link-SerieA")),
                dbc.NavItem(dbc.NavLink("Bundes", href="/Bundes")),
                dbc.NavItem(dbc.NavLink("PL", href="/PL")),
                
            ],
            brand="Navigation",
            brand_href="/",
            color="dark",
            dark=True,
        ),
        html.H1(
            children='Dashboard des différentes saisons de la Série A',
            style={
                'textAlign': 'center',
                'color': colors['text'],
                'padding': '20px'
            }
        ),

        html.Div([
        dcc.Dropdown(
            id='season-dropdown',
            options=[
                {'label': 'Saison actuelle', 'value': 'current_season'},
                {'label': '2022-2023', 'value': '22_23'},
                {'label': '2021-2022', 'value': '21_22'},
                {'label': '2020-2021', 'value': '20_21'}
            ],
            value='current_season',
            style={'width': '50%'}
        )
    ], style={'textAlign': 'center', 'margin-bottom': '20px'}),

        # Placeholder for graphs
        html.Div(id='graph-container')
    ])

    @app.callback(
        dash.dependencies.Output('graph-container', 'children'),
        [dash.dependencies.Input('season-dropdown', 'value')]
    )
    def update_graphs(selected_season):
        if selected_season == '21_22':
            df = load_dataserieA("matchs_serieA_21-22")
        elif selected_season == '22_23':
            df = load_dataserieA("matchs_serieA_22-23")
        elif selected_season == '20_21':
            df = load_dataserieA("matchs_serieA_20-21")
        else:
            df = load_dataserieA("matchs_serieA")  # For current season

        goals_summary, goals_conceded_summary, goals_per_matchday = calculate_summary_statistics_serieA(df)

        # Visualisation 1 : Histogramme des scores
        histogram_figure = px.histogram(df, x='Score', title='Distribution des scores', labels={'Score': 'Buts'})
        histogram_figure.update_traces(marker_color='#FFA500')
        histogram_figure.update_layout({
            'plot_bgcolor': '#000000',  # Couleur de fond
            'paper_bgcolor': '#000000',  # Couleur du papier
            'font': {'color': '#FFFFFF'}  # Couleur du texte
        })

        # Visualisation 2 : Graphique de tendance des scores par journée
        trend_figure = px.line(df, x='Journée', y='Score', title='Tendance des scores par journée')
        trend_figure.update_traces(line_color='#FF5733')  # Définir la couleur du trait en orange
        trend_figure.update_layout({
            'plot_bgcolor': '#000000',  # Couleur de fond
            'paper_bgcolor': '#000000',  # Couleur du papier
            'font': {'color': '#FFFFFF'}  # Couleur du texte
        })


        # Visualisation 3 : Carte thermique des buts marqués à domicile et à l'extérieur
        heatmap_goals_figure = go.Figure(data=go.Heatmap(
            z=[goals_summary['Buts_Extérieur_Visiteur'], goals_summary['Buts_Domicile_Equipe']],
            x=goals_summary['Equipe_Domicile'],
            y=['Buts_Extérieur', 'Buts_Domicile'],
            colorscale='Viridis',  # Choisir une échelle de couleurs personnalisée (ex: Viridis, Inferno, etc.)
            hoverongaps=False
        ), layout=go.Layout(
            plot_bgcolor=colors['background'],  # Couleur de fond
            paper_bgcolor=colors['background'],  # Couleur du papier
            font={'color': colors['text']}  # Couleur du texte
        ))

        # Visualisation 4 : Carte thermique des buts encaissés à domicile et à l'extérieur par équipe
        heatmap_goals_conceded_figure = go.Figure(data=go.Heatmap(
            z=[goals_conceded_summary['Buts_Encaissés_Extérieur_Domicile'], goals_conceded_summary['Buts_Encaissés_Domicile_Visiteur']],
            x=goals_conceded_summary['Equipe_Domicile'],
            y=['Buts_Encaissés_Extérieur_Domicile', 'Buts_Encaissés_Domicile_Visiteur'],
            colorscale='Viridis',  # Choisir une échelle de couleurs personnalisée (ex: Viridis, Inferno, etc.)
            hoverongaps=False
        ), layout=go.Layout(
            plot_bgcolor=colors['background'],  # Couleur de fond
            paper_bgcolor=colors['background'],  # Couleur du papier
            font={'color': colors['text']}  # Couleur du texte
        ))

        # Visualisation 5 : Diagramme de barres du nombre total de buts par journée jouée
        goals_per_matchday_bar_chart = go.Figure(
            data=[
                go.Bar(
                    x=goals_per_matchday['Journée'],  # Journée
                    y=goals_per_matchday['TotalButs'],  # Nombre total de buts
                    marker={'color': '#FF5733'},  # Couleur orange pour les barres
                )
            ],
            layout={
                'title': 'Nombre total de buts par journée jouée',
                'xaxis': {'title': 'Journée'},
                'yaxis': {'title': 'Nombre total de buts'},
                'plot_bgcolor': colors['background'],  # Couleur de fond
                'paper_bgcolor': colors['background'],  # Couleur du papier
                'font': {'color': colors['text']}  # Couleur du texte
            }
        )

        return [
            html.Div([
                html.H2('Histogramme du nombre des scores', style={'color': colors['text'], 'padding': '10px'}),
                dcc.Graph(
                    id='goals-histogram',
                    figure=histogram_figure,
                    style={'height': '400px'},
                    config={'displayModeBar': False}
                )
            ], style={'margin': '20px', 'borderRadius': '10px', 'backgroundColor': '#333333', 'padding': '20px'}),
            html.Div([
                html.H2('Graphique de tendance des scores par journée', style={'color': colors['text'], 'padding': '10px'}),
                dcc.Graph(
                    id='goals-trend',
                    figure=trend_figure,
                    style={'height': '400px'},
                    config={'displayModeBar': False}
                )
            ], style={'margin': '20px', 'borderRadius': '10px', 'backgroundColor': '#333333', 'padding': '20px'}),
            html.Div([
                html.H2('Carte thermique des buts marqués à domicile et à l\'extérieur par équipe', style={'color': colors['text'], 'padding': '10px'}),
                dcc.Graph(
                    id='heatmap-goals',
                    figure=heatmap_goals_figure,
                    style={'height': '400px'},
                    config={'displayModeBar': False}
                )
            ], style={'margin': '20px', 'borderRadius': '10px', 'backgroundColor': '#333333', 'padding': '20px'}),
            html.Div([
                html.H2('Carte thermique des buts encaissés à domicile et à l\'extérieur par équipe', style={'color': colors['text'], 'padding': '10px'}),
                dcc.Graph(
                    id='heatmap-goals-conceded',
                    figure=heatmap_goals_conceded_figure,
                    style={'height': '400px'},
                    config={'displayModeBar': False}
                )
            ], style={'margin': '20px', 'borderRadius': '10px', 'backgroundColor': '#333333', 'padding': '20px'}),
            html.Div([
                html.H2('Diagramme de barres du nombre total de buts par journée jouée', style={'color': colors['text'], 'padding': '10px'}),
                dcc.Graph(
                    id='goals-per-matchday-bar-chart',
                    figure=goals_per_matchday_bar_chart,
                    style={'height': '400px'},
                    config={'displayModeBar': False}
                )
            ], style={'margin': '20px', 'borderRadius': '10px', 'backgroundColor': '#333333', 'padding': '20px'})
        ]

    return app

# Exécution de l'application
if __name__ == '__main__':
    dash_serieA().run_server(debug=True)
