import dash
from dash import dcc, html
import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import pymongo
import dash_bootstrap_components as dbc

# Connexion à la base de données MongoDB
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client["SoccerStats"]
collection = db["matchs_PL"]

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

# Initialisation de l'application Dash avec le thème bootstrap
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Style pour le fond noir et les couleurs du texte
colors = {
    'background': '#212121',  # Gris foncé
    'text': '#FFFFFF'  # Blanc
}

# Calculer le nombre total de buts par journée jouée
def calculate_total_goals(row):
    goals = row['Score'].split('-')
    if len(goals) == 2 and all(map(lambda x: x.isdigit(), goals)):
        return int(goals[0]) + int(goals[1])
    else:
        return 0

df['TotalButs'] = df.apply(calculate_total_goals, axis=1)
goals_per_matchday = df.groupby('Journée')['TotalButs'].sum().reset_index()

# Mise en page de l'application
app.layout = html.Div(style={'backgroundColor': colors['background'], 'fontFamily': 'Arial'}, children=[
    # Barre de navigation
    dbc.NavbarSimple(
        children=[
            dbc.NavItem(dbc.NavLink("Accueil", href="/")),
            dbc.NavItem(dbc.NavLink("Autre Page", href="/autre-page")),
        ],
        brand="Navigation",
        brand_href="/",
        color="dark",
        dark=True,
    ),
    html.H1(
        children='Tableau de bord Premiere Ligue',
        style={
            'textAlign': 'center',
            'color': colors['text'],
            'padding': '20px'
        }
    ),

    # Visualisation 1 : Histogramme des scores
    html.Div([
        html.H2('Histogramme du nombre des scores', style={'color': colors['text'], 'padding': '10px'}),
        dcc.Graph(
            id='goals-histogram',
            figure=go.Figure(
                data=px.histogram(df, x='Score', title='Distribution des scores', labels={'Score': 'Buts'}).update_traces(marker_color='#FFA500').data,
                layout=go.Layout(
                    plot_bgcolor=colors['background'],  # Couleur de fond
                    paper_bgcolor=colors['background'],  # Couleur du papier
                    font={'color': colors['text']}  # Couleur du texte
                )
            ),
            style={'height': '400px'},
            config={'displayModeBar': False}
        )
    ], style={'margin': '20px', 'borderRadius': '10px', 'backgroundColor': '#333333', 'padding': '20px'}),

    # Visualisation 2 : Graphique de tendance des scores par journée
    html.Div([
        html.H2('Graphique de tendance des scores par journée', style={'color': colors['text'], 'padding': '10px'}),
        dcc.Graph(
            id='goals-trend',
            figure=go.Figure(
                data=[{
                    'x': df['Journée'],
                    'y': df['Score'],
                    'type': 'scatter',
                    'mode': 'lines',
                    'line': {'color': 'red', 'width': 3},  # Couleur et épaisseur du trait
                    'name': 'Score'
                }],
                layout=go.Layout(
                    plot_bgcolor=colors['background'],  # Couleur de fond
                    paper_bgcolor=colors['background'],  # Couleur du papier
                    font={'color': colors['text']},  # Couleur du texte
                    xaxis=dict(tickwidth=2, tickcolor='red'),  # Épaisseur et couleur des traits de l'axe des abscisses
                    yaxis=dict(tickwidth=2, tickcolor='red'),  # Épaisseur et couleur des traits de l'axe des ordonnées
                    hovermode='closest',  # Afficher le point de données le plus proche lors du survol
                    margin=dict(l=50, r=20, t=10, b=40),  # Marge autour du graphique
                )
            ),
            style={'height': '400px'},
            config={'displayModeBar': False}
        )
    ], style={'margin': '20px', 'borderRadius': '10px', 'backgroundColor': '#333333', 'padding': '20px'}),

    # Visualisation 3 : Carte thermique des scores à domicile et à l'extérieur
    html.Div([
        html.H2('Carte thermique des buts marqués à domicile et à l\'extérieur par équipe', style={'color': colors['text'], 'padding': '10px'}),
        dcc.Graph(
            id='heatmap-goals',
            figure=go.Figure(data=go.Heatmap(
                z=[goals_summary['Buts_Extérieur_Visiteur'], goals_summary['Buts_Domicile_Equipe']],
                x=goals_summary['Equipe_Domicile'],
                y=['Buts_Extérieur', 'Buts_Domicile'],
                colorscale='Viridis',  # Choisir une échelle de couleurs personnalisée (ex: Viridis, Inferno, etc.)
                hoverongaps=False
            ), layout=go.Layout(
                plot_bgcolor=colors['background'],  # Couleur de fond
                paper_bgcolor=colors['background'],  # Couleur du papier
                font={'color': colors['text']}  # Couleur du texte
            )),
            style={'height': '400px'},
            config={'displayModeBar': False}
        )
    ], style={'margin': '20px', 'borderRadius': '10px', 'backgroundColor': '#333333', 'padding': '20px'}),

    # Visualisation 4 : Carte thermique des buts encaissés à domicile et à l'extérieur par équipe
    html.Div([
        html.H2('Carte thermique des buts encaissés à domicile et à l\'extérieur par équipe', style={'color': colors['text'], 'padding': '10px'}),
        dcc.Graph(
            id='heatmap-goals-conceded',
            figure=go.Figure(data=go.Heatmap(
                z=[goals_conceded_summary['Buts_Encaissés_Extérieur_Domicile'], goals_conceded_summary['Buts_Encaissés_Domicile_Visiteur']],
                x=goals_conceded_summary['Equipe_Domicile'],
                y=['Buts_Encaissés_Extérieur_Domicile', 'Buts_Encaissés_Domicile_Visiteur'],
                colorscale='Viridis',  # Choisir une échelle de couleurs personnalisée (ex: Viridis, Inferno, etc.)
                hoverongaps=False
            ), layout=go.Layout(
                plot_bgcolor=colors['background'],  # Couleur de fond
                paper_bgcolor=colors['background'],  # Couleur du papier
                font={'color': colors['text']}  # Couleur du texte
            )),
            style={'height': '400px'},
            config={'displayModeBar': False}
        )
    ], style={'margin': '20px', 'borderRadius': '10px', 'backgroundColor': '#333333', 'padding': '20px'}),

    # Visualisation 5 : Diagramme de barres du nombre total de buts par journée jouée
    html.Div([
        html.H2('Diagramme de barres du nombre total de buts par journée jouée', style={'color': colors['text'], 'padding': '10px'}),
        dcc.Graph(
            id='goals-per-matchday-bar-chart',
            figure={
                'data': [
                    go.Bar(
                        x=goals_per_matchday['Journée'],  # Journée
                        y=goals_per_matchday['TotalButs'],  # Nombre total de buts
                        marker={'color': '#FF5733'},  # Couleur orange pour les barres
                    )
                ],
                'layout': {
                    'title': 'Nombre total de buts par journée jouée',
                    'xaxis': {'title': 'Journée'},
                    'yaxis': {'title': 'Nombre total de buts'},
                    'plot_bgcolor': colors['background'],  # Couleur de fond
                    'paper_bgcolor': colors['background'],  # Couleur du papier
                    'font': {'color': colors['text']}  # Couleur du texte
                }
            },
            config={'displayModeBar': False}
        )
    ], style={'margin': '20px', 'borderRadius': '10px', 'backgroundColor': '#333333', 'padding': '20px'}),
])

# Exécution de l'application
if __name__ == '__main__':
    app.run_server(debug=True)
