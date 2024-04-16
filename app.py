from flask import Flask, render_template, redirect, request, jsonify
from bson import ObjectId
import pymongo
import random
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import math
import dash
from dash import dcc, html, callback_context
import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import pymongo
import dash_bootstrap_components as db
import dash_bootstrap_components as dbc
# from scripts_py.dash_L1_all_saisons_matchs import test_all_dash
import threading
import subprocess
import os


def load_data(collection_name):
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

def load_data_liga(collection_name):
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

def load_data_bundes(collection_name):
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

def load_data_PL(collection_name):
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



def dash_L1(app):
    # Style pour le fond noir et les couleurs du texte
    colors = {
        'background': '#212121',  # Gris foncé
        'text': '#FFFFFF'  # Blanc
    }
    
    # Mise en page de l'application
    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
    app.layout = html.Div(style={'backgroundColor': colors['background'], 'color': colors['text'], 'height': '100vh'}, children=[
        # Barre de navigation
        dcc.Location(id='url', refresh=False),
        dbc.NavbarSimple(
            children=[
                dbc.NavItem(dbc.NavLink("Accueil", href="/")),
                dbc.NavItem(dbc.NavLink("L1", href="/L1", id="nav-link-L1")),
                dbc.NavItem(dbc.NavLink("Liga", href="/Liga", id="nav-link-Liga")),  # Ajoutez l'ID ici
                dbc.NavItem(dbc.NavLink("SerieA", href="/SerieA", id="nav-link-SerieA")),
                dbc.NavItem(dbc.NavLink("Bundes", href="/Bundes", id="nav-link-Bundes")),
                dbc.NavItem(dbc.NavLink("PL", href="/PL", id="nav-link-PL")),
            ],
            brand="Navigation",
            brand_href="/",
            color="dark",
            dark=True,
        ),
        html.H1(
            id='header-title',
            children='Dashboard des différentes saisons de la ligue 1',
            style={
                'textAlign': 'center',
                'color': colors['text'],
                'padding': '20px'
            }
        ),
        html.Div(
            id='dropdown-container',
            children=[
                dcc.Dropdown(
                    id='season-dropdown',
                    options=[
                        {'label': 'Saison actuelle', 'value': 'current_season'},
                        {'label': '2022-2023', 'value': '22_23'},
                        {'label': '2021-2022', 'value': '21_22'},
                        {'label': '2020-2021', 'value': '20_21'}
                    ],
                    value='current_season',
                    style={'width': '40%', 'backgroundColor': 'transparent', 'color': 'black', 'margin-left': '10px'}
                )
            ],
            style={'textAlign': 'center', 'margin-bottom': '20px'}
            ),

        # Placeholder for graphs
        html.Div(id='graph-container', className='container-fluid', style={'backgroundColor': colors['background'], 'padding': '20px', 'min-height': '100vh'})
    ])
        # Callback pour définir le style de fond noir sur toute la page d'accueil
    @app.callback(
        dash.dependencies.Output('app-layout', 'style'),
        [dash.dependencies.Input('url', 'pathname')]
    )
    def update_layout_style(pathname):
        if pathname == '/':
            return {'backgroundColor': colors['background'], 'padding': '50px'}  # Fond noir sur toute la page d'accueil
        else:
            return {'backgroundColor': colors['background'], 'padding': '20px', 'min-height': '100vh'}  # Appliquer le fond noir sur les autres pages
        
    # Callback pour afficher ou masquer la liste déroulante en fonction de l'URL
    @app.callback(
        dash.dependencies.Output('dropdown-container', 'style'),
        [dash.dependencies.Input('url', 'pathname')]
    )
    def display_dropdown(pathname):
        if pathname == '/':
            return {'display': 'none'}  # Masquer la liste déroulante sur la page d'accueil
        else:
            return {'textAlign': 'center', 'margin-bottom': '20px'}  # Afficher la liste déroulante sur les autres pages
    
    
# Définition des couleurs
    colors = {
        'background': '#000000',  # Noir
        'text': '#FFFFFF'  # Blanc
    }

    # Fonction pour le rendu de la page d'accueil
    def render_home_page():
        return html.Div([
            html.H1('Bienvenue sur notre Dashboard des championnats du big five Européen !', style={'color': colors['text'], 'margin-bottom': '80px'}),
            html.P("Vous trouverez des Dashboards actualisés à la saison actuelle ainsi que les précédentes saisons !", style={'color': colors['text']}),
            html.P("Vous pourrez faire des comparaisons intéressantes entre les championnats et entre les saisons !", style={'color': colors['text']}),
            html.P("Choisissez dans la navbar le championnat de votre choix ;)", style={'color': colors['text'], 'margin-top': '20px'})
        ], style={'textAlign': 'center', 'padding': '50px'})

    # Mise en page de la navbar
    navbar = dbc.NavbarSimple(
        children=[
            dbc.NavItem(dbc.NavLink("Accueil", href="/")),
            dbc.NavItem(dbc.NavLink("Ligue 1", href="/L1")),
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
    # def render_home_page():
    #     return html.Div(style={'backgroundColor': colors['background'], 'color': colors['text'], 'height': '100vh'}, children=[
    #         dbc.NavbarSimple(
    #             children=[
    #                 dbc.NavItem(dbc.NavLink("Accueil", href="/")),
    #                 dbc.NavItem(dbc.NavLink("L1", href="/L1", id="nav-link-L1")),
    #                 dbc.NavItem(dbc.NavLink("Liga", href="/Liga", id="nav-link-Liga")),
    #                 dbc.NavItem(dbc.NavLink("SerieA", href="/SerieA", id="nav-link-SerieA")),
    #                 dbc.NavItem(dbc.NavLink("Bundes", href="/Bundes", id="nav-link-Bundes")),
    #                 dbc.NavItem(dbc.NavLink("PL", href="/PL", id="nav-link-PL")),
    #             ],
    #             brand="Navigation",
    #             brand_href="/",
    #             color="dark",
    #             dark=True,
    #         ),
    #         html.Div([
    #             html.H1('Bienvenue sur notre Dashboard des championnats du big five Européen !', style={'margin-bottom': '80px'}),
    #             html.P("Vous trouverez des Dashboards actualisés à la saison actuelle ainsi que les précédentes saisons !"),
    #             html.P("Vous pourrez faire des comparaisons intéressantes entre les championnats et entre les saisons !", style={'margin-bottom': '20px'}),
    #             html.P("Choisissez dans la navbar le championnat de votre choix ;)")
    #         ], style={'textAlign': 'center', 'padding': '50px'})
    #     ])


    @app.callback(
    [dash.dependencies.Output('graph-container', 'children'),
     dash.dependencies.Output('header-title', 'children')],
    [dash.dependencies.Input('url', 'pathname'),
     dash.dependencies.Input('season-dropdown', 'value'),
     dash.dependencies.Input('nav-link-Liga', 'n_clicks'),
     dash.dependencies.Input('nav-link-L1', 'n_clicks'),
     dash.dependencies.Input('nav-link-SerieA', 'n_clicks'),
     dash.dependencies.Input('nav-link-Bundes', 'n_clicks'),
     dash.dependencies.Input('nav-link-PL', 'n_clicks')]
)
    
    # def render_home_page(pathname):
    #     if pathname == '/':
    #         return render_home_page()
    #     else:
    #         return html.H1('404 - Page not found', style={'color': 'white'})
    
    def update_page_content(pathname, selected_season, n_clicks_Liga, n_clicks_L1, n_clicks_SerieA, n_clicks_bundes, n_clicks_PL):
        if pathname == '/dashboard':
            return render_home_page(), ''

        # Déterminer quel onglet a été cliqué en dernier
        nav_clicks = {
            'Liga': n_clicks_Liga,
            'L1': n_clicks_L1,
            'SerieA': n_clicks_SerieA,
            'Bundes': n_clicks_bundes,
            'PL': n_clicks_PL
        }
        # Obtenir le nom de l'onglet cliqué le plus récemment
        nav_clicks = {key: value if value is not None else 0 for key, value in nav_clicks.items()}


        last_clicked = max(nav_clicks, key=nav_clicks.get)
        
        # Mettre à jour le titre en fonction de l'onglet sélectionné
        if last_clicked == 'Liga':
            return update_graphs('Liga', selected_season), 'Dashboard des différentes saisons de la Liga'
        elif last_clicked == 'L1':
            return update_graphs('L1', selected_season), 'Dashboard des différentes saisons de la Ligue 1'
        elif last_clicked == 'SerieA':
            return update_graphs('SerieA', selected_season), 'Dashboard des différentes saisons de la Série A'
        elif last_clicked == 'Bundes':
            return update_graphs('Bundes', selected_season), 'Dashboard des différentes saisons de la Bundesliga'
        elif last_clicked == 'PL':
            return update_graphs('PL', selected_season), 'Dashboard des différentes saisons de la PL'
        else:
            return html.H1('404 - Page not found', style={'color': colors['text']}), ''

    def update_graphs(championship, selected_season):
        # Charger les données en fonction de l'onglet cliqué et de la saison sélectionnée
        if championship == 'L1':  # Ligue 1
            if selected_season == '21_22':
                df = load_data("matchs_L1_21-22")
            elif selected_season == '22_23':
                df = load_data("matchs_L1_22-23")
            elif selected_season == '20_21':
                df = load_data("matchs_L1_20-21")
            else:
                df = load_data("matchs_L1")  # Pour la saison actuelle
        elif championship == 'Liga':
            if selected_season == '21_22':
                df = load_data_liga("matchs_Liga_21-22")
            elif selected_season == '22_23':
                df = load_data_liga("matchs_Liga_22-23")
            elif selected_season == '20_21':
                df = load_data_liga("matchs_Liga_20-21")
            else:
                df = load_data_liga("matchs_Liga")  # Pour la saison actuelle
        elif championship == 'SerieA':
            if selected_season == '21_22':
                df = load_dataserieA("matchs_serieA_21-22")
            elif selected_season == '22_23':
                df = load_dataserieA("matchs_serieA_22-23")
            elif selected_season == '20_21':
                df = load_dataserieA("matchs_serieA_20-21")
            else:
                df = load_dataserieA("matchs_serieA")  # Pour la saison actuelle
        elif championship == 'Bundes':
            if selected_season == '21_22':
                df = load_data_bundes("matchs_bundesliga_20-21")
            elif selected_season == '22_23':
                df = load_data_bundes("matchs_bundesliga_22-23")
            elif selected_season == '20_21':
                df = load_data_bundes("matchs_bundesliga_20-21")
            else:
                df = load_data_bundes("matchs_bundesliga")  # Pour la saison actuelle
        elif championship == 'PL':
            if selected_season == '21_22':
                df = load_data_PL("matchs_PL_20-21")
            elif selected_season == '22_23':
                df = load_data_PL("matchs_PL_22-23")
            elif selected_season == '20_21':
                df = load_data_PL("matchs_PL_20-21")
            else:
                df = load_data_PL("matchs_PL")  # Pour la saison actuelle
        # Ajouter des conditions pour les autres championnats ici

        goals_summary, goals_conceded_summary, goals_per_matchday = calculate_summary_statistics(df)

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
                html.H2('Carte thermique des buts marqués à domicile et à l\'extérieur', style={'color': colors['text'], 'padding': '10px'}),
                dcc.Graph(
                    id='goals-heatmap',
                    figure=heatmap_goals_figure,
                    style={'height': '400px'},
                    config={'displayModeBar': False}
                )
            ], style={'margin': '20px', 'borderRadius': '10px', 'backgroundColor': '#333333', 'padding': '20px'}),
            html.Div([
                html.H2('Carte thermique des buts encaissés à domicile et à l\'extérieur par équipe', style={'color': colors['text'], 'padding': '10px'}),
                dcc.Graph(
                    id='goals-conceded-heatmap',
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

app = Flask(__name__, static_folder='static')

# def create_dash_app():
#     import dash
#     from dash import dcc, html, callback_context
#     import plotly.express as px
#     import plotly.graph_objs as go
#     import pandas as pd
#     import pymongo
#     import dash_bootstrap_components as dbc

#     # Importez les fonctions nécessaires pour charger les données et calculer les statistiques
#     from scripts_py.dash_Liga_all_saisons_matchs import load_data_liga, calculate_summary_statistics_liga
#     from scripts_py.dash_serieA_all_saisons_matchs import load_dataserieA, calculate_summary_statistics_serieA
#     from scripts_py.dash_bundes_all_saisons_matchs import load_data_bundes, calculate_summary_statistics_bundes
#     from scripts_py.dash_PL_all_saisons_matchs import load_data_PL, calculate_summary_statistics_PL
#     from scripts_py.app_dash import render_home_page 

#     def load_data(collection_name):
#         # Connexion à la base de données MongoDB
#         client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
#         db = client["SoccerStats"]
#         collection = db[collection_name]

#         # Récupération des données depuis MongoDB
#         data = list(collection.find({}, {'_id': 0}))  # Exclure l'ID MongoDB
#         df = pd.DataFrame(data)

#         # Remplacer les valeurs vides dans la colonne 'Score' par '0-0'
#         df['Score'].fillna('0-0', inplace=True)

#         # Filtrer les lignes avec des scores incorrects
#         df = df.dropna(subset=['Score'])

#         # Utiliser str.extract() pour séparer les scores en buts à domicile et à l'extérieur
#         df[['Buts_Domicile', 'Buts_Extérieur']] = df['Score'].str.extract(r'(\d+)-(\d+)')
#         df[['Buts_Domicile', 'Buts_Extérieur']] = df[['Buts_Domicile', 'Buts_Extérieur']].astype(float)  # Convertir en float

#         # Remplacer les chaînes vides par NaN
#         df['Buts_Domicile'].replace('', pd.NA, inplace=True)
#         df['Buts_Extérieur'].replace('', pd.NA, inplace=True)

#         # Convertir les colonnes en float64 pour inclure NaN
#         df['Buts_Domicile'] = df['Buts_Domicile'].astype('float64')
#         df['Buts_Extérieur'] = df['Buts_Extérieur'].astype('float64')

#         # Remplacer NaN par 0
#         df['Buts_Domicile'].fillna(0, inplace=True)
#         df['Buts_Extérieur'].fillna(0, inplace=True)

#         # Convertir les colonnes en entiers
#         df['Buts_Domicile'] = df['Buts_Domicile'].astype(int)
#         df['Buts_Extérieur'] = df['Buts_Extérieur'].astype(int)

#         # Convertir la colonne 'Journée' en numérique pour trier correctement
#         df['Journée'] = pd.to_numeric(df['Journée'], errors='coerce')

#         # Supprimer les lignes avec des valeurs nulles dans la colonne 'Journée'
#         df = df.dropna(subset=['Journée'])

#         # Trier le DataFrame par la colonne 'Journée'
#         df = df.sort_values(by='Journée')

#         return df

#     def load_data_de_Liga(collection_name):
#         # Connexion à la base de données MongoDB
#         client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
#         db = client["SoccerStats"]
#         collection = db[collection_name]

#         # Récupération des données depuis MongoDB
#         data = list(collection.find({}, {'_id': 0}))  # Exclure l'ID MongoDB
#         df = pd.DataFrame(data)

#         # Remplacer les valeurs vides dans la colonne 'Score' par '0-0'
#         df['Score'].fillna('0-0', inplace=True)

#         # Filtrer les lignes avec des scores incorrects
#         df = df.dropna(subset=['Score'])

#         # Utiliser str.extract() pour séparer les scores en buts à domicile et à l'extérieur
#         df[['Buts_Domicile', 'Buts_Extérieur']] = df['Score'].str.extract(r'(\d+)-(\d+)')
#         df[['Buts_Domicile', 'Buts_Extérieur']] = df[['Buts_Domicile', 'Buts_Extérieur']].astype(float)  # Convertir en float

#         # Remplacer les chaînes vides par NaN
#         df['Buts_Domicile'].replace('', pd.NA, inplace=True)
#         df['Buts_Extérieur'].replace('', pd.NA, inplace=True)

#         # Convertir les colonnes en float64 pour inclure NaN
#         df['Buts_Domicile'] = df['Buts_Domicile'].astype('float64')
#         df['Buts_Extérieur'] = df['Buts_Extérieur'].astype('float64')

#         # Remplacer NaN par 0
#         df['Buts_Domicile'].fillna(0, inplace=True)
#         df['Buts_Extérieur'].fillna(0, inplace=True)

#         # Convertir les colonnes en entiers
#         df['Buts_Domicile'] = df['Buts_Domicile'].astype(int)
#         df['Buts_Extérieur'] = df['Buts_Extérieur'].astype(int)

#         # Convertir la colonne 'Journée' en numérique pour trier correctement
#         df['Journée'] = pd.to_numeric(df['Journée'], errors='coerce')

#         # Supprimer les lignes avec des valeurs nulles dans la colonne 'Journée'
#         df = df.dropna(subset=['Journée'])

#         # Trier le DataFrame par la colonne 'Journée'
#         df = df.sort_values(by='Journée')

#         return df

#     def calculate_summary_statistics(df):
#         # Créer une DataFrame résumant le nombre total de buts marqués à domicile et à l'extérieur pour chaque équipe
#         goals_summary = df.groupby('Equipe_Domicile').agg({'Buts_Domicile': 'sum', 'Buts_Extérieur': 'sum'}).reset_index()

#         # Renommer les colonnes pour correspondre à ce que vous avez demandé
#         goals_summary = goals_summary.rename(columns={'Buts_Domicile': 'Buts_Domicile_Equipe', 'Buts_Extérieur': 'Buts_Extérieur_Equipe'})

#         # Ajouter une colonne pour les buts à l'extérieur de l'équipe visiteuse
#         goals_summary['Buts_Extérieur_Visiteur'] = df.groupby('Equipe_Extérieur')['Buts_Extérieur'].sum().reset_index()['Buts_Extérieur']

#         # Créer une DataFrame résumant le nombre total de buts encaissés à domicile et à l'extérieur pour chaque équipe
#         goals_conceded_summary = df.groupby('Equipe_Domicile').agg({'Buts_Extérieur': 'sum', 'Buts_Domicile': 'sum'}).reset_index()

#         # Renommer les colonnes pour correspondre à ce que vous avez demandé
#         goals_conceded_summary = goals_conceded_summary.rename(columns={'Buts_Domicile': 'Buts_Encaissés_Domicile', 'Buts_Extérieur': 'Buts_Encaissés_Extérieur'})

#         # Ajouter une colonne pour les buts encaissés à l'extérieur par l'équipe domicile
#         goals_conceded_summary['Buts_Encaissés_Extérieur_Domicile'] = df.groupby('Equipe_Domicile')['Buts_Extérieur'].sum().reset_index()['Buts_Extérieur']

#         # Ajouter une colonne pour les buts encaissés à domicile par l'équipe visiteuse
#         goals_conceded_summary['Buts_Encaissés_Domicile_Visiteur'] = df.groupby('Equipe_Extérieur')['Buts_Domicile'].sum().reset_index()['Buts_Domicile']

#         # Calculer le nombre total de buts par journée jouée
#         def calculate_total_goals(row):
#             goals = row['Score'].split('-')
#             if len(goals) == 2 and all(map(lambda x: x.isdigit(), goals)):
#                 return int(goals[0]) + int(goals[1])
#             else:
#                 return 0

#         df['TotalButs'] = df.apply(calculate_total_goals, axis=1)
#         goals_per_matchday = df.groupby('Journée')['TotalButs'].sum().reset_index().sort_values(by='Journée')

#         return goals_summary, goals_conceded_summary, goals_per_matchday

#     def dash_L1():
#         # Style pour le fond noir et les couleurs du texte
#         colors = {
#             'background': '#212121',  # Gris foncé
#             'text': '#FFFFFF'  # Blanc
#         }
        
#         # Mise en page de l'application
#         app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
#         app.layout = html.Div(style={'backgroundColor': colors['background'], 'color': colors['text'], 'height': '100vh'}, children=[
#             # Barre de navigation
#             dcc.Location(id='url', refresh=False),
#             dbc.NavbarSimple(
#                 children=[
#                     dbc.NavItem(dbc.NavLink("Accueil", href="/")),
#                     dbc.NavItem(dbc.NavLink("L1", href="/L1", id="nav-link-L1")),
#                     dbc.NavItem(dbc.NavLink("Liga", href="/Liga", id="nav-link-Liga")),  # Ajoutez l'ID ici
#                     dbc.NavItem(dbc.NavLink("SerieA", href="/SerieA", id="nav-link-SerieA")),
#                     dbc.NavItem(dbc.NavLink("Bundes", href="/Bundes", id="nav-link-Bundes")),
#                     dbc.NavItem(dbc.NavLink("PL", href="/PL", id="nav-link-PL")),
#                 ],
#                 brand="Navigation",
#                 brand_href="/",
#                 color="dark",
#                 dark=True,
#             ),
#             html.H1(
#                 id='header-title',
#                 children='Dashboard des différentes saisons de la ligue 1',
#                 style={
#                     'textAlign': 'center',
#                     'color': colors['text'],
#                     'padding': '20px'
#                 }
#             ),
#             html.Div(
#                 id='dropdown-container',
#                 children=[
#                     dcc.Dropdown(
#                         id='season-dropdown',
#                         options=[
#                             {'label': 'Saison actuelle', 'value': 'current_season'},
#                             {'label': '2022-2023', 'value': '22_23'},
#                             {'label': '2021-2022', 'value': '21_22'},
#                             {'label': '2020-2021', 'value': '20_21'}
#                         ],
#                         value='current_season',
#                         style={'width': '40%', 'backgroundColor': 'transparent', 'color': 'black', 'margin-left': '10px'}
#                     )
#                 ],
#                 style={'textAlign': 'center', 'margin-bottom': '20px'}
#                 ),

#             # Placeholder for graphs
#             html.Div(id='graph-container', className='container-fluid', style={'backgroundColor': colors['background'], 'padding': '20px', 'min-height': '100vh'})
#         ])
#             # Callback pour définir le style de fond noir sur toute la page d'accueil
#         @app.callback(
#             dash.dependencies.Output('app-layout', 'style'),
#             [dash.dependencies.Input('url', 'pathname')]
#         )
#         def update_layout_style(pathname):
#             if pathname == '/':
#                 return {'backgroundColor': colors['background'], 'padding': '50px'}  # Fond noir sur toute la page d'accueil
#             else:
#                 return {'backgroundColor': colors['background'], 'padding': '20px', 'min-height': '100vh'}  # Appliquer le fond noir sur les autres pages}  # Ne pas appliquer de style spécifique sur les autres pages
        
#         # Callback pour afficher ou masquer la liste déroulante en fonction de l'URL
#         @app.callback(
#             dash.dependencies.Output('dropdown-container', 'style'),
#             [dash.dependencies.Input('url', 'pathname')]
#         )
#         def display_dropdown(pathname):
#             if pathname == '/':
#                 return {'display': 'none'}  # Masquer la liste déroulante sur la page d'accueil
#             else:
#                 return {'textAlign': 'center', 'margin-bottom': '20px'}  # Afficher la liste déroulante sur les autres pages
            
#         # def render_home_page():
#         #     return html.Div(style={'backgroundColor': colors['background'], 'color': colors['text'], 'height': '100vh'}, children=[
#         #         dbc.NavbarSimple(
#         #             children=[
#         #                 dbc.NavItem(dbc.NavLink("Accueil", href="/")),
#         #                 dbc.NavItem(dbc.NavLink("L1", href="/L1", id="nav-link-L1")),
#         #                 dbc.NavItem(dbc.NavLink("Liga", href="/Liga", id="nav-link-Liga")),
#         #                 dbc.NavItem(dbc.NavLink("SerieA", href="/SerieA", id="nav-link-SerieA")),
#         #                 dbc.NavItem(dbc.NavLink("Bundes", href="/Bundes", id="nav-link-Bundes")),
#         #                 dbc.NavItem(dbc.NavLink("PL", href="/PL", id="nav-link-PL")),
#         #             ],
#         #             brand="Navigation",
#         #             brand_href="/",
#         #             color="dark",
#         #             dark=True,
#         #         ),
#         #         html.Div([
#         #             html.H1('Bienvenue sur notre Dashboard des championnats du big five Européen !', style={'margin-bottom': '80px'}),
#         #             html.P("Vous trouverez des Dashboards actualisés à la saison actuelle ainsi que les précédentes saisons !"),
#         #             html.P("Vous pourrez faire des comparaisons intéressantes entre les championnats et entre les saisons !", style={'margin-bottom': '20px'}),
#         #             html.P("Choisissez dans la navbar le championnat de votre choix ;)")
#         #         ], style={'textAlign': 'center', 'padding': '50px'})
#         #     ])


#         @app.callback(
#         [dash.dependencies.Output('graph-container', 'children'),
#         dash.dependencies.Output('header-title', 'children')],
#         [dash.dependencies.Input('url', 'pathname'),
#         dash.dependencies.Input('season-dropdown', 'value'),
#         dash.dependencies.Input('nav-link-Liga', 'n_clicks'),
#         dash.dependencies.Input('nav-link-L1', 'n_clicks'),
#         dash.dependencies.Input('nav-link-SerieA', 'n_clicks'),
#         dash.dependencies.Input('nav-link-Bundes', 'n_clicks'),
#         dash.dependencies.Input('nav-link-PL', 'n_clicks')]
#     )
        
#         # def render_home_page(pathname):
#         #     if pathname == '/':
#         #         return render_home_page()
#         #     else:
#         #         return html.H1('404 - Page not found', style={'color': 'white'})
        
#         def update_page_content(pathname, selected_season, n_clicks_Liga, n_clicks_L1, n_clicks_SerieA, n_clicks_bundes, n_clicks_PL):
#             if pathname == '/dashboard':
#                 return render_home_page(), ''

#             # Déterminer quel onglet a été cliqué en dernier
#             nav_clicks = {
#                 'Liga': n_clicks_Liga,
#                 'L1': n_clicks_L1,
#                 'SerieA': n_clicks_SerieA,
#                 'Bundes': n_clicks_bundes,
#                 'PL': n_clicks_PL
#             }
#             # Obtenir le nom de l'onglet cliqué le plus récemment
#             nav_clicks = {key: value if value is not None else 0 for key, value in nav_clicks.items()}


#             last_clicked = max(nav_clicks, key=nav_clicks.get)
            
#             # Mettre à jour le titre en fonction de l'onglet sélectionné
#             if last_clicked == 'Liga':
#                 return update_graphs('Liga', selected_season), 'Dashboard des différentes saisons de la Liga'
#             elif last_clicked == 'L1':
#                 return update_graphs('L1', selected_season), 'Dashboard des différentes saisons de la Ligue 1'
#             elif last_clicked == 'SerieA':
#                 return update_graphs('SerieA', selected_season), 'Dashboard des différentes saisons de la Série A'
#             elif last_clicked == 'Bundes':
#                 return update_graphs('Bundes', selected_season), 'Dashboard des différentes saisons de la Bundesliga'
#             elif last_clicked == 'PL':
#                 return update_graphs('PL', selected_season), 'Dashboard des différentes saisons de la PL'
#             else:
#                 return html.H1('404 - Page not found', style={'color': colors['text']}), ''

#         def update_graphs(championship, selected_season):
#             # Charger les données en fonction de l'onglet cliqué et de la saison sélectionnée
#             if championship == 'L1':  # Ligue 1
#                 if selected_season == '21_22':
#                     df = load_data("matchs_L1_21-22")
#                 elif selected_season == '22_23':
#                     df = load_data("matchs_L1_22-23")
#                 elif selected_season == '20_21':
#                     df = load_data("matchs_L1_20-21")
#                 else:
#                     df = load_data("matchs_L1")  # Pour la saison actuelle
#             elif championship == 'Liga':
#                 if selected_season == '21_22':
#                     df = load_data_liga("matchs_Liga_21-22")
#                 elif selected_season == '22_23':
#                     df = load_data_liga("matchs_Liga_22-23")
#                 elif selected_season == '20_21':
#                     df = load_data_liga("matchs_Liga_20-21")
#                 else:
#                     df = load_data_liga("matchs_Liga")  # Pour la saison actuelle
#             elif championship == 'SerieA':
#                 if selected_season == '21_22':
#                     df = load_dataserieA("matchs_serieA_21-22")
#                 elif selected_season == '22_23':
#                     df = load_dataserieA("matchs_serieA_22-23")
#                 elif selected_season == '20_21':
#                     df = load_dataserieA("matchs_serieA_20-21")
#                 else:
#                     df = load_dataserieA("matchs_serieA")  # Pour la saison actuelle
#             elif championship == 'Bundes':
#                 if selected_season == '21_22':
#                     df = load_data_bundes("matchs_bundesliga_20-21")
#                 elif selected_season == '22_23':
#                     df = load_data_bundes("matchs_bundesliga_22-23")
#                 elif selected_season == '20_21':
#                     df = load_data_bundes("matchs_bundesliga_20-21")
#                 else:
#                     df = load_data_bundes("matchs_bundesliga")  # Pour la saison actuelle
#             elif championship == 'PL':
#                 if selected_season == '21_22':
#                     df = load_data_PL("matchs_PL_20-21")
#                 elif selected_season == '22_23':
#                     df = load_data_PL("matchs_PL_22-23")
#                 elif selected_season == '20_21':
#                     df = load_data_PL("matchs_PL_20-21")
#                 else:
#                     df = load_data_PL("matchs_PL")  # Pour la saison actuelle
#             # Ajouter des conditions pour les autres championnats ici

#             goals_summary, goals_conceded_summary, goals_per_matchday = calculate_summary_statistics(df)

#             # Visualisation 1 : Histogramme des scores
#             histogram_figure = px.histogram(df, x='Score', title='Distribution des scores', labels={'Score': 'Buts'})
#             histogram_figure.update_traces(marker_color='#FFA500')
#             histogram_figure.update_layout({
#                 'plot_bgcolor': '#000000',  # Couleur de fond
#                 'paper_bgcolor': '#000000',  # Couleur du papier
#                 'font': {'color': '#FFFFFF'}  # Couleur du texte
#             })

#             # Visualisation 2 : Graphique de tendance des scores par journée
#             trend_figure = px.line(df, x='Journée', y='Score', title='Tendance des scores par journée')
#             trend_figure.update_traces(line_color='#FF5733')  # Définir la couleur du trait en orange
#             trend_figure.update_layout({
#                 'plot_bgcolor': '#000000',  # Couleur de fond
#                 'paper_bgcolor': '#000000',  # Couleur du papier
#                 'font': {'color': '#FFFFFF'}  # Couleur du texte
#             })

#             # Visualisation 3 : Carte thermique des buts marqués à domicile et à l'extérieur
#             heatmap_goals_figure = go.Figure(data=go.Heatmap(
#                 z=[goals_summary['Buts_Extérieur_Visiteur'], goals_summary['Buts_Domicile_Equipe']],
#                 x=goals_summary['Equipe_Domicile'],
#                 y=['Buts_Extérieur', 'Buts_Domicile'],
#                 colorscale='Viridis',  # Choisir une échelle de couleurs personnalisée (ex: Viridis, Inferno, etc.)
#                 hoverongaps=False
#             ), layout=go.Layout(
#                 plot_bgcolor=colors['background'],  # Couleur de fond
#                 paper_bgcolor=colors['background'],  # Couleur du papier
#                 font={'color': colors['text']}  # Couleur du texte
#             ))

#             # Visualisation 4 : Carte thermique des buts encaissés à domicile et à l'extérieur par équipe
#             heatmap_goals_conceded_figure = go.Figure(data=go.Heatmap(
#                 z=[goals_conceded_summary['Buts_Encaissés_Extérieur_Domicile'], goals_conceded_summary['Buts_Encaissés_Domicile_Visiteur']],
#                 x=goals_conceded_summary['Equipe_Domicile'],
#                 y=['Buts_Encaissés_Extérieur_Domicile', 'Buts_Encaissés_Domicile_Visiteur'],
#                 colorscale='Viridis',  # Choisir une échelle de couleurs personnalisée (ex: Viridis, Inferno, etc.)
#                 hoverongaps=False
#             ), layout=go.Layout(
#                 plot_bgcolor=colors['background'],  # Couleur de fond
#                 paper_bgcolor=colors['background'],  # Couleur du papier
#                 font={'color': colors['text']}  # Couleur du texte
#             ))

#             # Visualisation 5 : Diagramme de barres du nombre total de buts par journée jouée
#             goals_per_matchday_bar_chart = go.Figure(
#                 data=[
#                     go.Bar(
#                         x=goals_per_matchday['Journée'],  # Journée
#                         y=goals_per_matchday['TotalButs'],  # Nombre total de buts
#                         marker={'color': '#FF5733'},  # Couleur orange pour les barres
#                     )
#                 ],
#                 layout={
#                     'title': 'Nombre total de buts par journée jouée',
#                     'xaxis': {'title': 'Journée'},
#                     'yaxis': {'title': 'Nombre total de buts'},
#                     'plot_bgcolor': colors['background'],  # Couleur de fond
#                     'paper_bgcolor': colors['background'],  # Couleur du papier
#                     'font': {'color': colors['text']}  # Couleur du texte
#                 }
#             )

#             return [
#                 html.Div([
#                     html.H2('Histogramme du nombre des scores', style={'color': colors['text'], 'padding': '10px'}),
#                     dcc.Graph(
#                         id='goals-histogram',
#                         figure=histogram_figure,
#                         style={'height': '400px'},
#                         config={'displayModeBar': False}
#                     )
#                 ], style={'margin': '20px', 'borderRadius': '10px', 'backgroundColor': '#333333', 'padding': '20px'}),
#                 html.Div([
#                     html.H2('Graphique de tendance des scores par journée', style={'color': colors['text'], 'padding': '10px'}),
#                     dcc.Graph(
#                         id='goals-trend',
#                         figure=trend_figure,
#                         style={'height': '400px'},
#                         config={'displayModeBar': False}
#                     )
#                 ], style={'margin': '20px', 'borderRadius': '10px', 'backgroundColor': '#333333', 'padding': '20px'}),
#                 html.Div([
#                     html.H2('Carte thermique des buts marqués à domicile et à l\'extérieur', style={'color': colors['text'], 'padding': '10px'}),
#                     dcc.Graph(
#                         id='goals-heatmap',
#                         figure=heatmap_goals_figure,
#                         style={'height': '400px'},
#                         config={'displayModeBar': False}
#                     )
#                 ], style={'margin': '20px', 'borderRadius': '10px', 'backgroundColor': '#333333', 'padding': '20px'}),
#                 html.Div([
#                     html.H2('Carte thermique des buts encaissés à domicile et à l\'extérieur par équipe', style={'color': colors['text'], 'padding': '10px'}),
#                     dcc.Graph(
#                         id='goals-conceded-heatmap',
#                         figure=heatmap_goals_conceded_figure,
#                         style={'height': '400px'},
#                         config={'displayModeBar': False}
#                     )
#                 ], style={'margin': '20px', 'borderRadius': '10px', 'backgroundColor': '#333333', 'padding': '20px'}),
#                 html.Div([
#                     html.H2('Diagramme de barres du nombre total de buts par journée jouée', style={'color': colors['text'], 'padding': '10px'}),
#                     dcc.Graph(
#                         id='goals-per-matchday-bar-chart',
#                         figure=goals_per_matchday_bar_chart,
#                         style={'height': '400px'},
#                         config={'displayModeBar': False}
#                     )
#                 ], style={'margin': '20px', 'borderRadius': '10px', 'backgroundColor': '#333333', 'padding': '20px'})
#             ]

#         return app

#     # Si le script est exécuté en tant que programme principal
#     if __name__ == '__main__':
#         app = dash_L1()
#         app.run_server(debug=True, use_reloader=True)
# create_dash_app

def get_logos_ligue_1():
    logos_ligue_1 = {
        "Paris SG": "https://seatpick.com/_next/image?url=https:%2F%2Fres.cloudinary.com%2Fdfwt0gh29%2Fimage%2Fupload%2Fc_pad%2Ch_170%2Cw_170%2Flogos%2Fligue1%2Fpsg&w=256&q=75",
        "Marseille": "https://betmonitor.com/amar/logo/Football/Marseille.png",
        "Lyon": "https://fullmatchsreplay.com/team/lyon.png",
        "Monaco": "https://www.score.fr/uploads/63d150f65c531-Monaco-144.png",
        "Lille": "https://1.bp.blogspot.com/-pPZv7PjsE8w/YHZTT21C08I/AAAAAAAACo4/eeA-Kyt0BScxqoCOTaibPXAYLnzY8wqRwCLcBGAsYHQ/s72-c/Lille%2Bfc%2BOSC%2Bfrance%2Bligue%2B1%2Bteams.png",
        "Rennes": "https://cdn.sportmonks.com/images/soccer/teams/22/598.png",
        "Montpellier": "https://cdn.sportmonks.com/images/soccer/teams/5/581.png",
        "Brest": "https://png100.maxifoot-live.com/brest.png",
        "Lens": "https://cdn.sportmonks.com/images/soccer/teams/15/271.png",
        "Le Havre": "https://cdn.footystats.org/img/teams/france-le-havre-ac.png",
        "Nice": "https://ftsdlskits.com/wp-content/uploads/2023/02/OGC-Nice-logo.png",
        "Metz": "https://1000logos.net/wp-content/uploads/2022/01/FC-Metz-logo-140x79.png",
        "Strasbourg": "https://assets.b365api.com/images/team/b/1659.png",
        "Lorient": "https://tmssl.akamaized.net/images/wappen/head/1158.png?lm=1406642498",
        "Nantes": "https://www.score.fr/uploads/63d15154a5a94-Nantes-144.png",
        "Toulouse": "https://cdn.sportmonks.com/images/soccer/teams/1/289.png",
        "Reims": "https://cdn.sportmonks.com/images/soccer/teams/4/1028.png",
        "Clermont F.": "https://static.sportytrader.com/icons/foot/teams/120x120/clermont_1.webp"
        }
    return logos_ligue_1

# Fonction pour récupérer les données à partir de MongoDB LIGUE1
def get_data_from_mongodb():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    classement_collection_name = "classement_L1"
    matchs_collection_name = "matchs_L1"
    buteurs_collection_name = "Buteurs_L1"
    
    logos_ligue_1 = {
    "Paris SG": "https://seatpick.com/_next/image?url=https:%2F%2Fres.cloudinary.com%2Fdfwt0gh29%2Fimage%2Fupload%2Fc_pad%2Ch_170%2Cw_170%2Flogos%2Fligue1%2Fpsg&w=256&q=75",
    "Marseille": "https://betmonitor.com/amar/logo/Football/Marseille.png",
    "Lyon": "https://fullmatchsreplay.com/team/lyon.png",
    "Monaco": "https://www.score.fr/uploads/63d150f65c531-Monaco-144.png",
    "Lille": "https://1.bp.blogspot.com/-pPZv7PjsE8w/YHZTT21C08I/AAAAAAAACo4/eeA-Kyt0BScxqoCOTaibPXAYLnzY8wqRwCLcBGAsYHQ/s72-c/Lille%2Bfc%2BOSC%2Bfrance%2Bligue%2B1%2Bteams.png",
    "Rennes": "https://cdn.sportmonks.com/images/soccer/teams/22/598.png",
    "Montpellier": "https://cdn.sportmonks.com/images/soccer/teams/5/581.png",
    "Brest": "https://png100.maxifoot-live.com/brest.png",
    "Lens": "https://cdn.sportmonks.com/images/soccer/teams/15/271.png",
    "Le Havre": "https://cdn.footystats.org/img/teams/france-le-havre-ac.png",
    "Nice": "https://ftsdlskits.com/wp-content/uploads/2023/02/OGC-Nice-logo.png",
    "Metz": "https://1000logos.net/wp-content/uploads/2022/01/FC-Metz-logo-140x79.png",
    "Strasbourg": "https://assets.b365api.com/images/team/b/1659.png",
    "Lorient": "https://tmssl.akamaized.net/images/wappen/head/1158.png?lm=1406642498",
    "Nantes": "https://www.score.fr/uploads/63d15154a5a94-Nantes-144.png",
    "Toulouse": "https://cdn.sportmonks.com/images/soccer/teams/1/289.png",
    "Reims": "https://cdn.sportmonks.com/images/soccer/teams/4/1028.png",
    "Clermont F.": "https://static.sportytrader.com/icons/foot/teams/120x120/clermont_1.webp"
    }

    logos_joueur_ligue1 = {
        "K. MBAPPE": "https://www.futwiz.com/assets/img/fifa23/faces/231747.png",
        "J. DAVID": "https://api.efootballdb.com/assets/2022/players/124839_.png.webp",
        "A. LACAZETTE": "https://api.efootballdb.com/assets/2022/players/40507_.png.webp",
        "P. AUBAMEYANG": "https://cdn.fifacm.com/content/media/imgs/fifa21/players/p188567.png?v=22",
        "W. BEN YEDDER": "https://game-assets.fut.gg/2024/players/199451.2.png?quality=80&width=250",
        "T. DALLINGA": "https://cdn.fifacm.com/content/media/imgs/fc24/players/p252021.png?v=22",
        "T. MOFFI": "https://api.efootballdb.com/assets/2022/players/134939_.png.webp",
        "MOSTAFA MOHAMED": "https://www.leballonrond.fr/img/jogadores/19/610619_20230124093042_mostafa_mohamed.png",
        "E. WAHI": "https://game-assets.fut.gg/2024/players/260407.2.png?quality=100&width=130", 
        "F. BALOGUN": "https://api.efootballdb.com/assets/2022/players/133551_.png.webp",
        "G. RAMOS": "https://static.flashscore.com/res/image/data/8v4pFVzB-8nvXRYze.png",
        "T. SAVANIER": "https://api.efootballdb.com/assets/2022/players/102483_.png.webp"

    }

    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    classement_collection = db[classement_collection_name]
    matchs_collection = db[matchs_collection_name]
    buteurs_collection = db[buteurs_collection_name]
    
    # Exclure l'ID des données récupérées
    classement = list(classement_collection.find({}, {"_id": 0}))
    matchs = list(matchs_collection.find({}, {"_id": 0}))
    
    # Récupérer les 10 meilleurs buteurs avec leur équipe
    buteurs = list(buteurs_collection.find({}, {"_id": 0, "Derniers_Buts": 0}).limit(10))

    # Ajouter les URLs des logos dans le classement
    for team_data in classement:
        team_name = team_data["Equipe"]
        if team_name in logos_ligue_1:
            team_data["Logo"] = logos_ligue_1[team_name]
        else:
            team_data["Logo"] = None  # Mettez None si aucun logo trouvé pour l'équipe

    # Ajouter les URLs des logos dans les données des buteurs
    for player_data in buteurs:
        player_name = player_data["Joueur"]
        if player_name in logos_joueur_ligue1:
            player_data["Logo"] = logos_joueur_ligue1[player_name]
        else:
            player_data["Logo"] = None  # Mettez None si aucun logo trouvé pour le joueur
    

    
    return classement, matchs, buteurs, logos_ligue_1

# Fonction pour récupérer les données à partir de MongoDB LIGA
def get_data_from_mongodb2():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    classement_collection_name = "classement_Liga"
    matchs_collection_name = "matchs_Liga"
    buteurs_collection_name = "Buteurs_Liga"
    
    logos_equipe_liga= {
        "Real Madrid":"https://toppng.com/uploads/thumbnail/real-madrid-logo-11527531676wf17omamqx.png",
        "FC Barcelone": "https://www.dreamscity.net/wp-content/uploads/barca-logo-02-150x150.png",
        "Gérone FC": "https://png100.maxifoot-live.com/gerone-fc.png",
        "Atl. Madrid": "https://png60.maxifoot-live.com/atl-madrid.png",
        "Athletic Bilbao": "https://static.sportytrader.com/icons/foot/teams/120x120/athletic_de_bilbao_1.webp",
        "Real Sociedad": "https://assets-fr.imgfoot.com/media/cache/150x150/club/real-sociedad.png",
        "Betis Séville": "https://i.eurosport.com/_iss_/sport/football/club/logo/large/4102.png",
        "FC Valence": "https://png100.maxifoot-live.com/fc-valence.png",
        "Osasuna": "https://www.geniescout.com/logos/clubs/1685.png",
        "Villarreal": "https://cdn.footboom.net/img/stats/teams/490.png",
        "Getafe": "https://toppng.com/uploads/thumbnail/getafe-logo-png-11536010522kzo86ihwrl.png", 
        "UD Las Palmas": "https://as01.epimg.net/img/comunes/fotos/fichas/equipos/medium/9.png",
        "Alaves": "https://cdn.sportmonks.com/images/soccer/teams/31/2975.png",
        "Real Majorque": "https://png100.maxifoot-live.com/real-majorque.png",
        "FC Seville": "https://www.footamax.com/logo/logo_fc_seville.gif",
        "Rayo Vallecano": "https://toppng.com/uploads/thumbnail/rayo-vallecano-de-madrid-vector-logo-11574289317wjdbexeaxn.png",
        "Celta Vigo": "https://fullmatchsreplay.com/team/Celta_Vigo.png",
        "Cadix": "https://png100.maxifoot-live.com/cadix.png",
        "Grenade": "https://www.footamax.com/logo/logo_grenade.gif",
        "Almeria": "https://fullmatchsreplay.com/team/Almeria.png"
    }

    logos_joueur_liga = {
        "A. DOVBYK": "https://cdn.fifacm.com/content/media/imgs/fc24/players/p242458.png?v=22",
        "A. BUDIMIR": "https://e00-marca.uecdn.es/assets/sports/headshots/football/all/png/144x144/94/94273.png",
        "J. BELLINGHAM": "https://cdn.footystats.org/img/players/england-jude-bellingham.png",
        "B. MAYORAL": "https://cdn.futwiz.com/assets/img/fc24/faces/228635.png",
        "A. MORATA": "https://api.efootballdb.com/assets/2022/players/43094_.png.webp",
        "A. SØRLOTH": "https://api.efootballdb.com/assets/2022/players/110838_.png.webp",
        "G. GURUZETA": "https://api.efootballdb.com/assets/2022/players/52849035817208_.png.webp",
        "R. LEWANDOWSKI": "https://www.careermodestars.com/assets/cdn/players/8990.png",
        "H. DURO": "https://sofoot.s3.eu-central-1.amazonaws.com/wp-content/uploads/2023/05/20093000/hugo-duro.png", 
        "VINÍCIUS JÚNIOR": "https://futhead.cursecdn.com/static/img/23/players/238794.png"
    }
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    classement_collection = db[classement_collection_name]
    matchs_collection = db[matchs_collection_name]
    buteurs_collection = db[buteurs_collection_name]
    
    # Exclure l'ID des données récupérées
    classement = list(classement_collection.find({}, {"_id": 0}))
    matchs = list(matchs_collection.find({}, {"_id": 0}))
    
    # Récupérer les 10 meilleurs buteurs avec leur équipe
    buteurs = list(buteurs_collection.find({}, {"_id": 0, "Derniers_Buts": 0}).limit(10))

    for team_data in classement:
        team_name = team_data["Équipe"] 
        if team_name in logos_equipe_liga:
            team_data["Logo"] = logos_equipe_liga[team_name]
        else:
            team_data["Logo"] = None  # Mettez None si aucun logo trouvé pour l'équipe

        # Ajouter les URLs des logos dans les données des buteurs
    for player_data in buteurs:
        player_name = player_data["Joueur"]
        if player_name in logos_joueur_liga:
            player_data["Logo"] = logos_joueur_liga[player_name]
        else:
            player_data["Logo"] = None  # Mettez None si aucun logo trouvé pour le joueur
    
    return classement, matchs, buteurs, logos_equipe_liga

# Fonction pour récupérer les données à partir de MongoDB LIGA
def get_data_from_mongodb3():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    classement_collection_name = "classement_SerieA"
    matchs_collection_name = "matchs_serieA"
    buteurs_collection_name = "Buteurs_SerieA"

    logos_equipe_serieA= {
        "Inter Milan":"https://www.footamax.com/logo/logo_inter_milan.gif",
        "Milan AC": "http://www.foot-actu.com/wp-content/uploads/2012/05/Logo_AC_Milan.svg_.png?593145",
        "Juventus Turin": "https://2.bp.blogspot.com/-d4mJLdfgFSk/WU_FWfrPpHI/AAAAAAABJuA/KxutVFlMNz0EAc0Smd46qg83cKXiJgDsQCLcBGAs/s1600/Juventus%2BFC128x.png",
        "Bologne": "https://png60.maxifoot-live.com/bologne.png",
        "AS Rome": "https://www.chainefoot.fr/storage/teams/asrome.png",
        "Atalanta Berga.": "https://png100.maxifoot-live.com/atalanta-bergame.png",
        "Lazio Rome": "https://cdn.sportmonks.com/images/soccer/teams/11/43.png",
        "Naples": "https://png100.maxifoot-live.com/naples.png",
        "Torino": "http://www.foot-actu.com/wp-content/uploads/2013/08/Torino_FC.png",
        "Fiorentina": "https://tmssl.akamaized.net/images/wappen/head/430.png?lm=1656660160",
        "Monza": "https://cdn.sportmonks.com/images/soccer/teams/28/1628.png", 
        "Genoa": "https://www.foot-actu.com/wp-content/uploads/2013/08/Logo_Genoa_CFC.png",
        "Lecce": "https://www.apuestas-deportivas.es/wp-content/uploads/imagenes/logos/lecce-logo.png",
        "Udinese": "https://1000logos.net/wp-content/uploads/2018/07/Udinese-Logo-140x88.png",
        "Hellas Vérone": "https://png60.maxifoot-live.com/hellas-verone.png",
        "Cagliari": "https://toppng.com/uploads/thumbnail/cagliari-logo-vector-download-free-11574169327kczcdvlzdt.png",
        "Frosinone": "https://png100.maxifoot-live.com/frosinone.png",
        "Empoli": "https://png100.maxifoot-live.com/empoli.png",
        "Sassuolo": "https://cdn.sportmonks.com/images/soccer/teams/26/2714.png",
        "Salernitana": "https://apuestas-deportivas.es/wp-content/uploads/imagenes/logos/salernitana-logo.png"
    }

    logos_joueur_serieA = {
        "L. MARTÍNEZ": "https://assets-es.imgfoot.com/media/cache/150x150/portrait/lautaro-martinez.png",
        "D. VLAHOVIC": "https://static.flashscore.com/res/image/data/xf66LPXg-xj0NJ68b.png",
        "P. DYBALA": "https://api.efootballdb.com/assets/2022/players/52782195325003_.png.webp",
        "O. GIROUD": "https://api.efootballdb.com/assets/2022/players/38627_.png.webp",
        "T. KOOPMEINERS": "https://api.efootballdb.com/assets/2022/players/118316_.png.webp",
        "A. GUDMUNDSSON": "https://static.flashscore.com/res/image/data/n11cA6il-GQYlV4jU.png",
        "J. ZIRKZEE": "https://api.efootballdb.com/assets/2022/players/132216_.png.webp",
        "V. OSIMHEN": "https://e00-marca.uecdn.es/assets/sports/headshots/football/459/png/144x144/218329.png",
        "R. LUKAKU": "https://api.efootballdb.com/assets/2022/players/40122_.png.webp", 
        "R. ORSOLINI": "https://static.flashscore.com/res/image/data/2g7A1xDa-KQJpoyOh.png",
        "D. ZAPATA": "https://static.flashscore.com/res/image/data/GY6JtMzB-zcl6fX2m.png"
    }
    
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    classement_collection = db[classement_collection_name]
    matchs_collection = db[matchs_collection_name]
    buteurs_collection = db[buteurs_collection_name]
    
    # Exclure l'ID des données récupérées
    classement = list(classement_collection.find({}, {"_id": 0}))
    matchs = list(matchs_collection.find({}, {"_id": 0}))
    
    # Récupérer les 10 meilleurs buteurs avec leur équipe
    buteurs = list(buteurs_collection.find({}, {"_id": 0, "Derniers_Buts": 0}).limit(10))

    for team_data in classement:
        team_name = team_data["Equipe"] 
        if team_name in logos_equipe_serieA:
            team_data["Logo"] = logos_equipe_serieA[team_name]
        else:
            team_data["Logo"] = None  # Mettez None si aucun logo trouvé pour l'équipe

        # Ajouter les URLs des logos dans les données des buteurs
    for player_data in buteurs:
        player_name = player_data["Joueur"]
        if player_name in logos_joueur_serieA:
            player_data["Logo"] = logos_joueur_serieA[player_name]
        else:
            player_data["Logo"] = None  # Mettez None si aucun logo trouvé pour le joueur
    
    return classement, matchs, buteurs, logos_equipe_serieA

# Fonction pour récupérer les données à partir de MongoDB LIGA
def get_data_from_mongodb4():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    classement_collection_name = "classement_bundesliga"
    matchs_collection_name = "matchs_bundesliga"
    buteurs_collection_name = "Buteurs_Bundesliga"
    


    logos_equipe_bundes= {
        "B. Leverkusen":"https://toppng.com/uploads/thumbnail/bayer-leverkusen-logo-vector-115742002113komqjgrii.png",
        "Bayern Munich": "https://assets-fr.imgfoot.com/media/cache/150x150/club/bayern-munich.png",
        "VfB Stuttgart": "https://1000logos.net/wp-content/uploads/2023/04/VfB-Stuttgart-logo-140x79.png",
        "Bor. Dortmund": "https://www.club-station.de/bilder/wappen/borussia-dortmund.gif",
        "RB Leipzig": "http://arrmaniac.de/empfehlungen/bilder/rbleLogo.png",
        "Eintr. Francfort": "https://png60.maxifoot-live.com/eintracht-francfort.png",
        "Augsbourg": "https://tmssl.akamaized.net/images/wappen/head/167.png?lm=1656582897",
        "Fribourg": "https://pbs.twimg.com/profile_images/402711201/freiburg-logo.png",
        "Hoffenheim": "https://cdn.sportmonks.com/images/soccer/teams/6/2726.png",
        "Werder Breme": "https://png60.maxifoot-live.com/werder-breme.png",
        "FC Heidenheim": "https://storage.fussballdaten.de/source/1/vereinhaupt/565/originale/1-fc-heidenheim-1846_2.png", 
        "Union Berlin": "https://storage.fussballdaten.de/source/1/vereinhaupt/282/originale/1-fc-union-berlin_3.png",
        "B. M'Gladbach": "https://i.eurosport.com/_iss_/sport/football/club/logo/large/4231.png",
        "Wolfsbourg": "https://png.vector.me/files/images/4/5/452395/wfl_wolfsburg_logo_vector.gif",
        "VfL Bochum": "https://www.club-station.de/bilder/wappen/vfl-bochum.gif",
        "Mayence": "https://www.sport365.fr/static/sport/bddimages/Teams/3/resized/977.png",
        "FC Cologne": "https://png100.maxifoot-live.com/fc-cologne.png",
        "Darmstadt": "https://cdn.sportmonks.com/images/soccer/teams/2/482.png"
    }

    logos_joueur_bundes = {
        "H. KANE": "https://static.flashscore.com/res/image/data/vXwPHlh5-YuIHFVzN.png",
        "S. GUIRASSY": "https://e00-marca.uecdn.es/assets/sports/headshots/football/169/png/144x144/162275.png",
        "L. OPENDA": "https://api.efootballdb.com/assets/2022/players/125370_.png.webp",
        "D. UNDAV": "https://api.efootballdb.com/assets/2022/players/146006_.png.webp",
        "E. DEMIROVIC": "https://cdn.fifacm.com/content/media/imgs/fifa21/players/p238900.png?v=22",
        "M. BEIER": "https://fifastatic.fifaindex.com/FIFA23/players/254117.png",
        "N. FÜLLKRUG": "https://api.efootballdb.com/assets/2022/players/142211_.png.webp",
        "D. MALEN": "https://cdn.sofifa.net/players/231/447/24_180.png",
        "V. BONIFACE": "https://cdn.fifacm.com/content/media/imgs/fc24/players/p247679.png?v=22", 
        "J. MUSIALA": "https://e00-marca.uecdn.es/assets/sports/headshots/football/156/png/144x144/244857.png",
        "T. KLEINDIENST": "https://static.flashscore.com/res/image/data/YmjZmWDa-E9pDw4pg.png"
    }

    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    classement_collection = db[classement_collection_name]
    matchs_collection = db[matchs_collection_name]
    buteurs_collection = db[buteurs_collection_name]
    
    # Exclure l'ID des données récupérées
    classement = list(classement_collection.find({}, {"_id": 0}))
    matchs = list(matchs_collection.find({}, {"_id": 0}))
    
    # Récupérer les 10 meilleurs buteurs avec leur équipe
    buteurs = list(buteurs_collection.find({}, {"_id": 0, "Derniers_Buts": 0}).limit(10))

    for team_data in classement:
        team_name = team_data["Équipe"] 
        if team_name in logos_equipe_bundes:
            team_data["Logo"] = logos_equipe_bundes[team_name]
        else:
            team_data["Logo"] = None  # Mettez None si aucun logo trouvé pour l'équipe

        # Ajouter les URLs des logos dans les données des buteurs
    for player_data in buteurs:
        player_name = player_data["Joueur"]
        if player_name in logos_joueur_bundes:
            player_data["Logo"] = logos_joueur_bundes[player_name]
        else:
            player_data["Logo"] = None  # Mettez None si aucun logo trouvé pour le joueur
    
    return classement, matchs, buteurs, logos_equipe_bundes

# Fonction pour récupérer les données à partir de MongoDB LIGA
def get_data_from_mongodb5():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    classement_collection_name = "classement_PL"
    matchs_collection_name = "matchs_PL"
    buteurs_collection_name = "Buteurs_PL"

    logos_equipe_PL= {
        "Liverpool":"http://icons.iconseeker.com/png/128/british-football-club/liverpool-fc.png",
        "Arsenal": "http://icons.iconseeker.com/png/128/soccer-teams/arsenal-fc-logo.png",
        "Manchest. City": "https://www.vsstats.com/img/sports/soccer/team/ManchesterCity9.png",
        "Aston Villa": "https://tmssl.akamaized.net/images/wappen/big/405.png?lm=1469443765",
        "Tottenham": "https://dlskits-logo.com/wp-content/uploads/2022/09/Tottenham-Hotspur-DLS-Logo-150x150.png",
        "Manchest. Utd": "http://icons.iconseeker.com/png/128/soccer-teams/manchester-united-fc-logo.png",
        "West Ham": "https://kids.kiddle.co/images/thumb/c/c2/West_Ham_United_FC_logo.svg/169px-West_Ham_United_FC_logo.svg.png",
        "Newcastle": "http://icons.iconseeker.com/png/128/british-football-club/newcastle-united.png",
        "Brighton": "https://www.geniescout.com/logos/clubs/618.png",
        "Wolverhampton": "https://logodownload.org/wp-content/uploads/2019/04/wolverhampton-logo-escudo-140x140.png",
        "Chelsea": "https://www.footamax.com/logo/logo_chelsea.gif", 
        "Fulham": "https://aux.iconspalace.com/uploads/fulham-fc-logo-icon-128.png",
        "Bournemouth": "https://toppng.com/uploads/thumbnail/afc-bournemouth-11536012908tqva3ttaht.png",
        "Crystal Palace": "https://www.livepremierleague.net/EPL/Crystal-Palace.png",
        "Brentford": "https://www.afcb.co.uk/media/16232/brentford.png",
        "Everton": "https://logodownload.org/wp-content/uploads/2019/04/everton-logo-escudo-0-140x140.png",
        "Nottingham F.": "https://res.cloudinary.com/dq4uxsxpv/image/upload/nottingham-forest-fc.webp",
        "Luton Town": "https://tmssl.akamaized.net/images/wappen/big/1031.png?lm=1457723228",
        "Burnley": "https://fmshots.com/bk/0nK6xtQ.png", 
        "Sheffield Utd": "https://www.colours-of-football.com/colours03/eng/sheff_u/Sheffield-United.png"
    }

    logos_joueur_PL = {
        "E. HÅLAND": "http://www.ogol.com.br/img/jogadores/41/512741_20220806173436_erling_haaland.png",
        "O. WATKINS": "https://api.efootballdb.com/assets/2022/players/105587476122906_.png.webp",
        "D. SOLANKE": "https://api.efootballdb.com/assets/2022/players/52849035811048_.png.webp",
        "M. SALAH": "https://i.pinimg.com/originals/a2/af/06/a2af0605cac4a787ca513586f6b7046d.png",
        "H. SON": "https://assets-es.imgfoot.com/media/cache/150x150/portrait/heung-min-son.png",
        "J. BOWEN": "https://api.efootballdb.com/assets/2022/players/52782463827466_.png.webp",
        "A. ISAK": "https://api.efootballdb.com/assets/2022/players/115079_.png.webp",
        "B. SAKA": "https://static.flashscore.com/res/image/data/pCHmmTYA-GxPHNWyM.png",
        "C. PALMER": "https://static.flashscore.com/res/image/data/I51rwQf5-l2N3kOUT.png", 
        "P. FODEN": "https://www.pesmaster.com/efootball-2022/graphics/players/Variation2022/105568685637752_.png"
    }

    
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    classement_collection = db[classement_collection_name]
    matchs_collection = db[matchs_collection_name]
    buteurs_collection = db[buteurs_collection_name]
    
    # Exclure l'ID des données récupérées
    classement = list(classement_collection.find({}, {"_id": 0}))
    matchs = list(matchs_collection.find({}, {"_id": 0}))
    
    # Récupérer les 10 meilleurs buteurs avec leur équipe
    buteurs = list(buteurs_collection.find({}, {"_id": 0, "Derniers_Buts": 0}).limit(10))

    for team_data in classement:
        team_name = team_data["Equipe"] 
        if team_name in logos_equipe_PL:
            team_data["Logo"] = logos_equipe_PL[team_name]
        else:
            team_data["Logo"] = None  # Mettez None si aucun logo trouvé pour l'équipe

        # Ajouter les URLs des logos dans les données des buteurs
    for player_data in buteurs:
        player_name = player_data["Joueur"]
        if player_name in logos_joueur_PL:
            player_data["Logo"] = logos_joueur_PL[player_name]
        else:
            player_data["Logo"] = None  # Mettez None si aucun logo trouvé pour le joueur
    
    return classement, matchs, buteurs, logos_equipe_PL

# Fonction pour récupérer les matchs pour une journée spécifique
def get_matchs_by_journee(journee):
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    matchs_collection_name = "matchs_L1"
    
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    matchs_collection = db[matchs_collection_name]
    
    # Convertir la journée en chaîne de caractères si nécessaire
    journee_str = str(journee)
    
    matchs = list(matchs_collection.find({"Journée": journee_str}, {"_id": 0}))
    
    # Convertir la valeur de la journée en entier pour la comparer
    journee_int = int(journee)
    
    # Mettre à jour la propriété disponible_pour_prevision
    for match in matchs:
        match['disponible_pour_prevision'] = int(match['Journée']) >= journee_int
    
    return matchs

# Fonction pour récupérer les matchs pour une journée spécifique
def get_matchs_by_journee2(journee):
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    matchs_collection_name = "matchs_Liga"
    
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    matchs_collection = db[matchs_collection_name]
    
    # Convertir la journée en chaîne de caractères si nécessaire
    journee_str = str(journee)
    
    matchs = list(matchs_collection.find({"Journée": journee_str}, {"_id": 0}))
    
    # Convertir la valeur de la journée en entier pour la comparer
    journee_int = int(journee)
    
    # Mettre à jour la propriété disponible_pour_prevision
    for match in matchs:
        match['disponible_pour_prevision'] = int(match['Journée']) >= journee_int
    
    return matchs

# Fonction pour récupérer les matchs pour une journée spécifique
def get_matchs_by_journee3(journee):
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    matchs_collection_name = "matchs_serieA"
    
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    matchs_collection = db[matchs_collection_name]
    
    # Convertir la journée en chaîne de caractères si nécessaire
    journee_str = str(journee)
    
    matchs = list(matchs_collection.find({"Journée": journee_str}, {"_id": 0}))
    
    # Convertir la valeur de la journée en entier pour la comparer
    journee_int = int(journee)
    
    # Mettre à jour la propriété disponible_pour_prevision
    for match in matchs:
        match['disponible_pour_prevision'] = int(match['Journée']) >= journee_int
    
    return matchs


# Fonction pour récupérer les matchs pour une journée spécifique
def get_matchs_by_journee4(journee):
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    matchs_collection_name = "matchs_bundesliga"
    
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    matchs_collection = db[matchs_collection_name]
    
    # Convertir la journée en chaîne de caractères si nécessaire
    journee_str = str(journee)
    
    matchs = list(matchs_collection.find({"Journée": journee_str}, {"_id": 0}))
    
    # Convertir la valeur de la journée en entier pour la comparer
    journee_int = int(journee)
    
    # Mettre à jour la propriété disponible_pour_prevision
    for match in matchs:
        match['disponible_pour_prevision'] = int(match['Journée']) >= journee_int
    
    return matchs

# Fonction pour récupérer les matchs pour une journée spécifique
def get_matchs_by_journee5(journee):
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    matchs_collection_name = "matchs_PL"
    
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    matchs_collection = db[matchs_collection_name]
    
    # Convertir la journée en chaîne de caractères si nécessaire
    journee_str = str(journee)
    
    matchs = list(matchs_collection.find({"Journée": journee_str}, {"_id": 0}))
    
    # Convertir la valeur de la journée en entier pour la comparer
    journee_int = int(journee)
    
    # Mettre à jour la propriété disponible_pour_prevision
    for match in matchs:
        match['disponible_pour_prevision'] = int(match['Journée']) >= journee_int
    
    return matchs

# Fonction pour récupérer les prévisions de matchs
def generate_predictions():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]

    matchs_collection = db["matchs_L1"]
    classement_collection = db["classement_L1"]
    buteurs_collection = db["Buteurs_L1"]

    matchs_passes = list(matchs_collection.find({"Journée": {"$gte": "27", "$lte": "34"}}))
    classement = list(classement_collection.find())
    buteurs = list(buteurs_collection.find())

    predictions = []
    for match in matchs_passes:
        equipe_domicile = match['Equipe_Domicile']
        equipe_exterieur = match['Equipe_Extérieur']
        
        classement_domicile = next((equipe for equipe in classement if equipe['Equipe'] == equipe_domicile), None)
        classement_exterieur = next((equipe for equipe in classement if equipe['Equipe'] == equipe_exterieur), None)
        
        if classement_domicile and classement_exterieur:
            # Générer des pourcentages aléatoires
            pourcentage_victoire_domicile = random.randint(1, 50)  # Maximum 50 %
            pourcentage_victoire_exterieur = random.randint(1, 50)  # Maximum 50 %
            pourcentage_match_nul = 100 - (pourcentage_victoire_domicile + pourcentage_victoire_exterieur)
            
            # Vérifier que les pourcentages sont positifs et que la somme ne dépasse pas 100 %
            if pourcentage_victoire_domicile >= 0 and pourcentage_victoire_exterieur >= 0 and pourcentage_match_nul >= 0:
                prediction = {
                    'equipe_domicile': equipe_domicile,
                    'equipe_exterieur': equipe_exterieur,
                    'pourcentage_victoire_domicile': pourcentage_victoire_domicile,
                    'pourcentage_victoire_exterieur': pourcentage_victoire_exterieur,
                    'pourcentage_match_nul': pourcentage_match_nul
                }
                predictions.append(prediction)

    return predictions

# Fonction pour récupérer les prévisions de matchs de liga
def generate_predictions2():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]

    matchs_collection = db["matchs_Liga"]
    classement_collection = db["classement_Liga"]
    buteurs_collection = db["Buteurs_Liga"]

    matchs_passes = list(matchs_collection.find({"Journée": {"$gte": "30", "$lte": "38"}}))
    classement = list(classement_collection.find())
    buteurs = list(buteurs_collection.find())

    predictions = []
    for match in matchs_passes:
        equipe_domicile = match['Equipe_Domicile']
        equipe_exterieur = match['Equipe_Extérieur']
        
        classement_domicile = next((equipe for equipe in classement if equipe['Équipe'] == equipe_domicile), None)
        classement_exterieur = next((equipe for equipe in classement if equipe['Équipe'] == equipe_exterieur), None)
        
        if classement_domicile and classement_exterieur:
            # Générer des pourcentages aléatoires
            pourcentage_victoire_domicile = random.randint(1, 50)  # Maximum 50 %
            pourcentage_victoire_exterieur = random.randint(1, 50)  # Maximum 50 %
            pourcentage_match_nul = 100 - (pourcentage_victoire_domicile + pourcentage_victoire_exterieur)
            
            # Vérifier que les pourcentages sont positifs et que la somme ne dépasse pas 100 %
            if pourcentage_victoire_domicile >= 0 and pourcentage_victoire_exterieur >= 0 and pourcentage_match_nul >= 0:
                prediction = {
                    'equipe_domicile': equipe_domicile,
                    'equipe_exterieur': equipe_exterieur,
                    'pourcentage_victoire_domicile': pourcentage_victoire_domicile,
                    'pourcentage_victoire_exterieur': pourcentage_victoire_exterieur,
                    'pourcentage_match_nul': pourcentage_match_nul
                }
                predictions.append(prediction)

    return predictions

# Fonction pour récupérer les prévisions de matchs de serieA
def generate_predictions3():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]

    matchs_collection = db["matchs_serieA"]
    classement_collection = db["classement_SerieA"]
    buteurs_collection = db["Buteurs_SerieA"]

    matchs_passes = list(matchs_collection.find({"Journée": {"$gte": "30", "$lte": "38"}}))
    classement = list(classement_collection.find())
    buteurs = list(buteurs_collection.find())

    predictions = []
    for match in matchs_passes:
        equipe_domicile = match['Equipe_Domicile']
        equipe_exterieur = match['Equipe_Extérieur']
        
        classement_domicile = next((equipe for equipe in classement if equipe['Equipe'] == equipe_domicile), None)
        classement_exterieur = next((equipe for equipe in classement if equipe['Equipe'] == equipe_exterieur), None)
        
        if classement_domicile and classement_exterieur:
            # Générer des pourcentages aléatoires
            pourcentage_victoire_domicile = random.randint(1, 50)  # Maximum 50 %
            pourcentage_victoire_exterieur = random.randint(1, 50)  # Maximum 50 %
            pourcentage_match_nul = 100 - (pourcentage_victoire_domicile + pourcentage_victoire_exterieur)
            
            # Vérifier que les pourcentages sont positifs et que la somme ne dépasse pas 100 %
            if pourcentage_victoire_domicile >= 0 and pourcentage_victoire_exterieur >= 0 and pourcentage_match_nul >= 0:
                prediction = {
                    'equipe_domicile': equipe_domicile,
                    'equipe_exterieur': equipe_exterieur,
                    'pourcentage_victoire_domicile': pourcentage_victoire_domicile,
                    'pourcentage_victoire_exterieur': pourcentage_victoire_exterieur,
                    'pourcentage_match_nul': pourcentage_match_nul
                }
                predictions.append(prediction)

    return predictions

# Fonction pour récupérer les prévisions de matchs de serieA
def generate_predictions4():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]

    matchs_collection = db["matchs_bundesliga"]
    classement_collection = db["classement_bundesliga"]
    buteurs_collection = db["Buteurs_Bundesliga"]

    matchs_passes = list(matchs_collection.find({"Journée": {"$gte": "27", "$lte": "34"}}))
    classement = list(classement_collection.find())
    buteurs = list(buteurs_collection.find())

    predictions = []
    for match in matchs_passes:
        equipe_domicile = match['Equipe_Domicile']
        equipe_exterieur = match['Equipe_Extérieur']
        
        classement_domicile = next((equipe for equipe in classement if equipe['Équipe'] == equipe_domicile), None)
        classement_exterieur = next((equipe for equipe in classement if equipe['Équipe'] == equipe_exterieur), None)
        
        if classement_domicile and classement_exterieur:
            # Générer des pourcentages aléatoires
            pourcentage_victoire_domicile = random.randint(1, 50)  # Maximum 50 %
            pourcentage_victoire_exterieur = random.randint(1, 50)  # Maximum 50 %
            pourcentage_match_nul = 100 - (pourcentage_victoire_domicile + pourcentage_victoire_exterieur)
            
            # Vérifier que les pourcentages sont positifs et que la somme ne dépasse pas 100 %
            if pourcentage_victoire_domicile >= 0 and pourcentage_victoire_exterieur >= 0 and pourcentage_match_nul >= 0:
                prediction = {
                    'equipe_domicile': equipe_domicile,
                    'equipe_exterieur': equipe_exterieur,
                    'pourcentage_victoire_domicile': pourcentage_victoire_domicile,
                    'pourcentage_victoire_exterieur': pourcentage_victoire_exterieur,
                    'pourcentage_match_nul': pourcentage_match_nul
                }
                predictions.append(prediction)

    return predictions

# Fonction pour récupérer les prévisions de matchs de serieA
def generate_predictions5():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]

    matchs_collection = db["matchs_PL"]
    classement_collection = db["classement_PL"]
    buteurs_collection = db["Buteurs_PL"]

    matchs_passes = list(matchs_collection.find({"Journée": {"$gte": "27", "$lte": "38"}}))
    classement = list(classement_collection.find())
    buteurs = list(buteurs_collection.find())

    predictions = []
    for match in matchs_passes:
        equipe_domicile = match['Equipe_Domicile']
        equipe_exterieur = match['Equipe_Extérieur']
        
        classement_domicile = next((equipe for equipe in classement if equipe['Equipe'] == equipe_domicile), None)
        classement_exterieur = next((equipe for equipe in classement if equipe['Equipe'] == equipe_exterieur), None)
        
        if classement_domicile and classement_exterieur:
            # Générer des pourcentages aléatoires
            pourcentage_victoire_domicile = random.randint(1, 50)  # Maximum 50 %
            pourcentage_victoire_exterieur = random.randint(1, 50)  # Maximum 50 %
            pourcentage_match_nul = 100 - (pourcentage_victoire_domicile + pourcentage_victoire_exterieur)
            
            # Vérifier que les pourcentages sont positifs et que la somme ne dépasse pas 100 %
            if pourcentage_victoire_domicile >= 0 and pourcentage_victoire_exterieur >= 0 and pourcentage_match_nul >= 0:
                prediction = {
                    'equipe_domicile': equipe_domicile,
                    'equipe_exterieur': equipe_exterieur,
                    'pourcentage_victoire_domicile': pourcentage_victoire_domicile,
                    'pourcentage_victoire_exterieur': pourcentage_victoire_exterieur,
                    'pourcentage_match_nul': pourcentage_match_nul
                }
                predictions.append(prediction)

    return predictions


# Fonction pour afficher les tendances des buteurs
def afficher_tendances_buteurs():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    buteurs_collection = db["Buteurs_L1"]
    
    # Récupérer les données des buteurs
    buteurs = list(buteurs_collection.find({}, {"_id": 0}).limit(10))  # Limiter aux 10 meilleurs buteurs
    
    # Afficher les joueurs avec le plus de buts
    print("Top 10 des meilleurs buteurs :")
    for i, joueur in enumerate(buteurs):
        print(f"{i + 1}. {joueur['Joueur']} - {joueur['Buts']} but(s)")
    
    # Inverser l'ordre des joueurs et des buts pour afficher le meilleur buteur en haut
    buteurs.reverse()
    
    # Créer un graphique montrant l'évolution du nombre de buts marqués par les principaux buteurs au fil du temps
    joueurs = [joueur['Joueur'] for joueur in buteurs]
    buts = [joueur['Buts'] for joueur in buteurs]
    plt.bar(joueurs, buts)
    plt.xlabel('Joueur')
    plt.ylabel('Buts')
    plt.title('Nombre de buts marqués par les 10 meilleurs buteurs')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Fonction pour afficher les tendances des buteurs
def afficher_tendances_buteurs2():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    buteurs_collection = db["Buteurs_Liga"]
    
    # Récupérer les données des buteurs
    buteurs = list(buteurs_collection.find({}, {"_id": 0}).limit(10))  # Limiter aux 10 meilleurs buteurs
    
    # Afficher les joueurs avec le plus de buts
    print("Top 10 des meilleurs buteurs :")
    for i, joueur in enumerate(buteurs):
        print(f"{i + 1}. {joueur['Joueur']} - {joueur['Buts']} but(s)")
    
    # Inverser l'ordre des joueurs et des buts pour afficher le meilleur buteur en haut
    buteurs.reverse()
    
    # Créer un graphique montrant l'évolution du nombre de buts marqués par les principaux buteurs au fil du temps
    joueurs = [joueur['Joueur'] for joueur in buteurs]
    buts = [joueur['Buts'] for joueur in buteurs]
    plt.bar(joueurs, buts)
    plt.xlabel('Joueur')
    plt.ylabel('Buts')
    plt.title('Nombre de buts marqués par les 10 meilleurs buteurs')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Fonction pour afficher les tendances des buteurs serieA
def afficher_tendances_buteurs3():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    buteurs_collection = db["Buteurs_SerieA"]
    
    # Récupérer les données des buteurs
    buteurs = list(buteurs_collection.find({}, {"_id": 0}).limit(10))  # Limiter aux 10 meilleurs buteurs
    
    # Afficher les joueurs avec le plus de buts
    print("Top 10 des meilleurs buteurs :")
    for i, joueur in enumerate(buteurs):
        print(f"{i + 1}. {joueur['Joueur']} - {joueur['Buts']} but(s)")
    
    # Inverser l'ordre des joueurs et des buts pour afficher le meilleur buteur en haut
    buteurs.reverse()
    
    # Créer un graphique montrant l'évolution du nombre de buts marqués par les principaux buteurs au fil du temps
    joueurs = [joueur['Joueur'] for joueur in buteurs]
    buts = [joueur['Buts'] for joueur in buteurs]
    plt.bar(joueurs, buts)
    plt.xlabel('Joueur')
    plt.ylabel('Buts')
    plt.title('Nombre de buts marqués par les 10 meilleurs buteurs')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Fonction pour afficher les tendances des buteurs serieA
def afficher_tendances_buteurs4():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    buteurs_collection = db["Buteurs_Bundesliga"]
    
    # Récupérer les données des buteurs
    buteurs = list(buteurs_collection.find({}, {"_id": 0}).limit(10))  # Limiter aux 10 meilleurs buteurs
    
    # Afficher les joueurs avec le plus de buts
    print("Top 10 des meilleurs buteurs :")
    for i, joueur in enumerate(buteurs):
        print(f"{i + 1}. {joueur['Joueur']} - {joueur['Buts']} but(s)")
    
    # Inverser l'ordre des joueurs et des buts pour afficher le meilleur buteur en haut
    buteurs.reverse()
    
    # Créer un graphique montrant l'évolution du nombre de buts marqués par les principaux buteurs au fil du temps
    joueurs = [joueur['Joueur'] for joueur in buteurs]
    buts = [joueur['Buts'] for joueur in buteurs]
    plt.bar(joueurs, buts)
    plt.xlabel('Joueur')
    plt.ylabel('Buts')
    plt.title('Nombre de buts marqués par les 10 meilleurs buteurs')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Fonction pour afficher les tendances des buteurs serieA
def afficher_tendances_buteurs5():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    buteurs_collection = db["Buteurs_PL"]
    
    # Récupérer les données des buteurs
    buteurs = list(buteurs_collection.find({}, {"_id": 0}).limit(10))  # Limiter aux 10 meilleurs buteurs
    
    # Afficher les joueurs avec le plus de buts
    print("Top 10 des meilleurs buteurs :")
    for i, joueur in enumerate(buteurs):
        print(f"{i + 1}. {joueur['Joueur']} - {joueur['Buts']} but(s)")
    
    # Inverser l'ordre des joueurs et des buts pour afficher le meilleur buteur en haut
    buteurs.reverse()
    
    # Créer un graphique montrant l'évolution du nombre de buts marqués par les principaux buteurs au fil du temps
    joueurs = [joueur['Joueur'] for joueur in buteurs]
    buts = [joueur['Buts'] for joueur in buteurs]
    plt.bar(joueurs, buts)
    plt.xlabel('Joueur')
    plt.ylabel('Buts')
    plt.title('Nombre de buts marqués par les 10 meilleurs buteurs')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def afficher_tendances_equipes():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    classement_collection = db["classement_L1"]
    
    # Récupérer les données de classement
    classement_data = list(classement_collection.find({}, {"_id": 0}))
    
    # Trier les équipes par position dans le classement
    classement_data.sort(key=lambda x: x["Position"])
    
    # Extraire les données pour le graphique
    equipes = [equipe["Equipe"] for equipe in classement_data]
    victoires = [equipe["G"] for equipe in classement_data]
    nuls = [equipe["N"] for equipe in classement_data]
    defaites = [equipe["P"] for equipe in classement_data]
    
    # Afficher les tendances des équipes sous forme de graphique à barres
    plt.figure(figsize=(12, 6))
    plt.barh(equipes, victoires, color='green', label='Victoires')
    plt.barh(equipes, nuls, left=victoires, color='yellow', label='Matchs nuls')
    plt.barh(equipes, defaites, left=[x + y for x, y in zip(victoires, nuls)], color='red', label='Défaites')
    plt.xlabel('Nombre de matchs')
    plt.ylabel('Équipe')
    plt.title('Tendances des équipes')
    plt.legend()
    plt.tight_layout()
    plt.show()

def afficher_tendances_equipes2():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    classement_collection = db["classement_Liga"]
    
    # Récupérer les données de classement
    classement_data = list(classement_collection.find({}, {"_id": 0}))
    
    # Trier les équipes par position dans le classement
    classement_data.sort(key=lambda x: x["Position"])
    
    # Extraire les données pour le graphique
    equipes = [equipe["Équipe"] for equipe in classement_data]
    victoires = [equipe["G"] for equipe in classement_data]
    nuls = [equipe["N"] for equipe in classement_data]
    defaites = [equipe["P"] for equipe in classement_data]
    
    # Afficher les tendances des équipes sous forme de graphique à barres
    plt.figure(figsize=(12, 6))
    plt.barh(equipes, victoires, color='green', label='Victoires')
    plt.barh(equipes, nuls, left=victoires, color='yellow', label='Matchs nuls')
    plt.barh(equipes, defaites, left=[x + y for x, y in zip(victoires, nuls)], color='red', label='Défaites')
    plt.xlabel('Nombre de matchs')
    plt.ylabel('Équipe')
    plt.title('Tendances des équipes')
    plt.legend()
    plt.tight_layout()
    plt.show()

def afficher_tendances_equipes3():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    classement_collection = db["classement_SerieA"]
    
    # Récupérer les données de classement
    classement_data = list(classement_collection.find({}, {"_id": 0}))
    
    # Trier les équipes par position dans le classement
    classement_data.sort(key=lambda x: x["Position"])
    
    # Extraire les données pour le graphique
    equipes = [equipe["Equipe"] for equipe in classement_data]
    victoires = [equipe["G"] for equipe in classement_data]
    nuls = [equipe["N"] for equipe in classement_data]
    defaites = [equipe["P"] for equipe in classement_data]
    
    # Afficher les tendances des équipes sous forme de graphique à barres
    plt.figure(figsize=(12, 6))
    plt.barh(equipes, victoires, color='green', label='Victoires')
    plt.barh(equipes, nuls, left=victoires, color='yellow', label='Matchs nuls')
    plt.barh(equipes, defaites, left=[x + y for x, y in zip(victoires, nuls)], color='red', label='Défaites')
    plt.xlabel('Nombre de matchs')
    plt.ylabel('Équipe')
    plt.title('Tendances des équipes')
    plt.legend()
    plt.tight_layout()
    plt.show()

def afficher_tendances_equipes4():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    classement_collection = db["classement_bundesliga"]
    
    # Récupérer les données de classement
    classement_data = list(classement_collection.find({}, {"_id": 0}))
    
    # Trier les équipes par position dans le classement
    classement_data.sort(key=lambda x: x["Position"])
    
    # Extraire les données pour le graphique
    equipes = [equipe["Équipe"] for equipe in classement_data]
    victoires = [equipe["G"] for equipe in classement_data]
    nuls = [equipe["N"] for equipe in classement_data]
    defaites = [equipe["P"] for equipe in classement_data]
    
    # Afficher les tendances des équipes sous forme de graphique à barres
    plt.figure(figsize=(12, 6))
    plt.barh(equipes, victoires, color='green', label='Victoires')
    plt.barh(equipes, nuls, left=victoires, color='yellow', label='Matchs nuls')
    plt.barh(equipes, defaites, left=[x + y for x, y in zip(victoires, nuls)], color='red', label='Défaites')
    plt.xlabel('Nombre de matchs')
    plt.ylabel('Équipe')
    plt.title('Tendances des équipes')
    plt.legend()
    plt.tight_layout()
    plt.show()

def afficher_tendances_equipes5():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    classement_collection = db["classement_PL"]
    
    # Récupérer les données de classement
    classement_data = list(classement_collection.find({}, {"_id": 0}))
    
    # Trier les équipes par position dans le classement
    classement_data.sort(key=lambda x: x["Position"])
    
    # Extraire les données pour le graphique
    equipes = [equipe["Equipe"] for equipe in classement_data]
    victoires = [equipe["G"] for equipe in classement_data]
    nuls = [equipe["N"] for equipe in classement_data]
    defaites = [equipe["P"] for equipe in classement_data]
    
    # Afficher les tendances des équipes sous forme de graphique à barres
    plt.figure(figsize=(12, 6))
    plt.barh(equipes, victoires, color='green', label='Victoires')
    plt.barh(equipes, nuls, left=victoires, color='yellow', label='Matchs nuls')
    plt.barh(equipes, defaites, left=[x + y for x, y in zip(victoires, nuls)], color='red', label='Défaites')
    plt.xlabel('Nombre de matchs')
    plt.ylabel('Équipe')
    plt.title('Tendances des équipes')
    plt.legend()
    plt.tight_layout()
    plt.show()

# Fonction pour prédire les meilleurs buteurs
import re
from datetime import datetime

# Fonction pour prédire les meilleurs buteurs
# Fonction pour prédire les meilleurs buteurs en tenant compte du contexte de la saison
def predict_top_scorers(current_matchday, total_matchdays=34):
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    
    # Récupérer les données des buteurs
    buteurs_collection = db["Buteurs_L1"]
    buteurs = list(buteurs_collection.find({}, {"_id": 0}).limit(10))  # Limiter aux 10 meilleurs buteurs
    
    # Récupérer les données du classement des équipes
    classement_collection = db["classement_L1"]
    classement = {team['Equipe']: team['Position'] for team in classement_collection.find({}, {"_id": 0})}

    for joueur in buteurs:
        joueur['Buts'] = extract_goals(joueur.get('Buts', '0'))

    # Estimation du nombre de buts restants pour chaque joueur et attribution de la position prédite
    predictions = []
    for i, joueur in enumerate(buteurs, start=1):
        joueur_name = joueur.get('Joueur', 'Inconnu')
        equipe = joueur.get('Equipe', 'Inconnue')
        buts = extract_goals(joueur.get('Buts', '0'))
        matchs = extract_matches(joueur.get('Matchs', '0'))
        dernier_buts = joueur.get('Derniers_buts', 'Inconnu')
        
        # Estimer le nombre de matchs restants dans la saison
        matchs_restants = total_matchdays - current_matchday
        
        # Calculer le nombre moyen de buts par match
        moyenne_buts_par_match = buts / matchs

        # Prédire les buts pour les matchs restants en utilisant la moyenne
        buts_restants = moyenne_buts_par_match * matchs_restants

        buts_restants = str(int(buts_restants))  # Convertir en chaîne avant de fractionner
        if '.' in buts_restants:
            buts_restants = buts_restants.split('.')[0]

        buts_restants = int(buts_restants)

        # Ajuster les prédictions en tenant compte du classement de l'équipe
        if equipe in classement:
            position = classement[equipe]
            # Supposer que les joueurs des équipes mieux classées auront plus de chances de marquer des buts
            if position <= 5:  # Par exemple, considérer les 5 premières équipes comme meilleures
                buts_restants = int(buts_restants * 1.2)  # Augmenter le nombre de buts prévus
            elif position >= 15:  # Par exemple, considérer les 15 dernières équipes comme moins performantes
                buts_restants = int(buts_restants * 0.8)  # Réduire le nombre de buts prévus

        total_buts_predits = int(buts + buts_restants)
        joueur['Position'] = i
        predictions.append((total_buts_predits, joueur))

    # Tri des prédictions par nombre total de buts prédits
    predictions.sort(reverse=True, key=lambda x: x[0])

    # Formater les prédictions
    formatted_predictions = []
    for rank, (total_buts_predits, joueur) in enumerate(predictions[:10], start=1):
        joueur_name = joueur.get('Joueur', 'Inconnu')
        formatted_predictions.append(f"{joueur_name}: {total_buts_predits} but(s) à la fin de la saison (Position prédite: {rank})")
    
    return formatted_predictions

# Fonction pour prédire les meilleurs buteurs en tenant compte du contexte de la saison
def predict_top_scorers2(current_matchday, total_matchdays=38):
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    
    # Récupérer les données des buteurs
    buteurs_collection = db["Buteurs_Liga"]
    buteurs = list(buteurs_collection.find({}, {"_id": 0}).limit(10))  # Limiter aux 10 meilleurs buteurs
    
    # Récupérer les données du classement des équipes
    classement_collection = db["classement_Liga"]
    classement = {team['Équipe']: team['Position'] for team in classement_collection.find({}, {"_id": 0})}

    for joueur in buteurs:
        joueur['Buts'] = extract_goals(joueur.get('Buts', '0'))

    # Estimation du nombre de buts restants pour chaque joueur et attribution de la position prédite
    predictions = []
    for i, joueur in enumerate(buteurs, start=1):
        joueur_name = joueur.get('Joueur', 'Inconnu')
        equipe = joueur.get('Equipe', 'Inconnue')
        buts = extract_goals(joueur.get('Buts', '0'))
        matchs = extract_matches(joueur.get('Matchs', '0'))
        dernier_buts = joueur.get('Derniers_buts', 'Inconnu')
        
        # Estimer le nombre de matchs restants dans la saison
        matchs_restants = total_matchdays - current_matchday
        
        # Calculer le nombre moyen de buts par match
        moyenne_buts_par_match = buts / matchs

        # Prédire les buts pour les matchs restants en utilisant la moyenne
        buts_restants = moyenne_buts_par_match * matchs_restants

        buts_restants = str(int(buts_restants))  # Convertir en chaîne avant de fractionner
        if '.' in buts_restants:
            buts_restants = buts_restants.split('.')[0]

        buts_restants = int(buts_restants)

        # Ajuster les prédictions en tenant compte du classement de l'équipe
        if equipe in classement:
            position = classement[equipe]
            # Supposer que les joueurs des équipes mieux classées auront plus de chances de marquer des buts
            if position <= 5:  # Par exemple, considérer les 5 premières équipes comme meilleures
                buts_restants = int(buts_restants * 1.2)  # Augmenter le nombre de buts prévus
            elif position >= 15:  # Par exemple, considérer les 15 dernières équipes comme moins performantes
                buts_restants = int(buts_restants * 0.8)  # Réduire le nombre de buts prévus

        total_buts_predits = int(buts + buts_restants)
        joueur['Position'] = i
        predictions.append((total_buts_predits, joueur))

    # Tri des prédictions par nombre total de buts prédits
    predictions.sort(reverse=True, key=lambda x: x[0])

    # Formater les prédictions
    formatted_predictions = []
    for rank, (total_buts_predits, joueur) in enumerate(predictions[:10], start=1):
        joueur_name = joueur.get('Joueur', 'Inconnu')
        formatted_predictions.append(f"{joueur_name}: {total_buts_predits} but(s) à la fin de la saison (Position prédite: {rank})")
    
    return formatted_predictions

# Fonction pour prédire les meilleurs buteurs en tenant compte du contexte de la saison --> serie A
def predict_top_scorers3(current_matchday, total_matchdays=38):
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    
    # Récupérer les données des buteurs
    buteurs_collection = db["Buteurs_SerieA"]
    buteurs = list(buteurs_collection.find({}, {"_id": 0}).limit(10))  # Limiter aux 10 meilleurs buteurs
    
    # Récupérer les données du classement des équipes
    classement_collection = db["classement_SerieA"]
    classement = {team['Equipe']: team['Position'] for team in classement_collection.find({}, {"_id": 0})}

    for joueur in buteurs:
        joueur['Buts'] = extract_goals(joueur.get('Buts', '0'))

    # Estimation du nombre de buts restants pour chaque joueur et attribution de la position prédite
    predictions = []
    for i, joueur in enumerate(buteurs, start=1):
        joueur_name = joueur.get('Joueur', 'Inconnu')
        equipe = joueur.get('Equipe', 'Inconnue')
        buts = extract_goals(joueur.get('Buts', '0'))
        matchs = extract_matches(joueur.get('Matchs', '0'))
        dernier_buts = joueur.get('Derniers_buts', 'Inconnu')
        
        # Estimer le nombre de matchs restants dans la saison
        matchs_restants = total_matchdays - current_matchday
        
        # Calculer le nombre moyen de buts par match
        moyenne_buts_par_match = buts / matchs

        # Prédire les buts pour les matchs restants en utilisant la moyenne
        buts_restants = moyenne_buts_par_match * matchs_restants

        buts_restants = str(int(buts_restants))  # Convertir en chaîne avant de fractionner
        if '.' in buts_restants:
            buts_restants = buts_restants.split('.')[0]

        buts_restants = int(buts_restants)

        # Ajuster les prédictions en tenant compte du classement de l'équipe
        if equipe in classement:
            position = classement[equipe]
            # Supposer que les joueurs des équipes mieux classées auront plus de chances de marquer des buts
            if position <= 5:  # Par exemple, considérer les 5 premières équipes comme meilleures
                buts_restants = int(buts_restants * 1.2)  # Augmenter le nombre de buts prévus
            elif position >= 15:  # Par exemple, considérer les 15 dernières équipes comme moins performantes
                buts_restants = int(buts_restants * 0.8)  # Réduire le nombre de buts prévus

        total_buts_predits = int(buts + buts_restants)
        joueur['Position'] = i
        predictions.append((total_buts_predits, joueur))

    # Tri des prédictions par nombre total de buts prédits
    predictions.sort(reverse=True, key=lambda x: x[0])

    # Formater les prédictions
    formatted_predictions = []
    for rank, (total_buts_predits, joueur) in enumerate(predictions[:10], start=1):
        joueur_name = joueur.get('Joueur', 'Inconnu')
        formatted_predictions.append(f"{joueur_name}: {total_buts_predits} but(s) à la fin de la saison (Position prédite: {rank})")
    
    return formatted_predictions

# Fonction pour prédire les meilleurs buteurs en tenant compte du contexte de la saison --> serie A
def predict_top_scorers4(current_matchday, total_matchdays=34):
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    
    # Récupérer les données des buteurs
    buteurs_collection = db["Buteurs_Bundesliga"]
    buteurs = list(buteurs_collection.find({}, {"_id": 0}).limit(10))  # Limiter aux 10 meilleurs buteurs
    
    # Récupérer les données du classement des équipes
    classement_collection = db["classement_bundesliga"]
    classement = {team['Équipe']: team['Position'] for team in classement_collection.find({}, {"_id": 0})}

    for joueur in buteurs:
        joueur['Buts'] = extract_goals(joueur.get('Buts', '0'))

    # Estimation du nombre de buts restants pour chaque joueur et attribution de la position prédite
    predictions = []
    for i, joueur in enumerate(buteurs, start=1):
        joueur_name = joueur.get('Joueur', 'Inconnu')
        equipe = joueur.get('Équipe', 'Inconnue')
        buts = extract_goals(joueur.get('Buts', '0'))
        matchs = extract_matches(joueur.get('Matchs', '0'))
        dernier_buts = joueur.get('Derniers_buts', 'Inconnu')
        
        # Estimer le nombre de matchs restants dans la saison
        matchs_restants = total_matchdays - current_matchday
        
        # Calculer le nombre moyen de buts par match
        moyenne_buts_par_match = buts / matchs

        # Prédire les buts pour les matchs restants en utilisant la moyenne
        buts_restants = moyenne_buts_par_match * matchs_restants

        buts_restants = str(int(buts_restants))  # Convertir en chaîne avant de fractionner
        if '.' in buts_restants:
            buts_restants = buts_restants.split('.')[0]

        buts_restants = int(buts_restants)

        # Ajuster les prédictions en tenant compte du classement de l'équipe
        if equipe in classement:
            position = classement[equipe]
            # Supposer que les joueurs des équipes mieux classées auront plus de chances de marquer des buts
            if position <= 5:  # Par exemple, considérer les 5 premières équipes comme meilleures
                buts_restants = int(buts_restants * 1.2)  # Augmenter le nombre de buts prévus
            elif position >= 15:  # Par exemple, considérer les 15 dernières équipes comme moins performantes
                buts_restants = int(buts_restants * 0.8)  # Réduire le nombre de buts prévus

        total_buts_predits = int(buts + buts_restants)
        joueur['Position'] = i
        predictions.append((total_buts_predits, joueur))

    # Tri des prédictions par nombre total de buts prédits
    predictions.sort(reverse=True, key=lambda x: x[0])

    # Formater les prédictions
    formatted_predictions = []
    for rank, (total_buts_predits, joueur) in enumerate(predictions[:10], start=1):
        joueur_name = joueur.get('Joueur', 'Inconnu')
        formatted_predictions.append(f"{joueur_name}: {total_buts_predits} but(s) à la fin de la saison (Position prédite: {rank})")
    
    return formatted_predictions

# Fonction pour prédire les meilleurs buteurs en tenant compte du contexte de la saison --> serie A
def predict_top_scorers5(current_matchday, total_matchdays=34):
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    
    # Récupérer les données des buteurs
    buteurs_collection = db["Buteurs_PL"]
    buteurs = list(buteurs_collection.find({}, {"_id": 0}).limit(10))  # Limiter aux 10 meilleurs buteurs
    
    # Récupérer les données du classement des équipes
    classement_collection = db["classement_PL"]
    classement = {team['Equipe']: team['Position'] for team in classement_collection.find({}, {"_id": 0})}

    for joueur in buteurs:
        joueur['Buts'] = extract_goals(joueur.get('Buts', '0'))

    # Estimation du nombre de buts restants pour chaque joueur et attribution de la position prédite
    predictions = []
    for i, joueur in enumerate(buteurs, start=1):
        joueur_name = joueur.get('Joueur', 'Inconnu')
        equipe = joueur.get('Équipe', 'Inconnue')
        buts = extract_goals(joueur.get('Buts', '0'))
        matchs = extract_matches(joueur.get('Matchs', '0'))
        dernier_buts = joueur.get('Derniers_buts', 'Inconnu')
        
        # Estimer le nombre de matchs restants dans la saison
        matchs_restants = total_matchdays - current_matchday
        
        # Calculer le nombre moyen de buts par match
        moyenne_buts_par_match = buts / matchs

        # Prédire les buts pour les matchs restants en utilisant la moyenne
        buts_restants = moyenne_buts_par_match * matchs_restants

        buts_restants = str(int(buts_restants))  # Convertir en chaîne avant de fractionner
        if '.' in buts_restants:
            buts_restants = buts_restants.split('.')[0]

        buts_restants = int(buts_restants)

        # Ajuster les prédictions en tenant compte du classement de l'équipe
        if equipe in classement:
            position = classement[equipe]
            # Supposer que les joueurs des équipes mieux classées auront plus de chances de marquer des buts
            if position <= 5:  # Par exemple, considérer les 5 premières équipes comme meilleures
                buts_restants = int(buts_restants * 1.2)  # Augmenter le nombre de buts prévus
            elif position >= 15:  # Par exemple, considérer les 15 dernières équipes comme moins performantes
                buts_restants = int(buts_restants * 0.8)  # Réduire le nombre de buts prévus

        total_buts_predits = int(buts + buts_restants)
        joueur['Position'] = i
        predictions.append((total_buts_predits, joueur))

    # Tri des prédictions par nombre total de buts prédits
    predictions.sort(reverse=True, key=lambda x: x[0])

    # Formater les prédictions
    formatted_predictions = []
    for rank, (total_buts_predits, joueur) in enumerate(predictions[:10], start=1):
        joueur_name = joueur.get('Joueur', 'Inconnu')
        formatted_predictions.append(f"{joueur_name}: {total_buts_predits} but(s) à la fin de la saison (Position prédite: {rank})")
    
    return formatted_predictions

# Fonction pour extraire le nombre de buts d'une chaîne
# Modifier la fonction d'extraction des buts pour retourner un entier

def extract_goals(goals_str):
    if isinstance(goals_str, int):
        return goals_str
    
    if isinstance(goals_str, str):
        parts = goals_str.split()
        if parts:
            goals_part = parts[0]
            # Utiliser une expression régulière pour extraire uniquement les chiffres
            match = re.search(r'\d+', goals_part)
            if match:
                goals = int(match.group())
                
                # Vérifier si la chaîne contient un nombre décimal
                decimal_match = re.search(r'\d+[\.,]\d+', goals_part)
                if decimal_match:
                    # S'il y a un nombre décimal, l'ignorer en prenant seulement la partie entière
                    goals = int(decimal_match.group().split('.')[0].split(',')[0])
                
                return goals
    
    return 0

# Fonction pour extraire le nombre de matchs d'une chaîne
def extract_matches(matches_str):
    match = re.search(r'\d+', matches_str)
    if match:
        return int(match.group())
    else:
        return 0

def get_database_connection():
    mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
    db_name = "SoccerStats"
    
    client = pymongo.MongoClient(mongodb_uri)
    db = client[db_name]
    
    return db

import random
import random

import random

def predict_league_table(db):
    # Nom de la collection pour les prédictions de classement
    prediction_collection_name = "classement_L1_prediction"
    prediction_collection = db[prediction_collection_name]

    # Supprimer la collection si elle existe déjà
    if prediction_collection_name in db.list_collection_names():
        db.drop_collection(prediction_collection_name)

    # Créer une nouvelle collection pour les prédictions de classement
    db.create_collection(prediction_collection_name)

    # Récupérer les équipes depuis la collection de classement
    team_stats_collection = db["classement_L1"]
    teams_stats = list(team_stats_collection.find({}, {"_id": 0}))

    # Tri des équipes en fonction des points (ordre décroissant)
    teams_stats_sorted = sorted(teams_stats, key=lambda x: x["Pts"], reverse=True)

    # Positionner les équipes en fonction de leur nombre de points
    for position, equipe in enumerate(teams_stats_sorted, start=1):
        equipe["Position"] = position

        # Ajuster aléatoirement les résultats des matchs pour maintenir la cohérence
        equipe["J"] += 9
        g = random.randint(0, 9)  # Nombre de victoires aléatoires
        n = random.randint(0, 9 - g)  # Nombre de matchs nuls aléatoires
        p = 9 - g - n  # Nombre de défaites aléatoires
        equipe["G"] += g
        equipe["N"] += n
        equipe["P"] += p

        # Mettre à jour les points en fonction des résultats des matchs
        equipe["Pts"] += 3 * g + n  # 3 points pour une victoire, 1 point pour un match nul

        equipe["BP"] += random.randint(0, 18)  # Buts marqués aléatoirement
        equipe["BC"] += random.randint(0, 18)  # Buts encaissés aléatoirement
        equipe["Diff"] = equipe["BP"] - equipe["BC"]  # Mettre à jour la différence de buts

        # Ajouter un champ pour indiquer qu'il s'agit de données prédites
        equipe["Predicted"] = True

        # Insérer l'équipe ajustée dans la collection de prédictions de classement
        prediction_collection.insert_one(equipe)

    # Trier les équipes en fonction de leur position (ordre croissant)
    teams_stats_sorted = sorted(teams_stats_sorted, key=lambda x: x["Position"])

    # Ajuster les points des équipes en fonction de leur position
    for i, equipe in enumerate(teams_stats_sorted):
        equipe["Pts"] -= i

        # Mise à jour de l'équipe dans la collection de prédictions de classement
        prediction_collection.update_one({"Equipe": equipe["Equipe"]}, {"$set": {"Pts": equipe["Pts"]}})

    return True  # Indiquer que la prédiction a été réalisée avec succès


def predict_league_table2(db):
    # Nom de la collection pour les prédictions de classement
    prediction_collection_name = "classement_Liga_prediction"
    prediction_collection = db[prediction_collection_name]

    # Supprimer la collection si elle existe déjà
    if prediction_collection_name in db.list_collection_names():
        db.drop_collection(prediction_collection_name)

    # Créer une nouvelle collection pour les prédictions de classement
    db.create_collection(prediction_collection_name)

    # Récupérer les équipes depuis la collection de classement
    team_stats_collection = db["classement_Liga"]
    teams_stats = list(team_stats_collection.find({}, {"_id": 0}))

    # Tri des équipes en fonction des points (ordre décroissant)
    teams_stats_sorted = sorted(teams_stats, key=lambda x: x["Pts"], reverse=True)

    # Positionner les équipes en fonction de leur nombre de points
    for position, equipe in enumerate(teams_stats_sorted, start=1):
        equipe["Position"] = position

        # Ajuster aléatoirement les résultats des matchs pour maintenir la cohérence
        equipe["J"] += 9
        g = random.randint(0, 9)  # Nombre de victoires aléatoires
        n = random.randint(0, 9 - g)  # Nombre de matchs nuls aléatoires
        p = 9 - g - n  # Nombre de défaites aléatoires
        equipe["G"] += g
        equipe["N"] += n
        equipe["P"] += p

        # Mettre à jour les points en fonction des résultats des matchs
        equipe["Pts"] += 3 * g + n  # 3 points pour une victoire, 1 point pour un match nul

        equipe["BP"] += random.randint(0, 18)  # Buts marqués aléatoirement
        equipe["BC"] += random.randint(0, 18)  # Buts encaissés aléatoirement
        equipe["Diff"] = equipe["BP"] - equipe["BC"]  # Mettre à jour la différence de buts

        # Ajouter un champ pour indiquer qu'il s'agit de données prédites
        equipe["Predicted"] = True

        # Insérer l'équipe ajustée dans la collection de prédictions de classement
        prediction_collection.insert_one(equipe)

    # Trier les équipes en fonction de leur position (ordre croissant)
    teams_stats_sorted = sorted(teams_stats_sorted, key=lambda x: x["Position"])

    # Ajuster les points des équipes en fonction de leur position
    for i, equipe in enumerate(teams_stats_sorted):
        equipe["Pts"] -= i

        # Mise à jour de l'équipe dans la collection de prédictions de classement
        prediction_collection.update_one({"Équipe": equipe["Équipe"]}, {"$set": {"Pts": equipe["Pts"]}})

    return True  # Indiquer que la prédiction a été réalisée avec succès

def predict_league_table3(db):
    # Nom de la collection pour les prédictions de classement
    prediction_collection_name = "classement_SerieA_prediction"
    prediction_collection = db[prediction_collection_name]

    # Supprimer la collection si elle existe déjà
    if prediction_collection_name in db.list_collection_names():
        db.drop_collection(prediction_collection_name)

    # Créer une nouvelle collection pour les prédictions de classement
    db.create_collection(prediction_collection_name)

    # Récupérer les équipes depuis la collection de classement
    team_stats_collection = db["classement_SerieA"]
    teams_stats = list(team_stats_collection.find({}, {"_id": 0}))

    # Tri des équipes en fonction des points (ordre décroissant)
    teams_stats_sorted = sorted(teams_stats, key=lambda x: x["Pts"], reverse=True)

    # Positionner les équipes en fonction de leur nombre de points
    for position, equipe in enumerate(teams_stats_sorted, start=1):
        equipe["Position"] = position

        # Ajuster aléatoirement les résultats des matchs pour maintenir la cohérence
        equipe["J"] += 9
        g = random.randint(0, 9)  # Nombre de victoires aléatoires
        n = random.randint(0, 9 - g)  # Nombre de matchs nuls aléatoires
        p = 9 - g - n  # Nombre de défaites aléatoires
        equipe["G"] += g
        equipe["N"] += n
        equipe["P"] += p

        # Mettre à jour les points en fonction des résultats des matchs
        equipe["Pts"] += 3 * g + n  # 3 points pour une victoire, 1 point pour un match nul

        equipe["BP"] += random.randint(0, 18)  # Buts marqués aléatoirement
        equipe["BC"] += random.randint(0, 18)  # Buts encaissés aléatoirement
        equipe["Diff"] = equipe["BP"] - equipe["BC"]  # Mettre à jour la différence de buts

        # Ajouter un champ pour indiquer qu'il s'agit de données prédites
        equipe["Predicted"] = True

        # Insérer l'équipe ajustée dans la collection de prédictions de classement
        prediction_collection.insert_one(equipe)

    # Trier les équipes en fonction de leur position (ordre croissant)
    teams_stats_sorted = sorted(teams_stats_sorted, key=lambda x: x["Position"])

    # Ajuster les points des équipes en fonction de leur position
    for i, equipe in enumerate(teams_stats_sorted):
        equipe["Pts"] -= i

        # Mise à jour de l'équipe dans la collection de prédictions de classement
        prediction_collection.update_one({"Equipe": equipe["Equipe"]}, {"$set": {"Pts": equipe["Pts"]}})

    return True  # Indiquer que la prédiction a été réalisée avec succès

def predict_league_table4(db):
    # Nom de la collection pour les prédictions de classement
    prediction_collection_name = "classement_bundesliga_prediction"
    prediction_collection = db[prediction_collection_name]

    # Supprimer la collection si elle existe déjà
    if prediction_collection_name in db.list_collection_names():
        db.drop_collection(prediction_collection_name)

    # Créer une nouvelle collection pour les prédictions de classement
    db.create_collection(prediction_collection_name)

    # Récupérer les équipes depuis la collection de classement
    team_stats_collection = db["classement_bundesliga"]
    teams_stats = list(team_stats_collection.find({}, {"_id": 0}))

    # Tri des équipes en fonction des points (ordre décroissant)
    teams_stats_sorted = sorted(teams_stats, key=lambda x: x["Pts"], reverse=True)

    # Positionner les équipes en fonction de leur nombre de points
    for position, equipe in enumerate(teams_stats_sorted, start=1):
        equipe["Position"] = position

        # Ajuster aléatoirement les résultats des matchs pour maintenir la cohérence
        equipe["J"] += 9
        g = random.randint(0, 9)  # Nombre de victoires aléatoires
        n = random.randint(0, 9 - g)  # Nombre de matchs nuls aléatoires
        p = 9 - g - n  # Nombre de défaites aléatoires
        equipe["G"] += g
        equipe["N"] += n
        equipe["P"] += p

        # Mettre à jour les points en fonction des résultats des matchs
        equipe["Pts"] += 3 * g + n  # 3 points pour une victoire, 1 point pour un match nul

        equipe["BP"] += random.randint(0, 18)  # Buts marqués aléatoirement
        equipe["BC"] += random.randint(0, 18)  # Buts encaissés aléatoirement
        equipe["Diff"] = equipe["BP"] - equipe["BC"]  # Mettre à jour la différence de buts

        # Ajouter un champ pour indiquer qu'il s'agit de données prédites
        equipe["Predicted"] = True

        # Insérer l'équipe ajustée dans la collection de prédictions de classement
        prediction_collection.insert_one(equipe)

    # Trier les équipes en fonction de leur position (ordre croissant)
    teams_stats_sorted = sorted(teams_stats_sorted, key=lambda x: x["Position"])

    # Ajuster les points des équipes en fonction de leur position
    for i, equipe in enumerate(teams_stats_sorted):
        equipe["Pts"] -= i

        # Mise à jour de l'équipe dans la collection de prédictions de classement
        prediction_collection.update_one({"Équipe": equipe["Équipe"]}, {"$set": {"Pts": equipe["Pts"]}})

    return True  # Indiquer que la prédiction a été réalisée avec succès

def predict_league_table5(db):
    # Nom de la collection pour les prédictions de classement
    prediction_collection_name = "classement_PL_prediction"
    prediction_collection = db[prediction_collection_name]

    # Supprimer la collection si elle existe déjà
    if prediction_collection_name in db.list_collection_names():
        db.drop_collection(prediction_collection_name)

    # Créer une nouvelle collection pour les prédictions de classement
    db.create_collection(prediction_collection_name)

    # Récupérer les équipes depuis la collection de classement
    team_stats_collection = db["classement_PL"]
    teams_stats = list(team_stats_collection.find({}, {"_id": 0}))

    # Tri des équipes en fonction des points (ordre décroissant)
    teams_stats_sorted = sorted(teams_stats, key=lambda x: x["Pts"], reverse=True)

    # Positionner les équipes en fonction de leur nombre de points
    for position, equipe in enumerate(teams_stats_sorted, start=1):
        equipe["Position"] = position

        # Ajuster aléatoirement les résultats des matchs pour maintenir la cohérence
        equipe["J"] += 9
        g = random.randint(0, 9)  # Nombre de victoires aléatoires
        n = random.randint(0, 9 - g)  # Nombre de matchs nuls aléatoires
        p = 9 - g - n  # Nombre de défaites aléatoires
        equipe["G"] += g
        equipe["N"] += n
        equipe["P"] += p

        # Mettre à jour les points en fonction des résultats des matchs
        equipe["Pts"] += 3 * g + n  # 3 points pour une victoire, 1 point pour un match nul

        equipe["BP"] += random.randint(0, 18)  # Buts marqués aléatoirement
        equipe["BC"] += random.randint(0, 18)  # Buts encaissés aléatoirement
        equipe["Diff"] = equipe["BP"] - equipe["BC"]  # Mettre à jour la différence de buts

        # Ajouter un champ pour indiquer qu'il s'agit de données prédites
        equipe["Predicted"] = True

        # Insérer l'équipe ajustée dans la collection de prédictions de classement
        prediction_collection.insert_one(equipe)

    # Trier les équipes en fonction de leur position (ordre croissant)
    teams_stats_sorted = sorted(teams_stats_sorted, key=lambda x: x["Position"])

    # Ajuster les points des équipes en fonction de leur position
    for i, equipe in enumerate(teams_stats_sorted):
        equipe["Pts"] -= i

        # Mise à jour de l'équipe dans la collection de prédictions de classement
        prediction_collection.update_one({"Equipe": equipe["Equipe"]}, {"$set": {"Pts": equipe["Pts"]}})

    return True  # Indiquer que la prédiction a été réalisée avec succès

    
def calculate_statistics_from_past_matches(equipe, db):
    # Récupérer les matchs passés jusqu'à la journée 26
    past_matches = db["matchs_L1"].find({"Journée": {"$lt": "27"}, "$or": [{"Equipe_Domicile": equipe["Equipe"]}, {"Equipe_Extérieur": equipe["Equipe"]}], "Score": {"$nin": ["-", ""]}})
    
    # Initialiser les variables pour les statistiques
    wins = draws = losses = goals_scored = goals_conceded = 0

    # Parcourir les matchs passés
    for match in past_matches:
        home_goals, away_goals = map(int, match["Score"].split("-"))
        home_team = match["Equipe_Domicile"]
        away_team = match["Equipe_Extérieur"]

        # Mettre à jour les statistiques de l'équipe
        if equipe["Equipe"] == home_team:
            if home_goals > away_goals:
                wins += 1
            elif home_goals == away_goals:
                draws += 1
            else:
                losses += 1
            goals_scored += home_goals
            goals_conceded += away_goals
        else:
            if away_goals > home_goals:
                wins += 1
            elif away_goals == home_goals:
                draws += 1
            else:
                losses += 1
            goals_scored += away_goals
            goals_conceded += home_goals

    # Mettre à jour les statistiques de l'équipe
    equipe["G"] += wins + draws + losses
    equipe["N"] += draws
    equipe["P"] += losses
    equipe["BP"] += goals_scored
    equipe["BC"] += goals_conceded
    equipe["Diff"] += goals_scored - goals_conceded


    
def calculate_statistics_from_past_matches2(equipe, db):
    # Récupérer les matchs passés jusqu'à la journée 26
    past_matches = db["matchs_Liga"].find({"Journée": {"$lt": "30"}, "$or": [{"Equipe_Domicile": equipe["Equipe"]}, {"Equipe_Extérieur": equipe["Equipe"]}], "Score": {"$nin": ["-", ""]}})
    
    # Initialiser les variables pour les statistiques
    wins = draws = losses = goals_scored = goals_conceded = 0

    # Parcourir les matchs passés
    for match in past_matches:
        home_goals, away_goals = map(int, match["Score"].split("-"))
        home_team = match["Equipe_Domicile"]
        away_team = match["Equipe_Extérieur"]

        # Mettre à jour les statistiques de l'équipe
        if equipe["Equipe"] == home_team:
            if home_goals > away_goals:
                wins += 1
            elif home_goals == away_goals:
                draws += 1
            else:
                losses += 1
            goals_scored += home_goals
            goals_conceded += away_goals
        else:
            if away_goals > home_goals:
                wins += 1
            elif away_goals == home_goals:
                draws += 1
            else:
                losses += 1
            goals_scored += away_goals
            goals_conceded += home_goals

    # Mettre à jour les statistiques de l'équipe
    equipe["G"] += wins + draws + losses
    equipe["N"] += draws
    equipe["P"] += losses
    equipe["BP"] += goals_scored
    equipe["BC"] += goals_conceded
    equipe["Diff"] += goals_scored - goals_conceded

def calculate_statistics_from_past_matches3(equipe, db):
    # Récupérer les matchs passés jusqu'à la journée 26
    past_matches = db["matchs_SerieA"].find({"Journée": {"$lt": "30"}, "$or": [{"Equipe_Domicile": equipe["Equipe"]}, {"Equipe_Extérieur": equipe["Equipe"]}], "Score": {"$nin": ["-", ""]}})
    
    # Initialiser les variables pour les statistiques
    wins = draws = losses = goals_scored = goals_conceded = 0

    # Parcourir les matchs passés
    for match in past_matches:
        home_goals, away_goals = map(int, match["Score"].split("-"))
        home_team = match["Equipe_Domicile"]
        away_team = match["Equipe_Extérieur"]

        # Mettre à jour les statistiques de l'équipe
        if equipe["Equipe"] == home_team:
            if home_goals > away_goals:
                wins += 1
            elif home_goals == away_goals:
                draws += 1
            else:
                losses += 1
            goals_scored += home_goals
            goals_conceded += away_goals
        else:
            if away_goals > home_goals:
                wins += 1
            elif away_goals == home_goals:
                draws += 1
            else:
                losses += 1
            goals_scored += away_goals
            goals_conceded += home_goals

    # Mettre à jour les statistiques de l'équipe
    equipe["G"] += wins + draws + losses
    equipe["N"] += draws
    equipe["P"] += losses
    equipe["BP"] += goals_scored
    equipe["BC"] += goals_conceded
    equipe["Diff"] += goals_scored - goals_conceded


def calculate_statistics_from_past_matches4(equipe, db):
    # Récupérer les matchs passés jusqu'à la journée 26
    past_matches = db["matchs_bundesliga"].find({"Journée": {"$lt": "27"}, "$or": [{"Equipe_Domicile": equipe["Equipe"]}, {"Equipe_Extérieur": equipe["Equipe"]}], "Score": {"$nin": ["-", ""]}})
    
    # Initialiser les variables pour les statistiques
    wins = draws = losses = goals_scored = goals_conceded = 0

    # Parcourir les matchs passés
    for match in past_matches:
        home_goals, away_goals = map(int, match["Score"].split("-"))
        home_team = match["Equipe_Domicile"]
        away_team = match["Equipe_Extérieur"]

        # Mettre à jour les statistiques de l'équipe
        if equipe["Equipe"] == home_team:
            if home_goals > away_goals:
                wins += 1
            elif home_goals == away_goals:
                draws += 1
            else:
                losses += 1
            goals_scored += home_goals
            goals_conceded += away_goals
        else:
            if away_goals > home_goals:
                wins += 1
            elif away_goals == home_goals:
                draws += 1
            else:
                losses += 1
            goals_scored += away_goals
            goals_conceded += home_goals

    # Mettre à jour les statistiques de l'équipe
    equipe["G"] += wins + draws + losses
    equipe["N"] += draws
    equipe["P"] += losses
    equipe["BP"] += goals_scored
    equipe["BC"] += goals_conceded
    equipe["Diff"] += goals_scored - goals_conceded

def calculate_statistics_from_past_matches5(equipe, db):
    # Récupérer les matchs passés jusqu'à la journée 26
    past_matches = db["matchs_PL"].find({"Journée": {"$lt": "27"}, "$or": [{"Equipe_Domicile": equipe["Equipe"]}, {"Equipe_Extérieur": equipe["Equipe"]}], "Score": {"$nin": ["-", ""]}})
    
    # Initialiser les variables pour les statistiques
    wins = draws = losses = goals_scored = goals_conceded = 0

    # Parcourir les matchs passés
    for match in past_matches:
        home_goals, away_goals = map(int, match["Score"].split("-"))
        home_team = match["Equipe_Domicile"]
        away_team = match["Equipe_Extérieur"]

        # Mettre à jour les statistiques de l'équipe
        if equipe["Equipe"] == home_team:
            if home_goals > away_goals:
                wins += 1
            elif home_goals == away_goals:
                draws += 1
            else:
                losses += 1
            goals_scored += home_goals
            goals_conceded += away_goals
        else:
            if away_goals > home_goals:
                wins += 1
            elif away_goals == home_goals:
                draws += 1
            else:
                losses += 1
            goals_scored += away_goals
            goals_conceded += home_goals

    # Mettre à jour les statistiques de l'équipe
    equipe["G"] += wins + draws + losses
    equipe["N"] += draws
    equipe["P"] += losses
    equipe["BP"] += goals_scored
    equipe["BC"] += goals_conceded
    equipe["Diff"] += goals_scored - goals_conceded


def calculate_default_goal_probability(db):
    total_goals = 0
    total_matches = 0

    # Parcourir les matchs passés dans la collection matchs_L1 pour la saison actuelle
    past_matches = db["matchs_L1"].find({"Journée": {"$lte": "26"}, "Score": {"$nin": ["-", ""]}})
    for match in past_matches:
        home_goals, away_goals = map(int, match["Score"].split("-"))
        total_goals += home_goals + away_goals
        total_matches += 1

    # Calculer la moyenne des buts marqués par match
    if total_matches > 0:
        average_goals_per_match = total_goals / total_matches
        return average_goals_per_match / 90  # Convertir en probabilité par minute
    else:
        return 0.0  # Aucun match n'a été joué, donc probabilité par défaut est 0


def calculate_max_goals_possible(remaining_rounds, db):
    total_possible_goals = 0

    # Parcourir les matchs restants dans la collection matchs_L1 pour la saison actuelle
    for _ in range(remaining_rounds):
        total_possible_goals += calculate_default_goal_probability(db) * 90  # Nombre moyen de buts par match * 90 minutes

    return total_possible_goals


def simulate_remaining_matches(teams_stats_dict, db, current_round, remaining_rounds):
    # Récupérer les équipes et leurs positions actuelles
    teams_positions = {team: stats["Position"] for team, stats in teams_stats_dict.items()}

    # Parcourir les matchs restants à partir de la journée actuelle depuis la collection matchs_L1
    remaining_matches = db["matchs_L1"].find({"Journée": {"$gt": str(current_round)}, "Score": {"$in": ["-", ""]}})
    for match in remaining_matches:
        # Simulation des matchs restants
        simulate_match(match, teams_positions, teams_stats_dict)


def simulate_match(match, teams_positions, teams_stats_dict):
    home_team = match["Equipe_Domicile"]
    away_team = match["Equipe_Extérieur"]

    home_position = teams_positions[home_team]
    away_position = teams_positions[away_team]

    # Simulation du score en fonction des positions des équipes
    if home_position <= 5 and away_position > 15:
        home_goals = 2
        away_goals = 0
    elif away_position <= 5 and home_position > 15:
        home_goals = 0
        away_goals = 2
    else:
        home_goals, away_goals = simulate_goals(home_position, away_position)

    # Mise à jour des statistiques des équipes en fonction du résultat simulé
    update_team_stats(teams_stats_dict, home_team, away_team, home_goals, away_goals)


def simulate_goals(home_position, away_position):
    # Simulation du nombre de buts en fonction des positions des équipes
    if abs(home_position - away_position) <= 2:
        # Match nul
        return random.randint(0, 2), random.randint(0, 2)
    elif home_position < away_position:
        # Victoire à domicile
        return random.randint(1, 3), random.randint(0, 2)
    else:
        # Victoire à l'extérieur
        return random.randint(0, 2), random.randint(1, 3)


def update_team_stats(teams_stats_dict, home_team, away_team, home_goals, away_goals):
    # Mise à jour des statistiques des équipes en fonction du résultat du match
    for team, goals_scored, goals_conceded in [(home_team, home_goals, away_goals), (away_team, away_goals, home_goals)]:
        teams_stats_dict[team]["G"] += 1
        teams_stats_dict[team]["BP"] += goals_scored
        teams_stats_dict[team]["BC"] += goals_conceded

        if goals_scored > goals_conceded:
            teams_stats_dict[team]["Pts"] += 3
            teams_stats_dict[team]["V"] += 1
        elif goals_scored == goals_conceded:
            teams_stats_dict[team]["Pts"] += 1
            teams_stats_dict[team]["N"] += 1
        else:
            teams_stats_dict[team]["D"] += 1


def fetch_predictions_from_database(db):
    # Récupérer les prédictions de classement depuis la base de données
    prediction_collection_name = "classement_L1_prediction"
    prediction_collection = db[prediction_collection_name]
    predictions = list(prediction_collection.find({"Predicted": True}, {"_id": 0}))
    return predictions

def fetch_predictions_from_database2(db):
    # Récupérer les prédictions de classement depuis la base de données
    prediction_collection_name = "classement_Liga_prediction"
    prediction_collection = db[prediction_collection_name]
    predictions = list(prediction_collection.find({"Predicted": True}, {"_id": 0}))
    return predictions

def fetch_predictions_from_database3(db):
    # Récupérer les prédictions de classement depuis la base de données
    prediction_collection_name = "classement_SerieA_prediction"
    prediction_collection = db[prediction_collection_name]
    predictions = list(prediction_collection.find({"Predicted": True}, {"_id": 0}))
    return predictions

def fetch_predictions_from_database4(db):
    # Récupérer les prédictions de classement depuis la base de données
    prediction_collection_name = "classement_bundesliga_prediction"
    prediction_collection = db[prediction_collection_name]
    predictions = list(prediction_collection.find({"Predicted": True}, {"_id": 0}))
    return predictions

def fetch_predictions_from_database5(db):
    # Récupérer les prédictions de classement depuis la base de données
    prediction_collection_name = "classement_PL_prediction"
    prediction_collection = db[prediction_collection_name]
    predictions = list(prediction_collection.find({"Predicted": True}, {"_id": 0}))
    return predictions

import pandas as pd
from pymongo import MongoClient
client = MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
db = client['SoccerStats']  # Nom de votre base de données

def get_match_data_from_database(journee):
    try:
        # Connexion à MongoDB
        client = MongoClient("mongodb://127.0.0.1:27017")
        db = client["SoccerStats"]  # Nom de votre base de données
        
        # Nom de la collection contenant les matchs
        matchs_collection = db["matchs_L1"]
        
        # Récupérer les données spécifiques du match en fonction de la journée
        match_data = matchs_collection.find_one({"Journée": journee})
        
        # Concaténer les équipes domicile et extérieur pour obtenir le nom de l'équipe
        equipe = match_data.get("Equipe_Domicile") + " vs " + match_data.get("Equipe_Extérieur")
        
        # Ajouter le nom de l'équipe au dictionnaire match_data
        match_data["Équipe"] = equipe
        
        return match_data  # Renvoie les données du match sous forme de dictionnaire ou None si non trouvé
    except Exception as e:
        print("Error fetching match data:", e)
        return None


def preprocess_match_stats(match_stats):
    # Prétraitement des statistiques du match
    
    # Prétraitement spécifique à chaque type de statistiques
    preprocessed_stats = {
        'defensive': preprocess_defensive_stats(match_stats),
        'offensive': preprocess_offensive_stats(match_stats),
        'general': preprocess_general_stats(match_stats)
    }
    
    return preprocessed_stats

def preprocess_defensive_stats(match_stats):
    # Prétraitement des statistiques défensives du match
    columns_defensive = ['Tirs pm', 'Tacles pm', 'Interceptions pm', 'Fautes pm', 'Hors-jeux pm']
    return match_stats[columns_defensive].fillna(match_stats.mean())

def preprocess_offensive_stats(match_stats):
    # Prétraitement des statistiques offensives du match
    columns_offensive = ['Tirs pm', 'Tirs CA pm', 'Dribbles pm', 'Fautes subies pm']
    return match_stats[columns_offensive].fillna(match_stats.mean())

def preprocess_general_stats(match_stats):
    # Prétraitement des statistiques générales du match
    columns_general = ['Buts', 'Tirs pm', 'Possession%', 'PassesRéussies%', 'AériensGagnés']
    match_stats['Possession%'] = match_stats['Possession%'].str.rstrip('%').astype('float') / 100
    match_stats['PassesRéussies%'] = match_stats['PassesRéussies%'].str.rstrip('%').astype('float') / 100
    return match_stats[columns_general].fillna(match_stats.mean())

def get_season_stats(season):
    # Récupérer les statistiques de la saison spécifiée depuis la base de données
    stats_collection = db[f'Stats_All_Team_General_{season}_L1']
    season_stats = pd.DataFrame(list(stats_collection.find()))
    return season_stats

def get_defensive_stats(season):
    # Récupérer les statistiques défensives de la saison spécifiée depuis la base de données
    stats_collection = db[f'Stats_All_Team_def_{season}_L1']
    defensive_stats = pd.DataFrame(list(stats_collection.find()))
    return defensive_stats

def get_offensive_stats(season):
    # Récupérer les statistiques offensives de la saison spécifiée depuis la base de données
    stats_collection = db[f'Stats_All_Team_off_{season}_L1']
    offensive_stats = pd.DataFrame(list(stats_collection.find()))
    return offensive_stats

def get_match_stats(match_id, season):
    # Récupérer les statistiques du match depuis la base de données en fonction de l'ID du match et de la saison
    match_stats_collection = db[f'matchs_L1_{season}']  # Nom de la collection contenant les statistiques des matchs de la saison spécifiée
    match_stats = match_stats_collection.find_one({'match_id': match_id})
    return match_stats

import joblib

def make_match_prediction(preprocessed_stats):
    # Charger les modèles préalablement entraînés
    model_defensive = joblib.load('prediction_model_defensive.pkl')
    model_offensive = joblib.load('prediction_model_offensive.pkl')
    model_general = joblib.load('prediction_model_general.pkl')

    # Faire des prédictions pour les statistiques défensives, offensives et générales
    defensive_prediction = model_defensive.predict(preprocessed_stats['defensive'].values.reshape(1, -1))
    offensive_prediction = model_offensive.predict(preprocessed_stats['offensive'].values.reshape(1, -1))
    general_prediction = model_general.predict(preprocessed_stats['general'].values.reshape(1, -1))

    # Créer un dictionnaire de prédictions
    match_prediction = {
        'defensive_prediction': defensive_prediction[0],
        'offensive_prediction': offensive_prediction[0],
        'general_prediction': general_prediction[0]
    }

    return match_prediction

# Fonction pour calculer les moyennes des statistiques sur l'historique des matchs d'une équipe
def calculer_moyennes_stats(db, equipe):
    stats_moyennes = {}

    matchs_historique = []
    for collection_name in ["matchs_L1", "matchs_L1_20-21", "matchs_L1_21-22", "matchs_L1_22-23"]:
        collection = db[collection_name]
        matchs_historique.extend(collection.find({"$or": [{"Equipe_Domicile": equipe}, {"Equipe_Extérieur": equipe}]}, {"_id": 0}))

    total_matchs = len(matchs_historique)
    if total_matchs > 0:
        for match in matchs_historique:
            if match["Equipe_Domicile"] == equipe:
                stats_equipe = match["Stats_Domicile"]
            else:
                stats_equipe = match["Stats_Exterieur"]

            for key, value in stats_equipe.items():
                stats_moyennes[key] = stats_moyennes.get(key, 0) + value

        for key, value in stats_moyennes.items():
            stats_moyennes[key] = value / total_matchs

    return stats_moyennes

# Fonction pour prédire les statistiques d'un match entre deux équipes
def predire_stats_match(stats_moyennes_dom, stats_moyennes_ext):
    stats_predites = {}

    # Exemple simple : juste utiliser les moyennes des statistiques des deux équipes
    for key in stats_moyennes_dom:
        stats_predites[key] = (stats_moyennes_dom[key] + stats_moyennes_ext[key]) / 2

    return stats_predites

# Fonction pour ajuster les prévisions en fonction des performances récentes (facultatif)
def ajuster_previsions(stats_predites, performances_recentes_dom, performances_recentes_ext):
    # Exemple simple : ajuster les prévisions en fonction des performances récentes
    for key in stats_predites:
        stats_predites[key] = stats_predites[key] * 0.8 + performances_recentes_dom[key] * 0.1 + performances_recentes_ext[key] * 0.1

    return stats_predites

#Fonction pour Ligue 1 --> faire apparaitre les logos dans les matchs par journée
def get_matchs_by_journee_with_logos(journee):
    matchs_journee = get_matchs_by_journee(journee)
    classement, _, _, logos_ligue_1 = get_data_from_mongodb()
    for match in matchs_journee:
        match['Logo_Domicile'] = logos_ligue_1.get(match['Equipe_Domicile'])
        match['Logo_Exterieur'] = logos_ligue_1.get(match['Equipe_Extérieur'])
    return matchs_journee, logos_ligue_1


#Fonction pour Liga --> faire apparaitre les logos dans les matchs par journée
def get_matchs_by_journee_with_logos_liga(journee):
    matchs_journee = get_matchs_by_journee2(journee)
    classement, _, _, logos_equipe_liga = get_data_from_mongodb2()
    for match in matchs_journee:
        match['Logo_Domicile'] = logos_equipe_liga.get(match['Equipe_Domicile'])
        match['Logo_Exterieur'] = logos_equipe_liga.get(match['Equipe_Extérieur'])

    return matchs_journee, logos_equipe_liga

#Fonction pour serieA  --> faire apparaitre les logos dans les matchs par journée
def get_matchs_by_journee_with_logos_serieA(journee):
    matchs_journee = get_matchs_by_journee3(journee)
    classement, _, _, logos_equipe_serieA = get_data_from_mongodb3()
    for match in matchs_journee:
        match['Logo_Domicile'] = logos_equipe_serieA.get(match['Equipe_Domicile'])
        match['Logo_Exterieur'] = logos_equipe_serieA.get(match['Equipe_Extérieur'])

    return matchs_journee, logos_equipe_serieA

#Fonction pour bundes  --> faire apparaitre les logos dans les matchs par journée
def get_matchs_by_journee_with_logos_bundes(journee):
    matchs_journee = get_matchs_by_journee4(journee)
    classement, _, _, logos_equipe_bundes = get_data_from_mongodb4()
    for match in matchs_journee:
        match['Logo_Domicile'] = logos_equipe_bundes.get(match['Equipe_Domicile'])
        match['Logo_Exterieur'] = logos_equipe_bundes.get(match['Equipe_Extérieur'])

    return matchs_journee, logos_equipe_bundes

#Fonction pour PL  --> faire apparaitre les logos dans les matchs par journée
def get_matchs_by_journee_with_logos_PL(journee):
    matchs_journee = get_matchs_by_journee5(journee)
    classement, _, _, logos_equipe_PL = get_data_from_mongodb5()
    for match in matchs_journee:
        match['Logo_Domicile'] = logos_equipe_PL.get(match['Equipe_Domicile'])
        match['Logo_Exterieur'] = logos_equipe_PL.get(match['Equipe_Extérieur'])

    return matchs_journee, logos_equipe_PL

@app.route('/classement_ligue1')
def classement_ligue1():
    classement, matchs, buteurs = get_data_from_mongodb()
    logos_ligue_1 = get_logos_ligue_1()
    return render_template('matches.html', classement=classement, matchs=matchs, buteurs=buteurs, logos_ligue_1=logos_ligue_1)


# Route pour les matchs de la Ligue 1
@app.route('/matches_ligue1')
def matches_ligue1():
    classement, matchs, buteurs = get_data_from_mongodb()
    logos_ligue_1 = get_logos_ligue_1()
    return render_template('matches.html', classement=classement, matchs=matchs, buteurs=buteurs, logos_ligue_1=logos_ligue_1)

@app.route('/classement_final')
def classement_final():
    # Obtenez la connexion à la base de données
    db = get_database_connection()

    # La fonction predict_league_table est appelée ici pour mettre à jour les prédictions
    predict_league_table(db)  # Passez la connexion à la base de données

    # Récupérer les prédictions de classement depuis la base de données
    predictions = fetch_predictions_from_database(db)  # Passez la connexion à la base de données

    print(predictions)

    # Rendre le template HTML avec le classement final
    return render_template('classement.html', classement_final=predictions)

@app.route('/predict_ranking', methods=['POST'])
def predict_ranking():
    # Obtenez la connexion à la base de données
    db = get_database_connection()

    # La fonction predict_league_table est appelée ici pour mettre à jour les prédictions
    predict_league_table(db)

    # Récupérer les prédictions de classement depuis la base de données en utilisant la fonction appropriée
    predictions = fetch_predictions_from_database(db)

    # Récupérer les données actuelles du classement depuis la base de données
    current_rankings = db["classement_L1"].find({}, {"_id": 0})

    # Limite maximale de points qu'une équipe peut obtenir
    max_points_allowed = 83

    for current, prediction in zip(current_rankings, predictions):
        # Calculer le maximum de points que l'équipe peut obtenir
        max_pts = min(current["Pts"] + 21, max_points_allowed)

        # Ajuster les prédictions si nécessaire
        if prediction["Pts"] < current["Pts"]:
            # Si les prédictions de points sont inférieures aux points actuels, les corriger pour ne pas perdre de points
            prediction["Pts"] = current["Pts"]

        prediction["Pts"] = min(prediction["Pts"], max_pts)
        prediction["BP"] = max(prediction["BP"], current["BP"])
        prediction["BC"] = max(prediction["BC"], current["BC"])

    # Assurez-vous que les prédictions sont sérialisables en JSON avant de les retourner
    serialized_predictions = [
        {
            "Equipe": prediction["Equipe"],
            "Position": prediction["Position"],
            "Pts": prediction["Pts"],
            "Diff": prediction["BP"] - prediction["BC"],  # Calculer la différence comme BP - BC
            "BP": prediction["BP"],
            "BC": prediction["BC"]
        }
        for prediction in predictions
    ]

    return jsonify(serialized_predictions)



@app.route('/classement_final2')
def classement_final2():
    # Obtenez la connexion à la base de données
    db = get_database_connection()

    # La fonction predict_league_table est appelée ici pour mettre à jour les prédictions
    predict_league_table2(db)  # Passez la connexion à la base de données

    # Récupérer les prédictions de classement depuis la base de données
    predictions = fetch_predictions_from_database2(db)  # Passez la connexion à la base de données

    print(predictions)

    # Rendre le template HTML avec le classement final
    return render_template('classement2.html', classement_final=predictions)

@app.route('/predict_ranking2', methods=['POST'])
def predict_ranking2():
    # Obtenez la connexion à la base de données
    db = get_database_connection()

    # La fonction predict_league_table est appelée ici pour mettre à jour les prédictions
    predict_league_table2(db)

    # Récupérer les prédictions de classement depuis la base de données en utilisant la fonction appropriée
    predictions = fetch_predictions_from_database2(db)

    # Récupérer les données actuelles du classement depuis la base de données
    current_rankings = db["classement_Liga"].find({}, {"_id": 0})

    # Limite maximale de points qu'une équipe peut obtenir
    max_points_allowed = 99

    for current, prediction in zip(current_rankings, predictions):
        # Calculer le maximum de points que l'équipe peut obtenir
        max_pts = min(current["Pts"] + 24, max_points_allowed)

        # Ajuster les prédictions si nécessaire
        if prediction["Pts"] < current["Pts"]:
            # Si les prédictions de points sont inférieures aux points actuels, les corriger pour ne pas perdre de points
            prediction["Pts"] = current["Pts"]

        prediction["Pts"] = min(prediction["Pts"], max_pts)
        prediction["BP"] = max(prediction["BP"], current["BP"])
        prediction["BC"] = max(prediction["BC"], current["BC"])

    # Assurez-vous que les prédictions sont sérialisables en JSON avant de les retourner
    serialized_predictions = [
        {
            "Équipe": prediction["Équipe"],
            "Position": prediction["Position"],
            "Pts": prediction["Pts"],
            "Diff": prediction["BP"] - prediction["BC"],  # Calculer la différence comme BP - BC
            "BP": prediction["BP"],
            "BC": prediction["BC"]
        }
        for prediction in predictions
    ]

    return jsonify(serialized_predictions)



@app.route('/classement_final3')
def classement_final3():
    # Obtenez la connexion à la base de données
    db = get_database_connection()

    # La fonction predict_league_table est appelée ici pour mettre à jour les prédictions
    predict_league_table3(db)  # Passez la connexion à la base de données

    # Récupérer les prédictions de classement depuis la base de données
    predictions = fetch_predictions_from_database3(db)  # Passez la connexion à la base de données

    print(predictions)

    # Rendre le template HTML avec le classement final
    return render_template('classement3.html', classement_final=predictions)

@app.route('/predict_ranking3', methods=['POST'])
def predict_ranking3():
    # Obtenez la connexion à la base de données
    db = get_database_connection()

    # La fonction predict_league_table est appelée ici pour mettre à jour les prédictions
    predict_league_table3(db)

    # Récupérer les prédictions de classement depuis la base de données en utilisant la fonction appropriée
    predictions = fetch_predictions_from_database3(db)

    # Récupérer les données actuelles du classement depuis la base de données
    current_rankings = db["classement_SerieA"].find({}, {"_id": 0})

    # Limite maximale de points qu'une équipe peut obtenir
    max_points_allowed = 103

    for current, prediction in zip(current_rankings, predictions):
        # Calculer le maximum de points que l'équipe peut obtenir
        max_pts = min(current["Pts"] + 24, max_points_allowed)

        # Ajuster les prédictions si nécessaire
        if prediction["Pts"] < current["Pts"]:
            # Si les prédictions de points sont inférieures aux points actuels, les corriger pour ne pas perdre de points
            prediction["Pts"] = current["Pts"]

        prediction["Pts"] = min(prediction["Pts"], max_pts)
        prediction["BP"] = max(prediction["BP"], current["BP"])
        prediction["BC"] = max(prediction["BC"], current["BC"])

    # Assurez-vous que les prédictions sont sérialisables en JSON avant de les retourner
    serialized_predictions = [
        {
            "Equipe": prediction["Equipe"],
            "Position": prediction["Position"],
            "Pts": prediction["Pts"],
            "Diff": prediction["BP"] - prediction["BC"],  # Calculer la différence comme BP - BC
            "BP": prediction["BP"],
            "BC": prediction["BC"]
        }
        for prediction in predictions
    ]

    return jsonify(serialized_predictions)



@app.route('/classement_final4')
def classement_final4():
    # Obtenez la connexion à la base de données
    db = get_database_connection()

    # La fonction predict_league_table est appelée ici pour mettre à jour les prédictions
    predict_league_table4(db)  # Passez la connexion à la base de données

    # Récupérer les prédictions de classement depuis la base de données
    predictions = fetch_predictions_from_database4(db)  # Passez la connexion à la base de données

    print(predictions)

    # Rendre le template HTML avec le classement final
    return render_template('classement4.html', classement_final=predictions)

@app.route('/predict_ranking4', methods=['POST'])
def predict_ranking4():
    # Obtenez la connexion à la base de données
    db = get_database_connection()

    # La fonction predict_league_table est appelée ici pour mettre à jour les prédictions
    predict_league_table4(db)

    # Récupérer les prédictions de classement depuis la base de données en utilisant la fonction appropriée
    predictions = fetch_predictions_from_database4(db)

    # Récupérer les données actuelles du classement depuis la base de données
    current_rankings = db["classement_bundesliga"].find({}, {"_id": 0})

    # Limite maximale de points qu'une équipe peut obtenir
    max_points_allowed = 94

    for current, prediction in zip(current_rankings, predictions):
        # Calculer le maximum de points que l'équipe peut obtenir
        max_pts = min(current["Pts"] + 21, max_points_allowed)

        # Ajuster les prédictions si nécessaire
        if prediction["Pts"] < current["Pts"]:
            # Si les prédictions de points sont inférieures aux points actuels, les corriger pour ne pas perdre de points
            prediction["Pts"] = current["Pts"]

        prediction["Pts"] = min(prediction["Pts"], max_pts)
        prediction["BP"] = max(prediction["BP"], current["BP"])
        prediction["BC"] = max(prediction["BC"], current["BC"])

    # Assurez-vous que les prédictions sont sérialisables en JSON avant de les retourner
    serialized_predictions = [
        {
            "Équipe": prediction["Équipe"],
            "Position": prediction["Position"],
            "Pts": prediction["Pts"],
            "Diff": prediction["BP"] - prediction["BC"],  # Calculer la différence comme BP - BC
            "BP": prediction["BP"],
            "BC": prediction["BC"]
        }
        for prediction in predictions
    ]

    return jsonify(serialized_predictions)








@app.route('/classement_final5')
def classement_final5():
    # Obtenez la connexion à la base de données
    db = get_database_connection()

    # La fonction predict_league_table est appelée ici pour mettre à jour les prédictions
    predict_league_table5(db)  # Passez la connexion à la base de données

    # Récupérer les prédictions de classement depuis la base de données
    predictions = fetch_predictions_from_database5(db)  # Passez la connexion à la base de données

    print(predictions)

    # Rendre le template HTML avec le classement final
    return render_template('classement5.html', classement_final=predictions)

@app.route('/predict_ranking5', methods=['POST'])
def predict_ranking5():
    # Obtenez la connexion à la base de données
    db = get_database_connection()

    # La fonction predict_league_table est appelée ici pour mettre à jour les prédictions
    predict_league_table5(db)

    # Récupérer les prédictions de classement depuis la base de données en utilisant la fonction appropriée
    predictions = fetch_predictions_from_database5(db)

    # Récupérer les données actuelles du classement depuis la base de données
    current_rankings = db["classement_PL"].find({}, {"_id": 0})

    # Limite maximale de points qu'une équipe peut obtenir
    max_points_allowed = 94

    for current, prediction in zip(current_rankings, predictions):
        # Calculer le maximum de points que l'équipe peut obtenir
        max_pts = min(current["Pts"] + 27, max_points_allowed)

        # Ajuster les prédictions si nécessaire
        if prediction["Pts"] < current["Pts"]:
            # Si les prédictions de points sont inférieures aux points actuels, les corriger pour ne pas perdre de points
            prediction["Pts"] = current["Pts"]

        prediction["Pts"] = min(prediction["Pts"], max_pts)
        prediction["BP"] = max(prediction["BP"], current["BP"])
        prediction["BC"] = max(prediction["BC"], current["BC"])

    # Assurez-vous que les prédictions sont sérialisables en JSON avant de les retourner
    serialized_predictions = [
        {
            "Equipe": prediction["Equipe"],
            "Position": prediction["Position"],
            "Pts": prediction["Pts"],
            "Diff": prediction["BP"] - prediction["BC"],  # Calculer la différence comme BP - BC
            "BP": prediction["BP"],
            "BC": prediction["BC"]
        }
        for prediction in predictions
    ]

    return jsonify(serialized_predictions)


# Route pour prédire les meilleurs buteurs ligue 1
@app.route('/predict_top_scorers/<int:current_matchday>', methods=['POST'])
def predict_top_scorers_route(current_matchday):
    predictions = predict_top_scorers(current_matchday)
    return jsonify(predictions)

# Route pour prédire les meilleurs buteurs liga
@app.route('/predict_top_scorers2/<int:current_matchday>', methods=['POST'])
def predict_top_scorers_route2(current_matchday):
    predictions = predict_top_scorers2(current_matchday)
    return jsonify(predictions)

# Route pour prédire les meilleurs buteurs serieA
@app.route('/predict_top_scorers3/<int:current_matchday>', methods=['POST'])
def predict_top_scorers_route3(current_matchday):
    predictions = predict_top_scorers3(current_matchday)
    return jsonify(predictions)

# Route pour prédire les meilleurs buteurs bundesliga
@app.route('/predict_top_scorers4/<int:current_matchday>', methods=['POST'])
def predict_top_scorers_route4(current_matchday):
    predictions = predict_top_scorers4(current_matchday)
    return jsonify(predictions)

# Route pour prédire les meilleurs buteurs bundesliga
@app.route('/predict_top_scorers5/<int:current_matchday>', methods=['POST'])
def predict_top_scorers_route5(current_matchday):
    predictions = predict_top_scorers5(current_matchday)
    return jsonify(predictions)

# Route pour afficher les tendances des buteurs et equipe ligue 1
@app.route('/tendances_buteurs')
def tendances_buteurs():
    afficher_tendances_buteurs()
    return "Veuillez consulter la console pour les tendances des buteurs."

@app.route('/tendances_equipes')
def tendances_equipes():
    afficher_tendances_equipes()
    return "Veuillez consulter la console pour les tendances des équipes."

# Route pour afficher les tendances des buteurs et equipe liga
@app.route('/tendances_buteurs2')
def tendances_buteurs2():
    afficher_tendances_buteurs2()
    return "Veuillez consulter la console pour les tendances des buteurs."

@app.route('/tendances_equipes2')
def tendances_equipes2():
    afficher_tendances_equipes2()
    return "Veuillez consulter la console pour les tendances des équipes."

# Route pour afficher les tendances des buteurs et equipe serieA
@app.route('/tendances_buteurs3')
def tendances_buteurs3():
    afficher_tendances_buteurs3()
    return "Veuillez consulter la console pour les tendances des buteurs."

@app.route('/tendances_equipes3')
def tendances_equipes3():
    afficher_tendances_equipes3()
    return "Veuillez consulter la console pour les tendances des équipes."

# Route pour afficher les tendances des buteurs et equipe bundesliga
@app.route('/tendances_buteurs4')
def tendances_buteurs4():
    afficher_tendances_buteurs4()
    return "Veuillez consulter la console pour les tendances des buteurs."

@app.route('/tendances_equipes4')
def tendances_equipes4():
    afficher_tendances_equipes4()
    return "Veuillez consulter la console pour les tendances des équipes."

# Route pour afficher les tendances des buteurs et equipe bundesliga
@app.route('/tendances_buteurs5')
def tendances_buteurs5():
    afficher_tendances_buteurs5()
    return "Veuillez consulter la console pour les tendances des buteurs."

@app.route('/tendances_equipes5')
def tendances_equipes5():
    afficher_tendances_equipes5()
    return "Veuillez consulter la console pour les tendances des équipes."

# Route pour la page d'accueil
@app.route('/')
def index():
    return render_template('index.html')

# Route pour la page d'accueil liga
# @app.route('/liga')
# def liga():
#     return render_template('Liga.html')

# Route pour la page des matchs
@app.route('/matches')
def matches():
    classement, matchs, buteurs, logos_ligue_1 = get_data_from_mongodb()
    last_matches = get_matchs_by_journee(28)  # Récupérer les matchs de la 26e journée
    next_matches = get_matchs_by_journee(2)  # Récupérer les matchs de la 27e journée
    return render_template('matches.html', classement=classement, matchs=matchs, last_matches=last_matches, next_matches=next_matches, buteurs=buteurs, logos_ligue_1=logos_ligue_1)

# Route pour la page des matchs par journée
@app.route('/matches_by_journee', methods=['POST'])
def matches_by_journee():
    journee = request.form['journee']
    matchs_journee, logos_ligue_1 = get_matchs_by_journee_with_logos(journee)

    if not isinstance(journee, str):
        journee = str(journee)

    for match in matchs_journee:
        if '_id' in match:
            match['_id'] = str(match['_id'])

    return render_template('matches_partial.html', matchs=matchs_journee, selected_journee=journee, logos_ligue_1=logos_ligue_1)

# Route pour la génération de prévisions de matchs
@app.route('/predictions')
def predictions():
    predictions = generate_predictions()  # Générer les prévisions de matchs
    return render_template('predictions.html', predictions=predictions)  # Rendre le template avec les prévisions

# Route pour la prédiction de matchs
@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json()
    equipe_domicile = data.get('equipe_domicile')
    equipe_exterieur = data.get('equipe_exterieur')
    
    # Récupérer les prévisions depuis la fonction generate_predictions()
    predictions = generate_predictions()
    
    # Trouver la prédiction correspondante aux équipes sélectionnées
    for prediction in predictions:
        if prediction['equipe_domicile'] == equipe_domicile and prediction['equipe_exterieur'] == equipe_exterieur:
            # Formater la prédiction
            prediction_text = f"Prédiction pour le match entre {equipe_domicile} et {equipe_exterieur} :"
            prediction_text += f" Victoire domicile : {prediction['pourcentage_victoire_domicile']}%"
            prediction_text += f" | Victoire extérieur : {prediction['pourcentage_victoire_exterieur']}%"
            prediction_text += f" | Match nul : {prediction['pourcentage_match_nul']}%"
            
            return jsonify({'prediction': prediction_text})
    
    # Si aucune prédiction correspondante n'est trouvée, renvoyer un message d'erreur
    return jsonify({'prediction': "Aucune prédiction disponible pour ce match."})

@app.route("/stats_avancees/<journee>/<equipe_dom>/<equipe_ext>")
def stats_avancees(journee, equipe_dom, equipe_ext):
        # Connexion à la collection pour les statistiques offensives
        client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
        db = client["SoccerStats"]
    
        # Récupérer les données pour les statistiques offensives, défensives et générales de l'équipe domicile
        stats_dom_off = db["Stats_off_prevision_L1"].find_one({"Équipe": equipe_dom}, {"_id": 0}) or {}
        stats_dom_def = db["Stats_def_prevision_L1"].find_one({"Équipe": equipe_dom}, {"_id": 0}) or {}
        stats_dom_gen = db["Stats_gen_prevision_L1"].find_one({"Équipe": equipe_dom}, {"_id": 0}) or {}
    
        # Récupérer les données pour les statistiques offensives, défensives et générales de l'équipe extérieure
        stats_ext_off = db["Stats_off_prevision_L1"].find_one({"Équipe": equipe_ext}, {"_id": 0}) or {}
        stats_ext_def = db["Stats_def_prevision_L1"].find_one({"Équipe": equipe_ext}, {"_id": 0}) or {}
        stats_ext_gen = db["Stats_gen_prevision_L1"].find_one({"Équipe": equipe_ext}, {"_id": 0}) or {}

         # Récupérer les données du match pour la journée, l'équipe domicile et l'équipe extérieure spécifiées
        match_data = db["Resultats_Predits"].find_one({"Saison": "matchs_L1", "Journée": journee, "Equipe_Domicile": equipe_dom, "Equipe_Extérieur": equipe_ext})

        # Vérifier si les données du match existent
        if match_data:
            # Récupérer les statistiques pour les équipes domicile et extérieure
            stats_dom = match_data.get("Stats_Domicile", {})
            stats_ext = match_data.get("Stats_Extérieur", {})

                    # Supprimer la clé "Position" des statistiques
            if "Position" in stats_dom:
                del stats_dom["Position"]
            if "Position" in stats_ext:
                del stats_ext["Position"]

        # Vérifier si les données des équipes existent
        if stats_dom_off and stats_ext_off and stats_dom_def and stats_ext_def and stats_dom_gen and stats_ext_gen and stats_dom and stats_ext:
            # Retourner les données à la page HTML
            return render_template("stats_avancees.html", journee=journee, equipe_dom=equipe_dom, equipe_ext=equipe_ext,
                                   stats_domicile_off=stats_dom_off, stats_domicile_def=stats_dom_def, stats_domicile_gen=stats_dom_gen,
                                   stats_exterieur_off=stats_ext_off, stats_exterieur_def=stats_ext_def, stats_exterieur_gen=stats_ext_gen, 
                                   stats_domicile=stats_dom, stats_exterieur=stats_ext)
        else:
            return "Les données pour une ou plusieurs de ces équipes ne sont pas disponibles."
    

@app.route("/stats_avancees_liga/<journee>/<equipe_dom>/<equipe_ext>")
def stats_avancees_liga(journee, equipe_dom, equipe_ext):
        # Connexion à la collection pour les statistiques offensives
        client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
        db = client["SoccerStats"]
    
        # Récupérer les données pour les statistiques offensives, défensives et générales de l'équipe domicile
        stats_dom_off = db["Stats_off_prevision_Liga"].find_one({"Équipe": equipe_dom}, {"_id": 0}) or {}
        stats_dom_def = db["Stats_def_prevision_Liga"].find_one({"Équipe": equipe_dom}, {"_id": 0}) or {}
        stats_dom_gen = db["Stats_gen_prevision_Liga"].find_one({"Équipe": equipe_dom}, {"_id": 0}) or {}
    
        # Récupérer les données pour les statistiques offensives, défensives et générales de l'équipe extérieure
        stats_ext_off = db["Stats_off_prevision_Liga"].find_one({"Équipe": equipe_ext}, {"_id": 0}) or {}
        stats_ext_def = db["Stats_def_prevision_Liga"].find_one({"Équipe": equipe_ext}, {"_id": 0}) or {}
        stats_ext_gen = db["Stats_gen_prevision_Liga"].find_one({"Équipe": equipe_ext}, {"_id": 0}) or {}

         # Récupérer les données du match pour la journée, l'équipe domicile et l'équipe extérieure spécifiées
        match_data = db["Resultats_Predits_Liga"].find_one({"Saison": "matchs_Liga", "Journée": journee, "Equipe_Domicile": equipe_dom, "Equipe_Extérieur": equipe_ext})

        # Vérifier si les données du match existent
        if match_data:
            # Récupérer les statistiques pour les équipes domicile et extérieure
            stats_dom = match_data.get("Stats_Domicile", {})
            stats_ext = match_data.get("Stats_Extérieur", {})

                    # Supprimer la clé "Position" des statistiques
            if "Position" in stats_dom:
                del stats_dom["Position"]
            if "Position" in stats_ext:
                del stats_ext["Position"]

        # Vérifier si les données des équipes existent
        if stats_dom_off and stats_ext_off and stats_dom_def and stats_ext_def and stats_dom_gen and stats_ext_gen and stats_dom and stats_ext:
            # Retourner les données à la page HTML
            return render_template("stats_avancees_Liga.html", journee=journee, equipe_dom=equipe_dom, equipe_ext=equipe_ext,
                                   stats_domicile_off=stats_dom_off, stats_domicile_def=stats_dom_def, stats_domicile_gen=stats_dom_gen,
                                   stats_exterieur_off=stats_ext_off, stats_exterieur_def=stats_ext_def, stats_exterieur_gen=stats_ext_gen, 
                                   stats_domicile=stats_dom, stats_exterieur=stats_ext)
        else:
            return "Les données pour une ou plusieurs de ces équipes ne sont pas disponibles."
        


@app.route("/stats_avancees_serieA/<journee>/<equipe_dom>/<equipe_ext>")
def stats_avancees_serieA(journee, equipe_dom, equipe_ext):
        # Connexion à la collection pour les statistiques offensives
        client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
        db = client["SoccerStats"]
    
        # Récupérer les données pour les statistiques offensives, défensives et générales de l'équipe domicile
        stats_dom_off = db["Stats_off_prevision_serieA"].find_one({"Équipe": equipe_dom}, {"_id": 0}) or {}
        stats_dom_def = db["Stats_def_prevision_serieA"].find_one({"Équipe": equipe_dom}, {"_id": 0}) or {}
        stats_dom_gen = db["Stats_gen_prevision_serieA"].find_one({"Équipe": equipe_dom}, {"_id": 0}) or {}
    
        # Récupérer les données pour les statistiques offensives, défensives et générales de l'équipe extérieure
        stats_ext_off = db["Stats_off_prevision_serieA"].find_one({"Équipe": equipe_ext}, {"_id": 0}) or {}
        stats_ext_def = db["Stats_def_prevision_serieA"].find_one({"Équipe": equipe_ext}, {"_id": 0}) or {}
        stats_ext_gen = db["Stats_gen_prevision_serieA"].find_one({"Équipe": equipe_ext}, {"_id": 0}) or {}

         # Récupérer les données du match pour la journée, l'équipe domicile et l'équipe extérieure spécifiées
        match_data = db["Resultats_Predits_serieA"].find_one({"Saison": "matchs_serieA", "Journée": journee, "Equipe_Domicile": equipe_dom, "Equipe_Extérieur": equipe_ext})

        # Vérifier si les données du match existent
        if match_data:
            # Récupérer les statistiques pour les équipes domicile et extérieure
            stats_dom = match_data.get("Stats_Domicile", {})
            stats_ext = match_data.get("Stats_Extérieur", {})

                    # Supprimer la clé "Position" des statistiques
            if "Position" in stats_dom:
                del stats_dom["Position"]
            if "Position" in stats_ext:
                del stats_ext["Position"]

        # Vérifier si les données des équipes existent
        if stats_dom_off and stats_ext_off and stats_dom_def and stats_ext_def and stats_dom_gen and stats_ext_gen and stats_dom and stats_ext:
            # Retourner les données à la page HTML
            return render_template("stats_avancees_serieA.html", journee=journee, equipe_dom=equipe_dom, equipe_ext=equipe_ext,
                                   stats_domicile_off=stats_dom_off, stats_domicile_def=stats_dom_def, stats_domicile_gen=stats_dom_gen,
                                   stats_exterieur_off=stats_ext_off, stats_exterieur_def=stats_ext_def, stats_exterieur_gen=stats_ext_gen, 
                                   stats_domicile=stats_dom, stats_exterieur=stats_ext)
        else:
            return "Les données pour une ou plusieurs de ces équipes ne sont pas disponibles."    


@app.route("/stats_avancees_bundes/<journee>/<equipe_dom>/<equipe_ext>")
def stats_avancees_bundes(journee, equipe_dom, equipe_ext):
        # Connexion à la collection pour les statistiques offensives
        client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
        db = client["SoccerStats"]
    
        # Récupérer les données pour les statistiques offensives, défensives et générales de l'équipe domicile
        stats_dom_off = db["Stats_off_prevision_bundes"].find_one({"Équipe": equipe_dom}, {"_id": 0}) or {}
        stats_dom_def = db["Stats_def_prevision_bundes"].find_one({"Équipe": equipe_dom}, {"_id": 0}) or {}
        stats_dom_gen = db["Stats_gen_prevision_bundes"].find_one({"Équipe": equipe_dom}, {"_id": 0}) or {}
    
        # Récupérer les données pour les statistiques offensives, défensives et générales de l'équipe extérieure
        stats_ext_off = db["Stats_off_prevision_bundes"].find_one({"Équipe": equipe_ext}, {"_id": 0}) or {}
        stats_ext_def = db["Stats_def_prevision_bundes"].find_one({"Équipe": equipe_ext}, {"_id": 0}) or {}
        stats_ext_gen = db["Stats_gen_prevision_bundes"].find_one({"Équipe": equipe_ext}, {"_id": 0}) or {}

         # Récupérer les données du match pour la journée, l'équipe domicile et l'équipe extérieure spécifiées
        match_data = db["Resultats_Predits_bundes"].find_one({"Saison": "matchs_bundesliga", "Journée": journee, "Equipe_Domicile": equipe_dom, "Equipe_Extérieur": equipe_ext})

        # Vérifier si les données du match existent
        if match_data:
            # Récupérer les statistiques pour les équipes domicile et extérieure
            stats_dom = match_data.get("Stats_Domicile", {})
            stats_ext = match_data.get("Stats_Extérieur", {})

                    # Supprimer la clé "Position" des statistiques
            if "Position" in stats_dom:
                del stats_dom["Position"]
            if "Position" in stats_ext:
                del stats_ext["Position"]

        # Vérifier si les données des équipes existent
        if stats_dom_off and stats_ext_off and stats_dom_def and stats_ext_def and stats_dom_gen and stats_ext_gen and stats_dom and stats_ext:
            # Retourner les données à la page HTML
            return render_template("stats_avancees_bundes.html", journee=journee, equipe_dom=equipe_dom, equipe_ext=equipe_ext,
                                   stats_domicile_off=stats_dom_off, stats_domicile_def=stats_dom_def, stats_domicile_gen=stats_dom_gen,
                                   stats_exterieur_off=stats_ext_off, stats_exterieur_def=stats_ext_def, stats_exterieur_gen=stats_ext_gen, 
                                   stats_domicile=stats_dom, stats_exterieur=stats_ext)
        else:
            return "Les données pour une ou plusieurs de ces équipes ne sont pas disponibles."    


@app.route("/stats_avancees_PL/<journee>/<equipe_dom>/<equipe_ext>")
def stats_avancees_PL(journee, equipe_dom, equipe_ext):
        # Connexion à la collection pour les statistiques offensives
        client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
        db = client["SoccerStats"]
    
        # Récupérer les données pour les statistiques offensives, défensives et générales de l'équipe domicile
        stats_dom_off = db["Stats_off_prevision_PL"].find_one({"Équipe": equipe_dom}, {"_id": 0}) or {}
        stats_dom_def = db["Stats_def_prevision_PL"].find_one({"Équipe": equipe_dom}, {"_id": 0}) or {}
        stats_dom_gen = db["Stats_gen_prevision_PL"].find_one({"Équipe": equipe_dom}, {"_id": 0}) or {}
    
        # Récupérer les données pour les statistiques offensives, défensives et générales de l'équipe extérieure
        stats_ext_off = db["Stats_off_prevision_PL"].find_one({"Équipe": equipe_ext}, {"_id": 0}) or {}
        stats_ext_def = db["Stats_def_prevision_PL"].find_one({"Équipe": equipe_ext}, {"_id": 0}) or {}
        stats_ext_gen = db["Stats_gen_prevision_PL"].find_one({"Équipe": equipe_ext}, {"_id": 0}) or {}

         # Récupérer les données du match pour la journée, l'équipe domicile et l'équipe extérieure spécifiées
        match_data = db["Resultats_Predits_PL"].find_one({"Saison": "matchs_PL", "Journée": journee, "Equipe_Domicile": equipe_dom, "Equipe_Extérieur": equipe_ext})

        # Vérifier si les données du match existent
        if match_data:
            # Récupérer les statistiques pour les équipes domicile et extérieure
            stats_dom = match_data.get("Stats_Domicile", {})
            stats_ext = match_data.get("Stats_Extérieur", {})

                    # Supprimer la clé "Position" des statistiques
            if "Position" in stats_dom:
                del stats_dom["Position"]
            if "Position" in stats_ext:
                del stats_ext["Position"]

        # Vérifier si les données des équipes existent
        if stats_dom_off and stats_ext_off and stats_dom_def and stats_ext_def and stats_dom_gen and stats_ext_gen and stats_dom and stats_ext:
            # Retourner les données à la page HTML
            return render_template("stats_avancees_PL.html", journee=journee, equipe_dom=equipe_dom, equipe_ext=equipe_ext,
                                   stats_domicile_off=stats_dom_off, stats_domicile_def=stats_dom_def, stats_domicile_gen=stats_dom_gen,
                                   stats_exterieur_off=stats_ext_off, stats_exterieur_def=stats_ext_def, stats_exterieur_gen=stats_ext_gen, 
                                   stats_domicile=stats_dom, stats_exterieur=stats_ext)
        else:
            return "Les données pour une ou plusieurs de ces équipes ne sont pas disponibles."    



# Route pour la page d'accueil liga
@app.route('/liga')
def liga():
    classement, matchs, buteurs, logos_equipe_liga = get_data_from_mongodb2()
    last_matches = get_matchs_by_journee2(30)  # Récupérer les matchs de la 26e journée
    next_matches = get_matchs_by_journee2(31)  # Récupérer les matchs de la 27e journée
    return render_template('liga.html', classement=classement, matchs=matchs, last_matches=last_matches, next_matches=next_matches, buteurs=buteurs, logos_equipe_liga=logos_equipe_liga)

# Route pour les matchs de la liga
@app.route('/liga/matches')
def liga_matches():
    classement, matchs, buteurs, logos_equipe_liga = get_data_from_mongodb2()
    last_matches = get_matchs_by_journee2(30)  # Récupérer les matchs de la 26e journée
    next_matches = get_matchs_by_journee2(31)  # Récupérer les matchs de la 27e journée
    return render_template('liga.html', classement=classement, matchs=matchs, last_matches=last_matches, next_matches=next_matches, buteurs=buteurs, logos_equipe_liga=logos_equipe_liga)


# Route pour la page des matchs par journée de la Liga
@app.route('/liga/matches_by_journee', methods=['POST'])
def liga_matches_by_journee2():
    journee = request.form['journee']  # Récupérer la journée sélectionnée depuis le formulaire
    matchs_journee, logos_equipe_liga = get_matchs_by_journee_with_logos_liga(journee)  # Récupérer les matchs pour la journée sélectionnée
    
    # Si vous stockez la journée sous forme d'ObjectId, convertissez-la en chaîne de caractères
    if not isinstance(journee, str):
        journee = str(journee)
    
    # Convertissez les ObjectIds en chaînes de caractères si nécessaire
    for match in matchs_journee:
        if '_id' in match:
            match['_id'] = str(match['_id'])
    
    return render_template('liga_matches_partial.html', matchs=matchs_journee, selected_journee=journee, logos_equipe_liga= logos_equipe_liga)

# Route pour la génération de prévisions de matchs de la Liga
@app.route('/liga/predictions')
def liga_predictions():
    predictions = generate_predictions2()  # Générer les prévisions de matchs
    return render_template('liga_predictions.html', predictions=predictions)  # Rendre le template avec les prévisions

# Route pour la prédiction de matchs de la Liga
@app.route('/liga/predict', methods=['POST'])
def liga_predict():
    data = request.get_json()
    equipe_domicile = data.get('equipe_domicile')
    equipe_exterieur = data.get('equipe_exterieur')
    
    # Récupérer les prévisions depuis la fonction generate_predictions()
    predictions = generate_predictions2()
    
    # Trouver la prédiction correspondante aux équipes sélectionnées
    for prediction in predictions:
        if prediction['equipe_domicile'] == equipe_domicile and prediction['equipe_exterieur'] == equipe_exterieur:
            # Formater la prédiction
            prediction_text = f"Prédiction pour le match entre {equipe_domicile} et {equipe_exterieur} :"
            prediction_text += f" Victoire domicile : {prediction['pourcentage_victoire_domicile']}%"
            prediction_text += f" | Victoire extérieur : {prediction['pourcentage_victoire_exterieur']}%"
            prediction_text += f" | Match nul : {prediction['pourcentage_match_nul']}%"
            
            return jsonify({'prediction': prediction_text})
    
    # Si aucune prédiction correspondante n'est trouvée, renvoyer un message d'erreur
    return jsonify({'prediction': "Aucune prédiction disponible pour ce match."})

# Route pour afficher les statistiques avancées du match sélectionné de la Liga
@app.route('/liga/stats_avancees', methods=['POST'])
def liga_stats_avancees():
    match_id = request.form['match_id']  # Récupérer l'ID du match sélectionné depuis le formulaire
    
    # Implémentez votre logique pour récupérer les statistiques avancées du match en utilisant match_id
    
    # Renvoyer les statistiques avancées au format JSON
    stats = {
        'match_id': match_id,
        'stats': {
            'statistique1': 'valeur1',
            'statistique2': 'valeur2',
            # Ajoutez d'autres statistiques au besoin
        }
    }
    
    return jsonify(stats)

# Route pour la page d'accueil serieA
@app.route('/serieA')
def serieA():
    classement, matchs, buteurs, logos_equipe_serieA = get_data_from_mongodb3()
    last_matches = get_matchs_by_journee3(31)  # Récupérer les matchs de la 26e journée
    next_matches = get_matchs_by_journee3(32)  # Récupérer les matchs de la 27e journée
    return render_template('serieA.html', classement=classement, matchs=matchs, last_matches=last_matches, next_matches=next_matches, buteurs=buteurs, logos_equipe_serieA=logos_equipe_serieA)

# Route pour les matchs de la liga
@app.route('/serieA/matches')
def serieA_matches():
    classement, matchs, buteurs, logos_equipe_serieA = get_data_from_mongodb3()
    last_matches = get_matchs_by_journee3(31)  # Récupérer les matchs de la 26e journée
    next_matches = get_matchs_by_journee3(32)  # Récupérer les matchs de la 27e journée
    return render_template('serieA.html', classement=classement, matchs=matchs, last_matches=last_matches, next_matches=next_matches, buteurs=buteurs, logos_equipe_serieA=logos_equipe_serieA)



# Route pour la page des matchs par journée de la serieA
@app.route('/serieA/matches_by_journee', methods=['POST'])
def serieA_matches_by_journee():
    journee = request.form['journee']  # Récupérer la journée sélectionnée depuis le formulaire
    matchs_journee, logos_equipe_serieA = get_matchs_by_journee_with_logos_serieA(journee)  # Récupérer les matchs pour la journée sélectionnée
    
    # Si vous stockez la journée sous forme d'ObjectId, convertissez-la en chaîne de caractères
    if not isinstance(journee, str):
        journee = str(journee)
    
    # Convertissez les ObjectIds en chaînes de caractères si nécessaire
    for match in matchs_journee:
        if '_id' in match:
            match['_id'] = str(match['_id'])
    
    return render_template('serieA_matches_partial.html', matchs=matchs_journee, selected_journee=journee, logos_equipe_serieA=logos_equipe_serieA)


# Route pour la génération de prévisions de matchs de la serieA
@app.route('/serieA/predictions')
def serieA_predictions():
    predictions = generate_predictions3()  # Générer les prévisions de matchs
    return render_template('serieA_predictions.html', predictions=predictions)  # Rendre le template avec les prévisions

# Route pour la prédiction de matchs de la serieA
@app.route('/serieA/predict', methods=['POST'])
def serieA_predict():
    data = request.get_json()
    equipe_domicile = data.get('equipe_domicile')
    equipe_exterieur = data.get('equipe_exterieur')
    
    # Récupérer les prévisions depuis la fonction generate_predictions()
    predictions = generate_predictions3()
    
    # Trouver la prédiction correspondante aux équipes sélectionnées
    for prediction in predictions:
        if prediction['equipe_domicile'] == equipe_domicile and prediction['equipe_exterieur'] == equipe_exterieur:
            # Formater la prédiction
            prediction_text = f"Prédiction pour le match entre {equipe_domicile} et {equipe_exterieur} :"
            prediction_text += f" Victoire domicile : {prediction['pourcentage_victoire_domicile']}%"
            prediction_text += f" | Victoire extérieur : {prediction['pourcentage_victoire_exterieur']}%"
            prediction_text += f" | Match nul : {prediction['pourcentage_match_nul']}%"
            
            return jsonify({'prediction': prediction_text})
    
    # Si aucune prédiction correspondante n'est trouvée, renvoyer un message d'erreur
    return jsonify({'prediction': "Aucune prédiction disponible pour ce match."})



# Route pour la page d'accueil bundesliga
@app.route('/bundesliga')
def bundesliga():
    classement, matchs, buteurs, logos_equipe_bundes = get_data_from_mongodb4()
    last_matches = get_matchs_by_journee4(28)  # Récupérer les matchs de la 26e journée
    next_matches = get_matchs_by_journee4(29)  # Récupérer les matchs de la 27e journée
    return render_template('bundesliga.html', classement=classement, matchs=matchs, last_matches=last_matches, next_matches=next_matches, buteurs=buteurs, logos_equipe_bundes=logos_equipe_bundes)

# Route pour les matchs de la bundesliga
@app.route('/bundesliga/matches')
def bundesliga_matches():
    classement, matchs, buteurs, logos_equipe_bundes = get_data_from_mongodb4()
    last_matches = get_matchs_by_journee4(28)  # Récupérer les matchs de la 26e journée
    next_matches = get_matchs_by_journee4(29)  # Récupérer les matchs de la 27e journée
    return render_template('bundesliga.html', classement=classement, matchs=matchs, last_matches=last_matches, next_matches=next_matches, buteurs=buteurs, logos_equipe_bundes=logos_equipe_bundes)


# Route pour la page des matchs par journée de la bundesliga
@app.route('/bundesliga/matches_by_journee', methods=['POST'])
def bundesliga_matches_by_journee4():
    journee = request.form['journee']  # Récupérer la journée sélectionnée depuis le formulaire
    matchs_journee, logos_equipe_bundes = get_matchs_by_journee_with_logos_bundes(journee)  # Récupérer les matchs pour la journée sélectionnée
    
    # Si vous stockez la journée sous forme d'ObjectId, convertissez-la en chaîne de caractères
    if not isinstance(journee, str):
        journee = str(journee)
    
    # Convertissez les ObjectIds en chaînes de caractères si nécessaire
    for match in matchs_journee:
        if '_id' in match:
            match['_id'] = str(match['_id'])
    
    return render_template('bundesliga_matches_partial.html', matchs=matchs_journee, selected_journee=journee, logos_equipe_bundes=logos_equipe_bundes)

# Route pour la génération de prévisions de matchs de la bundesliga
@app.route('/bundesliga/predictions')
def bundesliga_predictions():
    predictions = generate_predictions4()  # Générer les prévisions de matchs
    return render_template('bundesliga_predictions.html', predictions=predictions)  # Rendre le template avec les prévisions

# Route pour la prédiction de matchs de la bundesliga
@app.route('/bundesliga/predict', methods=['POST'])
def bundesliga_predict():
    data = request.get_json()
    equipe_domicile = data.get('equipe_domicile')
    equipe_exterieur = data.get('equipe_exterieur')
    
    # Récupérer les prévisions depuis la fonction generate_predictions()
    predictions = generate_predictions4()
    
    # Trouver la prédiction correspondante aux équipes sélectionnées
    for prediction in predictions:
        if prediction['equipe_domicile'] == equipe_domicile and prediction['equipe_exterieur'] == equipe_exterieur:
            # Formater la prédiction
            prediction_text = f"Prédiction pour le match entre {equipe_domicile} et {equipe_exterieur} :"
            prediction_text += f" Victoire domicile : {prediction['pourcentage_victoire_domicile']}%"
            prediction_text += f" | Victoire extérieur : {prediction['pourcentage_victoire_exterieur']}%"
            prediction_text += f" | Match nul : {prediction['pourcentage_match_nul']}%"
            
            return jsonify({'prediction': prediction_text})
    
    # Si aucune prédiction correspondante n'est trouvée, renvoyer un message d'erreur
    return jsonify({'prediction': "Aucune prédiction disponible pour ce match."})


# Route pour la page d'accueil serieA
@app.route('/PL')
def PL():
    classement, matchs, buteurs, logos_equipe_PL = get_data_from_mongodb5()
    last_matches = get_matchs_by_journee5(32)  # Récupérer les matchs de la 26e journée
    next_matches = get_matchs_by_journee5(33)  # Récupérer les matchs de la 27e journée
    return render_template('PL.html', classement=classement, matchs=matchs, last_matches=last_matches, next_matches=next_matches, buteurs=buteurs, logos_equipe_PL=logos_equipe_PL)

# Route pour les matchs de la liga
@app.route('/PL/matches')
def PL_matches():
    classement, matchs, buteurs, logos_equipe_PL = get_data_from_mongodb5()
    last_matches = get_matchs_by_journee5(32)  # Récupérer les matchs de la 26e journée
    next_matches = get_matchs_by_journee5(33)  # Récupérer les matchs de la 27e journée
    return render_template('PL.html', classement=classement, matchs=matchs, last_matches=last_matches, next_matches=next_matches, buteurs=buteurs, logos_equipe_PL=logos_equipe_PL)



# Route pour la page des matchs par journée de la Liga
@app.route('/PL/matches_by_journee', methods=['POST'])
def PL_matches_by_journee():
    journee = request.form['journee']  # Récupérer la journée sélectionnée depuis le formulaire
    matchs_journee, logos_equipe_PL = get_matchs_by_journee_with_logos_PL(journee)  # Récupérer les matchs pour la journée sélectionnée
    
    # Si vous stockez la journée sous forme d'ObjectId, convertissez-la en chaîne de caractères
    if not isinstance(journee, str):
        journee = str(journee)
    
    # Convertissez les ObjectIds en chaînes de caractères si nécessaire
    for match in matchs_journee:
        if '_id' in match:
            match['_id'] = str(match['_id'])
    
    return render_template('PL_matches_partial.html', matchs=matchs_journee, selected_journee=journee, logos_equipe_PL=logos_equipe_PL)

# Route pour la génération de prévisions de matchs de la serieA
@app.route('/PL/predictions')
def PL_predictions():
    predictions = generate_predictions5()  # Générer les prévisions de matchs
    return render_template('PL_predictions.html', predictions=predictions)  # Rendre le template avec les prévisions

# Route pour la prédiction de matchs de la serieA
@app.route('/PL/predict', methods=['POST'])
def PL_predict():
    data = request.get_json()
    equipe_domicile = data.get('equipe_domicile')
    equipe_exterieur = data.get('equipe_exterieur')
    
    # Récupérer les prévisions depuis la fonction generate_predictions()
    predictions = generate_predictions5()
    
    # Trouver la prédiction correspondante aux équipes sélectionnées
    for prediction in predictions:
        if prediction['equipe_domicile'] == equipe_domicile and prediction['equipe_exterieur'] == equipe_exterieur:
            # Formater la prédiction
            prediction_text = f"Prédiction pour le match entre {equipe_domicile} et {equipe_exterieur} :"
            prediction_text += f" Victoire domicile : {prediction['pourcentage_victoire_domicile']}%"
            prediction_text += f" | Victoire extérieur : {prediction['pourcentage_victoire_exterieur']}%"
            prediction_text += f" | Match nul : {prediction['pourcentage_match_nul']}%"
            
            return jsonify({'prediction': prediction_text})
    
    # Si aucune prédiction correspondante n'est trouvée, renvoyer un message d'erreur
    return jsonify({'prediction': "Aucune prédiction disponible pour ce match."})


# # Créez une instance de l'application Dash en utilisant dash_L1()
# dash_app = dash_L1(app)

# # Initialisez votre application Flask avec l'application Dash
# dash_app.init_app(app)

# Définissez la route de l'application Flask
# Définissez la route de l'application Flask
@app.route('/dashboard')
def dashboard():
    # Redirigez l'utilisateur vers l'URL souhaitée
    return redirect("http://127.0.0.1:8050/dashboard")

@app.route('/retour')
def retour():
    return redirect("http://127.0.0.1:5000/", code=302)


# def start_dash_app():
#     # Exécutez la fonction test_all_dash() pour obtenir l'application Dash
#     dash_app = test_all_dash()
#     # Démarrez le serveur Dash dans le thread actuel
#     dash_app.run_server(debug=True, host='127.0.0.1', port=8050)

# @app.route('/dashboard')
# def dashboard():
#     from scripts_py.dash_L1_all_saisons_matchs import test_all_dash
#     # Obtenez l'application Dash
#     dash_app = test_all_dash()
#     # Exécutez l'application Dash
#     dash_app.run_server(debug=True)
#     # Vous pouvez rediriger l'utilisateur si nécessaire, mais assurez-vous qu'il est après l'exécution de l'application Dash
#     return redirect("http://127.0.0.1:8050/")

# # Route pour l'onglet Dashboard
# @app.route('/dashboard')
# def dashboard():
#     return render_template('dashboard.html')  # Assurez-vous d'avoir un template HTML pour afficher votre application Dash

# @app.route('/dashboard/L1')
# def dashboard_L1():
#     # Votre code pour afficher la page de la Ligue 1
#     return render_template('L1.html')  # Assurez-vous d'avoir un template HTML pour afficher votre application Dash


# import json
# # Fonction pour créer les graphiques à partir des données MongoDB
# def create_graphs_from_mongodb_data(classement, matchs, buteurs, logos_ligue_1):
#     # Utilisez les données des matchs
#     trace = go.Bar(
#         x=[entry["Journée"] for entry in matchs],  
#         y=[entry["Score"] for entry in matchs],     
#         marker=dict(color='rgb(158,202,225)'),
#         name='Score'
#     )

#     layout = go.Layout(
#         title='Scores par journée',
#         xaxis=dict(title='Journée'),
#         yaxis=dict(title='Score')
#     )

#     graph_data = [trace]

#     return {'graph1': {'data': graph_data, 'layout': layout}}


# # Route pour renvoyer les données au format JSON
# @app.route("/data")
# def data():
#     matchs = get_data_from_mongodb()
#     return jsonify(data)


@app.route('/update_championship/<championship>', methods=['POST'])
def update_championship(championship):
    if request.method == 'POST':
        if championship == 'Ligue 1':
            try:
                from scripts_py.Scrap_Classement_L1 import scrap_maxifoot, write_to_mongodb
                from scripts_py.Matchs_L1 import extract_and_save_to_csv, store_in_mongodb
                from scripts_py.Buteurs_L1 import scrape_and_store_buteurs
                from scripts_py.Buteurs_Liga import buteurs_liga
                from scripts_py.Buteurs_SerieA import buteurs_serieA
                from scripts_py.Buteurs_Bundesliga import buteurs_bundesliga
                from scripts_py.Matchs_PL import matchs_PL
                from scripts_py.Buteurs_PL import buteurs_PL

                # URL des différentes pages à scraper
                classement_url = "https://www.maxifoot.fr/resultat-ligue-1-france.htm"
                matchs_url = "https://www.maxifoot.fr/calendrier-ligue-1-france-2023-2024.htm#j2"
                matchs_url_PL = "https://www.maxifoot.fr/calendrier-premier-league-angleterre.htm"
                buteurs_url = "https://www.maxifoot.fr/classement-buteur-ligue-1-france.htm"
                buteurs_url_liga = "https://www.maxifoot.fr/classement-buteur-liga-espagne.htm"
                buteurs_url_serieA = "https://www.maxifoot.fr/classement-buteur-serie-a-italie.htm"
                buteurs_url_bundesliga = "https://www.maxifoot.fr/classement-buteur-bundesliga-allemagne.htm"
                buteurs_url_PL = "https://www.maxifoot.fr/classement-buteur-premier-league-angleterre.htm"
                

                # Informations de connexion à MongoDB
                mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
                db_name = "SoccerStats"
                classement_collection_name = "classement_L1"
                matchs_collection_name = "matchs_L1"
                matchs_collection_name_PL = "matchs_PL"
                buteurs_collection_name = "Buteurs_L1"
                buteurs_liga_collection_name = "Buteurs_Liga"
                buteurs_serieA_collection_name = "Buteurs_SerieA"
                buteurs_bundes_collection_name = "Buteurs_Bundesliga"
                buteurs_PL_collection_name = "Buteurs_PL"

                # Scraping et écriture des classements dans MongoDB
                classement_data = scrap_maxifoot(classement_url)
                if classement_data:
                    write_to_mongodb(classement_data, mongodb_uri, db_name, classement_collection_name)
                else:
                    return jsonify({'error': 'Failed to update championship classement.'}), 500

                # Scraping et stockage des matchs dans MongoDB
                extract_and_save_to_csv(matchs_url)
                store_in_mongodb(mongodb_uri, db_name, matchs_collection_name)

                matchs_PL(matchs_url_PL, mongodb_uri, db_name, matchs_collection_name_PL)

                # Scraping et stockage des buteurs dans MongoDB
                scrape_and_store_buteurs(buteurs_url, mongodb_uri, db_name, buteurs_collection_name)

                buteurs_liga(buteurs_url_liga, mongodb_uri, db_name, buteurs_liga_collection_name)

                buteurs_serieA(buteurs_url_serieA, mongodb_uri, db_name, buteurs_serieA_collection_name)

                buteurs_bundesliga(buteurs_url_bundesliga, mongodb_uri, db_name, buteurs_bundes_collection_name)

                buteurs_PL(buteurs_url_PL, mongodb_uri, db_name, buteurs_PL_collection_name)

                return jsonify({'message': f'Championship {championship} updated successfully!'}), 200
            except Exception as e:
                return jsonify({'error': str(e)}), 500
                
        elif championship == 'Liga':
            try:
                from scripts_py.scrap_classement_Liga import scrap_liga, insert_into_mongodb
                from scripts_py.Matchs_Liga import extract_and_save_to_csv_liga, store_in_mongodb_liga
                from scripts_py.Buteurs_Liga import buteurs_liga


                # URL des différentes pages à scraper
                classement_url = "https://www.maxifoot.fr/resultat-liga-espagne.htm"
                matchs_url = "https://www.maxifoot.fr/calendrier-liga-espagne.htm"
                buteurs_url = "https://www.maxifoot.fr/classement-buteur-liga-espagne.htm"

                # Informations de connexion à MongoDB
                mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
                db_name = "SoccerStats"
                classement_collection_name = "classement_Liga"
                matchs_collection_name = "matchs_Liga"
                buteurs_collection_name = "Buteurs_Liga"

                # Scraping et écriture des classements dans MongoDB
                classement_data = scrap_liga(classement_url)
                if classement_data:
                    insert_into_mongodb(classement_data, mongodb_uri, db_name, classement_collection_name)
                else:
                    return jsonify({'error': 'Failed to update Liga classement.'}), 500
                

                # Scraping et stockage des matchs dans MongoDB
                csv_file = extract_and_save_to_csv_liga(matchs_url)
                if csv_file:
                    store_in_mongodb_liga(csv_file, mongodb_uri, db_name, matchs_collection_name)


                # Scraping et stockage des buteurs dans MongoDB
                print("Appel de la fonction buteurs_liga...")
                buteurs_liga(buteurs_url, mongodb_uri, db_name, buteurs_collection_name)
                print("Fin du scraping et stockage des buteurs de la Liga.")

                return jsonify({'message': f'Championship {championship} updated successfully!'}), 200
            except Exception as e:
                return jsonify({'error': str(e)}), 500
            
        elif championship == 'Série A':
            try:
                from scripts_py.Scrap_classement_serieA import classement_serieA
                from scripts_py.Matchs_SerieA import matchs_serieA
                from scripts_py.Buteurs_SerieA import buteurs_serieA


                # URL des différentes pages à scraper
                classement_url = "https://www.maxifoot.fr/resultat-serie-a-italie.htm"
                matchs_url = "https://www.maxifoot.fr/calendrier-serie-a-italie.htm"
                buteurs_url = "https://www.maxifoot.fr/classement-buteur-serie-a-italie.htm"

                # Informations de connexion à MongoDB
                mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
                db_name = "SoccerStats"
                classement_collection_name = "classement_SerieA"
                matchs_collection_name = "matchs_serieA"
                buteurs_collection_name = "Buteurs_SerieA"

                # Scraping et écriture des classements dans MongoDB
                classement_data = classement_serieA(classement_url)
                if classement_data:
                    classement_serieA(classement_data, mongodb_uri, db_name, classement_collection_name)
                else:
                    return jsonify({'error': 'Failed to update Liga classement.'}), 500
                

                # Scraping et stockage des matchs dans MongoDB
                csv_file = matchs_serieA(matchs_url)
                if csv_file:
                    matchs_serieA(csv_file, mongodb_uri, db_name, matchs_collection_name)


                # Scraping et stockage des buteurs dans MongoDB
                print("Appel de la fonction buteurs_serieA...")
                buteurs_serieA(buteurs_url, mongodb_uri, db_name, buteurs_collection_name)
                print("Fin du scraping et stockage des buteurs de la serieA.")

                return jsonify({'message': f'Championship {championship} updated successfully!'}), 200
            except Exception as e:
                return jsonify({'error': str(e)}), 500    
            
        elif championship == 'Bundesliga':
            try:
                from scripts_py.Scrap_classement_Bundesliga import classement_bundesliga
                from scripts_py.Matchs_Bundesliga import matchs_bundesliga
                from scripts_py.Buteurs_Bundesliga import buteurs_bundesliga


                # URL des différentes pages à scraper
                classement_url = "https://www.matchendirect.fr/allemagne/bundesliga-1/"
                matchs_url = "https://www.maxifoot.fr/calendrier-bundesliga-allemagne.htm"
                buteurs_url = "https://www.maxifoot.fr/classement-buteur-serie-a-italie.htm"

                # Informations de connexion à MongoDB
                mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
                db_name = "SoccerStats"
                classement_collection_name = "classement_bundesliga"
                matchs_collection_name = "matchs_bundesliga"
                buteurs_collection_name = "Buteurs_Bundesliga"

                # Scraping et écriture des classements dans MongoDB
                classement_data = classement_bundesliga(classement_url)
                if classement_data:
                    classement_bundesliga(classement_data, mongodb_uri, db_name, classement_collection_name)
                else:
                    return jsonify({'error': 'Failed to update Liga classement.'}), 500
                

                # Scraping et stockage des matchs dans MongoDB
                csv_file = matchs_bundesliga(matchs_url)
                if csv_file:
                    matchs_bundesliga(csv_file, mongodb_uri, db_name, matchs_collection_name)


                # Scraping et stockage des buteurs dans MongoDB
                print("Appel de la fonction buteurs_bundesliga...")
                buteurs_bundesliga(buteurs_url, mongodb_uri, db_name, buteurs_collection_name)
                print("Fin du scraping et stockage des buteurs de la bundesliga.")

                return jsonify({'message': f'Championship {championship} updated successfully!'}), 200
            except Exception as e:
                return jsonify({'error': str(e)}), 500

        elif championship == 'PL':
            try:
                from scripts_py.Scrap_classement_PL import classement_PL
                from scripts_py.Matchs_PL import matchs_PL
                from scripts_py.Buteurs_PL import buteurs_PL


                # URL des différentes pages à scraper
                classement_url = "https://www.maxifoot.fr/resultat-premier-league-angleterre.htm"
                matchs_url = "https://www.maxifoot.fr/calendrier-premier-league-angleterre.htm"
                buteurs_url = "https://www.maxifoot.fr/classement-buteur-premier-league-angleterre.htm"

                # Informations de connexion à MongoDB
                mongodb_uri = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1"
                db_name = "SoccerStats"
                classement_collection_name = "classement_PL"
                matchs_collection_name = "matchs_PL"
                buteurs_collection_name = "Buteurs_PL"

                # Scraping et écriture des classements dans MongoDB
                classement_data = classement_PL(classement_url)
                if classement_data:
                    classement_PL(classement_data, mongodb_uri, db_name, classement_collection_name)
                else:
                    return jsonify({'error': 'Failed to update Liga classement.'}), 500
                

                # Scraping et stockage des matchs dans MongoDB
                csv_file = matchs_PL(matchs_url)
                if csv_file:
                    matchs_PL(csv_file, mongodb_uri, db_name, matchs_collection_name)


                # Scraping et stockage des buteurs dans MongoDB
                print("Appel de la fonction buteurs_PL...")
                buteurs_PL(buteurs_url, mongodb_uri, db_name, buteurs_collection_name)
                print("Fin du scraping et stockage des buteurs de la PL.")

                return jsonify({'message': f'Championship {championship} updated successfully!'}), 200
            except Exception as e:
                return jsonify({'error': str(e)}), 500            
            
        else:
                return jsonify({'error': 'Invalid championship.'}), 400
        
        
    else:
        return jsonify({'error': 'Method not allowed.'}), 405
         

if __name__ == '__main__':
    app.run(debug=True)
