from pymongo import MongoClient
import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import joblib

def train_model():
    # Connexion à la base de données MongoDB
    client = MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.6.1")
    db = client['SoccerStats']  # Nom de votre base de données

    # Charger les données de chaque collection dans des DataFrames pandas
    df_general_current = pd.DataFrame(list(db['Stats_All_Team_General_L1'].find()))
    df_general_past = pd.concat([pd.DataFrame(list(db[f'Stats_All_Team_General_{year}_L1'].find())) for year in range(2020, 2024)])

    df_defensive_current = pd.DataFrame(list(db['Stats_All_Team_def_L1'].find()))
    df_defensive_past = pd.concat([pd.DataFrame(list(db[f'Stats_All_Team_def_{year}_L1'].find())) for year in range(2020, 2024)])

    df_offensive_current = pd.DataFrame(list(db['Stats_All_Team_off_L1'].find()))
    df_offensive_past = pd.concat([pd.DataFrame(list(db[f'Stats_All_Team_off_{year}_L1'].find())) for year in range(2020, 2024)])

    df_ranking_current = pd.DataFrame(list(db['classement_L1'].find()))
    df_ranking_past = pd.concat([pd.DataFrame(list(db[f'classement_L1_{year}'].find())) for year in range(2020, 2024)])

    df_matches_current = pd.DataFrame(list(db['matchs_L1'].find()))
    df_matches_past = pd.concat([pd.DataFrame(list(db[f'matchs_L1_{year}'].find())) for year in range(2020, 2024)])

    df_top_scorers_current = pd.DataFrame(list(db['Buteurs_L1'].find()))
    df_top_scorers_past = pd.concat([pd.DataFrame(list(db[f'Buteurs_L1_{year}'].find())) for year in range(2020, 2024)])

    # Sélection des colonnes pertinentes pour les statistiques défensives
    columns_defensive = ['Tirs pm', 'Tacles pm', 'Interceptions pm', 'Fautes pm', 'Hors-jeux pm']

    # Sélection des colonnes pertinentes pour les statistiques offensives
    columns_offensive = ['Tirs pm', 'Tirs CA pm', 'Dribbles pm', 'Fautes subies pm']

    # Sélection des colonnes pertinentes pour les statistiques générales
    columns_general = ['Buts', 'Tirs pm', 'Possession%', 'PassesRéussies%', 'AériensGagnés']

    # Fusionner les données des saisons précédentes avec les données actuelles
    df_defensive = pd.concat([df_defensive_current, df_defensive_past])
    df_offensive = pd.concat([df_offensive_current, df_offensive_past])
    df_general = pd.concat([df_general_current, df_general_past])

    # Remplacer les valeurs manquantes par la moyenne de chaque colonne
    df_defensive = df_defensive.fillna(df_defensive.mean(numeric_only=True))
    df_offensive = df_offensive.fillna(df_offensive.mean(numeric_only=True))
    df_general = df_general.fillna(df_general.mean(numeric_only=True))

    # Convertir les données textuelles en nombres si nécessaire (par exemple, en convertissant les pourcentages en nombres décimaux)
    df_general['Possession%'] = df_general['Possession%'].str.rstrip('%').astype('float') / 100
    df_general['PassesRéussies%'] = df_general['PassesRéussies%'].str.rstrip('%').astype('float') / 100

    # Sélection des colonnes pertinentes pour chaque collection
    columns_defensive = ['Tirs pm', 'Tacles pm', 'Interceptions pm', 'Fautes pm', 'Hors-jeux pm', 'Position']
    columns_offensive = ['Tirs pm', 'Tirs CA pm', 'Dribbles pm', 'Fautes subies pm', 'Position']
    columns_general = ['Buts', 'Tirs pm', 'Possession%', 'PassesRéussies%', 'AériensGagnés', 'Position']

    # Prétraitement des données pour chaque collection
    # Remplacer les valeurs manquantes par la moyenne de chaque colonne
    df_defensive = df_defensive.fillna(df_defensive.mean())
    df_offensive = df_offensive.fillna(df_offensive.mean())
    df_general = df_general.fillna(df_general.mean())

    # Séparation des caractéristiques et de la cible pour chaque collection
    X_defensive = df_defensive[columns_defensive].drop(columns=['Position'])
    y_defensive = df_defensive['Position']

    X_offensive = df_offensive[columns_offensive].drop(columns=['Position'])
    y_offensive = df_offensive['Position']

    X_general = df_general[columns_general].drop(columns=['Position'])
    y_general = df_general['Position']

    # Division des données en ensembles d'entraînement et de test
    X_train_defensive, X_test_defensive, y_train_defensive, y_test_defensive = train_test_split(X_defensive, y_defensive, test_size=0.2, random_state=42)
    X_train_offensive, X_test_offensive, y_train_offensive, y_test_offensive = train_test_split(X_offensive, y_offensive, test_size=0.2, random_state=42)
    X_train_general, X_test_general, y_train_general, y_test_general = train_test_split(X_general, y_general, test_size=0.2, random_state=42)

    # Définition des hyperparamètres à rechercher
    param_grid = {
        'n_estimators': [100, 150, 200],
        'max_depth': [5, 50, 100]
    }

    # GridSearchCV pour RandomForestRegressor pour les statistiques défensives
    grid_search_defensive = GridSearchCV(RandomForestRegressor(), param_grid, cv=5, scoring='neg_mean_squared_error')
    grid_search_defensive.fit(X_train_defensive, y_train_defensive)
    best_model_defensive = grid_search_defensive.best_estimator_

    # Validation du meilleur modèle défensif
    predictions_defensive = best_model_defensive.predict(X_test_defensive)
    mse_defensive = mean_squared_error(y_test_defensive, predictions_defensive)
    print("Mean Squared Error (Defensive):", mse_defensive)

    # Sauvegarde du meilleur modèle défensif
    joblib.dump(best_model_defensive, 'prediction_model_defensive.pkl')

    # GridSearchCV pour RandomForestRegressor pour les statistiques offensives
    grid_search_offensive = GridSearchCV(RandomForestRegressor(), param_grid, cv=5, scoring='neg_mean_squared_error')
    grid_search_offensive.fit(X_train_offensive, y_train_offensive)
    best_model_offensive = grid_search_offensive.best_estimator_

    # Validation du meilleur modèle offensif
    predictions_offensive = best_model_offensive.predict(X_test_offensive)
    mse_offensive = mean_squared_error(y_test_offensive, predictions_offensive)
    print("Mean Squared Error (Offensive):", mse_offensive)

    # Sauvegarde du meilleur modèle offensif
    joblib.dump(best_model_offensive, 'prediction_model_offensive.pkl')

    # GridSearchCV pour RandomForestRegressor pour les statistiques générales
    grid_search_general = GridSearchCV(RandomForestRegressor(), param_grid, cv=5, scoring='neg_mean_squared_error')
    grid_search_general.fit(X_train_general, y_train_general)
    best_model_general = grid_search_general.best_estimator_

    # Validation du meilleur modèle général
    predictions_general = best_model_general.predict(X_test_general)
    mse_general = mean_squared_error(y_test_general, predictions_general)
    print("Mean Squared Error (General):", mse_general)

    # Sauvegarde du meilleur modèle général
    joblib.dump(best_model_general, 'prediction_model_general.pkl')

    def make_match_prediction(match_data, model_defensive, model_offensive, model_general):
        # Prévoir les statistiques du match en utilisant les modèles entraînés
        # Les données du match doivent être fournies sous forme de dictionnaire contenant les statistiques défensives, offensives et générales
        # Les modèles entraînés doivent être passés en paramètres
        
        # Prédictions pour les statistiques défensives
        defensive_prediction = model_defensive.predict(match_data['defensive'].reshape(1, -1))
        
        # Prédictions pour les statistiques offensives
        offensive_prediction = model_offensive.predict(match_data['offensive'].reshape(1, -1))
        
        # Prédictions pour les statistiques générales
        general_prediction = model_general.predict(match_data['general'].reshape(1, -1))
        
        # Création d'un dictionnaire contenant les prédictions pour chaque type de statistiques
        match_predictions = {
            'defensive': defensive_prediction,
            'offensive': offensive_prediction,
            'general': general_prediction
        }
        
        return match_predictions
    
train_model()
