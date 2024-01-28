import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report
from sklearn.preprocessing import StandardScaler
from shapely.geometry import Point
import random
import geopandas as gpd
import os

script_dir = os.path.dirname(os.path.abspath(__file__))

data_file = os.path.join(script_dir, "../../dsp-dagster/data/test.csv")
data = pd.read_csv(data_file, low_memory=False)

tree_coord_file = os.path.join(script_dir, "../../dsp-dagster/data/tree_lon-lat.csv")
tree_coord = pd.read_csv(tree_coord_file, low_memory=False)

tree_data_file = os.path.join(script_dir, "../../dsp-dagster/data/tree_data.csv")
tree_data = pd.read_csv(tree_data_file, low_memory=False)

weather_data_file = os.path.join(script_dir, "../../dsp-dagster/data/maandag.txt")
weather_data = pd.read_csv(weather_data_file, low_memory=False)

augmented_non_incidents_file = os.path.join(
    script_dir, "../../dsp-dagster/data/non_incident_trees.csv"
)
augmented_non_incidents = pd.read_csv(augmented_non_incidents_file, low_memory=False)

data["Incident_Occurred"] = np.where(data["Incident_ID"].notna(), 1, 0)
non_incident_data = data[data["Incident_Occurred"] == 0]
incident_data = data[data["Incident_Occurred"] == 1]
# Filtering the incident_data for 'Damage_Type' equals 'Tree'
incident_data_tree = incident_data[incident_data["Damage_Type"] == "Tree"]

# Filtering incident_data_tree for unique values of "Incident_ID"
unique_incident_data_tree = incident_data_tree.drop_duplicates(subset=["Incident_ID"])

# Making a copy of the non_incident_data to avoid modifying the original DataFrame
non_incident_data_mod = non_incident_data.copy()

# Randomly assign tree coordinates to non-incident data using .loc
non_incident_data_mod.loc[:, "LON"] = np.random.choice(
    tree_coord["Longitude"], size=len(non_incident_data_mod)
)
non_incident_data_mod.loc[:, "LAT"] = np.random.choice(
    tree_coord["Latitude"], size=len(non_incident_data_mod)
)

non_incident_sample = non_incident_data_mod.sample(
    n=100 * len(unique_incident_data_tree), random_state=42
)

tree_data = tree_data.merge(tree_coord, on="id", how="left")
tree_data = tree_data.drop(tree_data[tree_data["jaarVanAanleg"] == 0.0].index)
tree_data.dropna(subset=["jaarVanAanleg"])

final_tree_data = pd.get_dummies(tree_data, columns=["boomhoogteklasseActueel"])
final_tree_data["LON"] = tree_data["Longitude"]
final_tree_data["LAT"] = tree_data["Latitude"]
final_tree_data = final_tree_data.drop(["Longitude", "Latitude"], axis=1)
final_tree_data = final_tree_data[
    [
        "jaarVanAanleg",
        "boomhoogteklasseActueel_a. tot 6 m.",
        "boomhoogteklasseActueel_b. 6 tot 9 m.",
        "boomhoogteklasseActueel_c. 9 tot 12 m.",
        "boomhoogteklasseActueel_d. 12 tot 15 m.",
        "boomhoogteklasseActueel_e. 15 tot 18 m.",
        "boomhoogteklasseActueel_f. 18 tot 24 m.",
        "boomhoogteklasseActueel_g. 24 m. en hoger",
        "boomhoogteklasseActueel_q. Niet van toepassing",
        "LON",
        "LAT",
    ]
]

weather_data.columns = [
    "# STN",
    "YYYYMMDD",
    "HH",
    "Dd",
    "Fh",
    "Ff",
    "Fx",
    "T",
    "T10N",
    "Td",
    "Sq",
    "Q",
    "Dr",
    "Rh",
    "P",
    "Vv",
    "N",
    "U",
    "Ww",
    "Ix",
    "M",
    "R",
    "S",
    "O",
    "Y",
]

weather_data_subset = weather_data.drop(["# STN", "T10N", "Ww"], axis=1)

merged_data = pd.merge(weather_data_subset, final_tree_data, how="cross")


# Convert incident data and tree data to GeoDataFrames
incident_gdf = gpd.GeoDataFrame(
    unique_incident_data_tree,
    geometry=gpd.points_from_xy(
        unique_incident_data_tree.LON, unique_incident_data_tree.LAT
    ),
)
tree_gdf = gpd.GeoDataFrame(
    tree_data, geometry=gpd.points_from_xy(tree_data.Longitude, tree_data.Latitude)
)

# Set the original CRS for both GeoDataFrames to WGS84 (EPSG:4326)
incident_gdf.set_crs(epsg=4326, inplace=True)
tree_gdf.set_crs(epsg=4326, inplace=True)

# Reproject to a suitable projected CRS (e.g., UTM zone 31N)
incident_gdf = incident_gdf.to_crs(epsg=32631)
tree_gdf = tree_gdf.to_crs(epsg=32631)

# Perform the spatial join
incident_with_trees = gpd.sjoin_nearest(incident_gdf, tree_gdf, distance_col="distance")

# incident_with_trees now contains incidents with the nearest tree's characteristics in the correct CRS

model_columns = [
    "Dd",
    "Fh",
    "Ff",
    "Fx",
    "T",
    "Td",
    "Sq",
    "Q",
    "Dr",
    "Rh",
    "P",
    "Vv",
    "N",
    "U",
    "Ix",
    "M",
    "R",
    "S",
    "O",
    "Y",
    "LON",
    "LAT",
    "boomhoogteklasseActueel",
    "jaarVanAanleg",
    "Incident_Occurred",
    "distance",
]
incident_with_trees_selected = incident_with_trees[model_columns]

# Dropping rows with any NaNs in the selected columns
incident_with_trees_cleaned = incident_with_trees_selected.dropna()

augmented_non_incidents["LON"] = augmented_non_incidents["Longitude"]
augmented_non_incidents["LAT"] = augmented_non_incidents["Latitude"]
augmented_non_incidents = augmented_non_incidents.drop(
    columns=["Longitude", "Latitude"]
)

### Prepping for modelling
balanced_data = pd.concat([incident_with_trees_cleaned, augmented_non_incidents])

# One-hot encoding for the categorical column
balanced_data_encoded = pd.get_dummies(
    balanced_data, columns=["boomhoogteklasseActueel"]
)
current_year = 2024  # Replace with the current year
balanced_data_encoded["Tree_Age"] = (
    current_year - balanced_data_encoded["jaarVanAanleg"]
)

balanced_data_encoded = balanced_data_encoded.dropna(
    subset=["Vv", "N", "M", "R", "S", "O", "Y", "Fh"]
)

# Selecting columns for the model
model_columns = [
    "Dd",
    "Fh",
    "Ff",
    "Fx",
    "T",
    "Td",
    "Sq",
    "Q",
    "Dr",
    "Rh",
    "P",
    "Vv",
    "N",
    "U",
    "Ix",
    "M",
    "R",
    "S",
    "O",
    "Y",
    "jaarVanAanleg",
    "boomhoogteklasseActueel_a. tot 6 m.",
    "boomhoogteklasseActueel_b. 6 tot 9 m.",
    "boomhoogteklasseActueel_c. 9 tot 12 m.",
    "boomhoogteklasseActueel_d. 12 tot 15 m.",
    "boomhoogteklasseActueel_e. 15 tot 18 m.",
    "boomhoogteklasseActueel_f. 18 tot 24 m.",
    "boomhoogteklasseActueel_g. 24 m. en hoger",
    "boomhoogteklasseActueel_q. Niet van toepassing",
    "LON",
    "LAT",
]
model_data = balanced_data_encoded[model_columns]

# Split data into features and target
X = model_data
y = balanced_data_encoded["Incident_Occurred"]

### Modelling

# Scaling features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Splitting data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(
    X_scaled, y, test_size=0.3, random_state=42
)

# Example: Training a Random Forest Classifier
model = RandomForestClassifier(random_state=42)
model.fit(X_train, y_train)
# Assuming 'model' is your trained random forest model
y_pred = model.predict(X_test)

accuracy = accuracy_score(y_test, y_pred)
roc_auc = roc_auc_score(y_test, y_pred)
classification_rep = classification_report(y_test, y_pred)

# Confusion matrix
conf_matrix = confusion_matrix(y_test, y_pred)

# Recall for class 1
# recall_class_1 = recall_score(y_test, y_pred, pos_label=1)

print("Accuracy:", accuracy)
print("ROC AUC:", roc_auc)
print("\nConfusion Matrix:\n", conf_matrix)
print("\nClassification Report:\n", classification_rep)
# print("Recall for Class 1:", recall_class_1)


# # Make predictions and calculate metrics
# y_pred = model.predict(X_test)
# mse = mean_squared_error(y_test, y_pred)
# rmse = np.sqrt(mse)
# mae = mean_absolute_error(y_test, y_pred)

# # Plot feature importances
# feature_importances = model.feature_importances_


# print(feature_importances)
# plot_feature_importances(model, X_train.columns, top_n=20)


# # Set the MLflow tracking URI
# mlflow.set_tracking_uri("http://dsp-mlflow:5001")

# # Start an MLFlow run
# with mlflow.start_run(run_name="Incident Prediction Model"):
#     # Log model
#     mlflow.xgboost.log_model(model, "xgboost-model")

#     # Log parameters
#     mlflow.log_params(model.get_params())

#     # Log metrics
#     mlflow.log_metric("MSE", mse)
#     mlflow.log_metric("RMSE", rmse)
#     mlflow.log_metric("MAE", mae)
