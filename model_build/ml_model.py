# 1. Import necessary libraries
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from imblearn.over_sampling import SMOTE
import pickle

# 2. Load data
data = pd.read_csv('synthetic_traffic_data.csv')

# 3. Preprocess data
# Encode categorical features and keep a clean traffic feature set.
X = data.drop('congestion_level', axis=1)
y = data['congestion_level']

categorical_features = ['weather']
numeric_features = ['road_id', 'avg_speed_kmph', 'vehicle_count', 'occupancy_pct', 'hour_of_day', 'day_of_week']

preprocessor = ColumnTransformer(
    transformers=[
        ('num', 'passthrough', numeric_features),
        ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features),
    ]
)

# 4. Handle class imbalance using SMOTE on the transformed training data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

transformed_train = preprocessor.fit_transform(X_train)
sm = SMOTE(random_state=42)
X_res, y_res = sm.fit_resample(transformed_train, y_train)

# 5. Train a Random Forest baseline
model = RandomForestClassifier(n_estimators=200, random_state=42, class_weight='balanced')
model.fit(X_res, y_res)

# 6. Evaluate the model
X_test_transformed = preprocessor.transform(X_test)
y_pred = model.predict(X_test_transformed)
print(classification_report(y_test, y_pred))
print(confusion_matrix(y_test, y_pred))

# 7. Save a pipeline containing preprocessing and model
pipeline = Pipeline([
    ('preprocessor', preprocessor),
    ('model', model),
])

with open('traffic_detection_model.pkl', 'wb') as model_file:
    pickle.dump(pipeline, model_file)
print("Model saved as traffic_detection_model.pkl")