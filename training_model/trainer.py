import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error as mae
from sklearn.linear_model import Ridge
import warnings
import pickle

# Suppress warnings
warnings.filterwarnings('ignore')

# Load data (assuming CSV file)
# Note: The data is loaded and a sample of 50% is taken to reduce the size for processing
ds = pd.read_csv("/home/melo/Desktop/TAP/Tap_project_jobs/archive/archive/job_descriptions2_categorized.csv").sample(frac=0.5, random_state=42)

# Separate features and target
# The target variable 'Salary Range' is separated from the features
ds1 = ds.drop(columns=["Salary Range", "Job Id"])
regr = ds["Salary Range"]

# Split data into training and testing sets
# The data is split into 80% training and 20% testing
X_train, X_test, y_train, y_test = train_test_split(ds1, regr, test_size=0.2, random_state=42)

# Initialize the Ridge regression model
# Ridge regression is chosen for regularization to handle multicollinearity and improve model performance
ridge_model = Ridge()

# Train the model
ridge_model.fit(X_train, y_train)

# Evaluate test error
# Predict the target variable on the test set and calculate the mean absolute error
test_preds = ridge_model.predict(X_test)
test_error = mae(y_test, test_preds)
print(f'Test Error: {test_error:.4f}')

# Save the trained model to a file for future use
with open('ridge_model.pkl', 'wb') as model_file:
    pickle.dump(ridge_model, model_file)
