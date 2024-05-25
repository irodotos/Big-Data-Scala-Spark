import pandas as pd
import numpy as np
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split

# Load data
data = pd.read_csv('data.csv')

# Convert Date to datetime
data['Date'] = pd.to_datetime(data['Date'])

# Sort by date
data = data.sort_values('Date')

# Select features and targets
features = ['ExchangeRateTo €']
targets = ['Unleaded 95 OCT (€/1000L) (I)', 'Automotive Diesel (€/1000L) (I)', 'Heating gas oil (€/1000L) (II)']

# Normalize the data
scaler_features = MinMaxScaler()
scaler_targets = MinMaxScaler()

data[features] = scaler_features.fit_transform(data[features])
data[targets] = scaler_targets.fit_transform(data[targets])

# Prepare sequences for LSTM
def create_sequences(data, features, targets, seq_length):
    xs, ys = [], []
    for i in range(len(data) - seq_length):
        x = data[i:i+seq_length, :len(features)]
        y = data[i+seq_length, len(features):]
        xs.append(x)
        ys.append(y)
    return np.array(xs), np.array(ys)

# Define sequence length
seq_length = 30  # For example, use 30 days of data to predict the next day's prices
data_values = data[features + targets].values
X, y = create_sequences(data_values, features, targets, seq_length)

# Split data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Build the model
model = tf.keras.models.Sequential([
    tf.keras.layers.LSTM(64, activation='relu', input_shape=(seq_length, len(features))),
    tf.keras.layers.Dense(32, activation='relu'),
    tf.keras.layers.Dense(len(targets))  # 3 outputs for the 3 targets
])

model.compile(optimizer='adam', loss='mse')

# Train the model
history = model.fit(X_train, y_train, epochs=50, batch_size=32, validation_split=0.2)

# Evaluate the model
test_loss = model.evaluate(X_test, y_test)
print(f'Test Loss: {test_loss}')

# Function to predict future prices
def predict_future(data, model, seq_length, n_future, num_features):
    future_preds = []
    current_sequence = data[-seq_length:, :num_features]  # Start with the last sequence in the dataset

    for _ in range(n_future):
        prediction = model.predict(current_sequence[np.newaxis, :, :])
        future_preds.append(prediction[0])
        # Update current_sequence by removing the first entry and adding the new prediction
        new_entry = np.hstack((current_sequence[-1, :num_features], prediction[0]))
        current_sequence = np.vstack((current_sequence[1:], new_entry[:num_features]))

    return np.array(future_preds)

# Predict the next 365 days
n_future = 365
future_predictions = predict_future(data_values, model, seq_length, n_future, len(features))

# Inverse transform to get the original scale
future_predictions = scaler_targets.inverse_transform(future_predictions)

# Display the future predictions
print(future_predictions)
