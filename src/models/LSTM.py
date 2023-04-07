import torch
import torch.nn as nn
from torch.autograd import Variable
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import matplotlib.pyplot as plt
import seaborn as sns


class LSTMHelper:
    @staticmethod
    def create_dummy_train_test_set():
        # Create dummy sets that represent time series data with a simple sine wave
        y = np.sin(np.linspace(0, 500, 5001))

        # Create sequences of 25 timesteps and add them to a list
        seq_length = 25
        sequences = []
        targets = []
        for i in range(len(y) - seq_length):
            sequences.append(y[i : i + seq_length])
            targets.append(y[i + seq_length])

        X = np.array(sequences).reshape(-1, seq_length, 1)
        y = np.array(targets).reshape(-1, 1)

        # Convert to Pytorch tensors
    
        X = torch.from_numpy(X).float()
        y = torch.from_numpy(y).float()
        return X, y

    @staticmethod
    def train_lstm(model, n_epochs):

        # Set loss and optimizer function
        criterion = torch.nn.MSELoss()
        optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

        # Train the model (without split)
        epochs = 150
        for epoch in range(n_epochs):

            model.train()

            # Forward pass
            outputs = model(X)
            optimizer.zero_grad()
            loss = criterion(outputs, y)

            # Backward and optimize
            loss.backward()
            optimizer.step()

            if (epoch + 1) % 10 == 0:
                print(f"Epoch: {epoch+1}/{epochs}, Loss: {loss.item():.4f}")
        
        model.eval()
        return model


class LSTM(nn.Module):
    def __init__(self, num_classes, input_size, hidden_size, num_layers):
        super().__init__()

        self.num_classes = num_classes
        self.num_layers = num_layers
        self.input_size = input_size
        self.hidden_size = hidden_size

        # LSTM model
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=0,
        )

        # Fully connected layer
        self.fc_1 = nn.Linear(hidden_size * num_layers, 128)

        # Fully connected last layer
        self.fc_2 = nn.Linear(128, num_classes)

        self.relu = nn.ReLU()

    def forward(self, x):
        # Propagate input through LSTM
        output, (hn, cn) = self.lstm(x)  # (input, hidden, and internal state)

        hn = hn.view(-1, self.hidden_size * self.num_layers)
        out = self.relu(hn)
        out = self.fc_1(out)  # first dense
        out = self.relu(out)  # relu
        out = self.fc_2(out)  # final output

        return out


if __name__ == "__main__":

    # Create dummy data
    X, y = LSTMHelper.create_dummy_train_test_set()

    # Create LSTM model
    input_size = 1
    hidden_size = 10
    num_layers = 3
    num_classes = 1
    model = LSTM(num_classes, input_size, hidden_size, num_layers)

    # Train the model
    model = LSTMHelper.train_lstm(model, 150)

    # Make a rolling window prediction
    predictions = []
    rolling_x = torch.zeros(1, 25, 1)
    for i in range(200):
        y_pred = model(rolling_x)
        predictions.append(y_pred.item())
        rolling_x = torch.cat((rolling_x[:,1:,:], y_pred.reshape(1, 1, 1)), dim=1)

    # Plot the predictions
    plt.plot(predictions, label="Predictions")
    plt.show()
