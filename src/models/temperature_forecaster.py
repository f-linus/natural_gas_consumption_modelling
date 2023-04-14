import pandas as pd
import pickle as pkl
from src.data_handling import ingestion
import neuralprophet as nprophet


class TemperatureForecaster:
    def __init__() -> None:
        pass

    def fit(self, model_file) -> None:

        # Read temperature data
        temperatures = ingestion.read_temperatures()
        temperatures.index.name = "ds"

        temperatures = temperatures.rename("y").to_frame().reset_index()

        # Split data into train and test
        train = temperatures[temperatures["ds"] < "2019-01-01"]
        test = temperatures[temperatures["ds"] >= "2019-01-01"]

        # Create model
        model = nprophet.NeuralProphet(
            n_forecasts=365,
            n_lags=45,
        )        

        # Fit model
        metrics = model.fit(train, freq="D")

        # Save model
        nprophet.save(model, model_file)

        return metrics

    def predict(self) -> pd.DataFrame:
        return None
    
    def load_model(self, model_file: str = "models/temperature_model.pkl") -> None:
        pass

if __name__ == "__main__":    
    print("Hello")