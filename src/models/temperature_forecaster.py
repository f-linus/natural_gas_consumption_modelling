import pandas as pd
import pickle as pkl
from src.data_handling import ingestion
import neuralprophet as nprophet
import requests
import os


open_meteo_endpoint = "https://api.open-meteo.com/v1/forecast"
weather_data_cities = [
    (52.520008, 13.404954),  # Berlin
    (51.339695, 12.373075),  # Dresden
    (50.937531, 6.960279),  # Cologne
    (48.135125, 11.581981),  # Munich
    (53.551086, 9.993682),  # Hamburg
]


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

        self.model = model

        return metrics

    def load_model(self, model_file: str = "models/temperature_model.pkl") -> None:

        if not os.path.exists(model_file):
            return False

        # Load model
        self.model = nprophet.load(model_file)

        return True

    @staticmethod
    def __get_current_weather_data() -> pd.Series:
        for city in weather_data_cities:
            params = {
                "latitude": city[0],
                "longitude": city[1],
                "hourly": "temperature_2m",
                "models": "ecmwf_ifs04",
                "past_days": 35,
                "forecast_days": 10,
            }
            response = requests.get(open_meteo_endpoint, params=params)
            data = response.json()

            temps = pd.Series(
                data["hourly"]["temperature_2m"], index=data["hourly"]["time"]
            )
            hourly_temperatures[city] = temps

        hourly_temperatures = hourly_temperatures.mean(axis="columns")

        # Make index a datetime object
        hourly_temperatures.index = pd.to_datetime(hourly_temperatures.index)

        # Group hourly temperatures by day
        daily_temperatures = hourly_temperatures.groupby(pd.Grouper(freq="D")).mean()

        return daily_temperatures

    def predict(self) -> pd.DataFrame:
        return None


if __name__ == "__main__":
    print("Hello")
