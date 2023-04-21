import pandas as pd
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
    def __init__(self):
        self.model = None

    def load_model(self, model_file: str = "models/temperature_model.pkl") -> None:
        """Load model from disk.

        Args:
            model_file (str, optional): Where to load the model from. Defaults to "models/temperature_model.pkl".
        """

        if not os.path.exists(model_file):
            return False

        # Load model
        self.model = nprophet.load(model_file)

        return True

    @staticmethod
    def __get_current_weather_data() -> pd.Series:
        """Get current weather data from open-meteo.com."""

        # Get temperature data for all cities (each city is one column in the resulting dataframe)
        hourly_temperatures = pd.DataFrame()

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

    def cobmined_forecast(self) -> pd.DataFrame:
        """Combine current weather data with forecast.

        Returns:
            pd.DataFrame: Dataframe containing the current weather data and the forecast.
        """

        # Get current weather data
        current_weather_data = self.__get_current_weather_data()

        actual_temp_df = current_weather_data.rename("y").to_frame()
        actual_temp_df.index.name = "ds"
        actual_temp_df = actual_temp_df.reset_index()

        future = self.model.make_future_dataframe(
            actual_temp_df, n_historic_predictions=30
        )
        prediction = self.model.predict(future)
        latest_prediction = self.model.get_latest_forecast(prediction).set_index("ds")

        # Combine data
        current_weather_data = current_weather_data.rename("temperature").to_frame()
        current_weather_data["model"] = "ECMWF IFS"

        latest_prediction = (
            latest_prediction["origin-0"].rename("temperature").to_frame()
        )
        latest_prediction["model"] = "NeuralProphet long-term"

        combined_forecast = pd.concat([current_weather_data, latest_prediction])
        combined_forecast = combined_forecast.sort_index()

        return combined_forecast


if __name__ == "__main__":
    temperature_forecaster = TemperatureForecaster()

    loaded = temperature_forecaster.load_model()

    if not loaded:
        raise Exception("Model not found!")

    forecast = temperature_forecaster.cobmined_forecast()

    exit()
