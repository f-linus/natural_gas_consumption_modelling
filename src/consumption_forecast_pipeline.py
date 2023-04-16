import json
import os
import pandas as pd
from src.data_handling import consumption_data
from src.data_handling import temperature_data
from src.models import temperature_forecaster
from src.models import consumption_forecaster


class ConsumptionForecastPipeline:
    def __init__(self, storage_backend: str = "firestore", verbose=False) -> None:
        self.storage_backend = storage_backend
        self.verbose = verbose

    def __get_json_db(self) -> None:

        # Check if the database exists
        # If not, return empty dict
        if not os.path.exists("db.json"):
            return {}

        # Get the JSON database from db.json
        with open("db.json", "r") as f:
            return json.load(f)

    def __write_json_db(self, db: dict) -> None:

        # Write the JSON database to db.json
        with open("db.json", "w") as f:
            json.dump(db, f, indent=4)

    def __get_newest_consumption_data(self) -> None:

        # Check what the newest consumption data is in the database
        # If there is no consumption data in the database, get the data two years + 1 week back

        db = self.__get_json_db()

        if "historic_consumption_data" in db:
            newest_consumption_data_date = pd.to_datetime(
                max(db["historic_consumption_data"].keys())
            )
        else:
            # If no consumption data is in the database, get the data two years + 1 week back
            newest_consumption_data_date = pd.Timestamp.now() - pd.Timedelta(weeks=104)

        # Calculate how many days of data we need to get (the -1 because we will never have todays value)
        days_to_get = (pd.Timestamp.now() - newest_consumption_data_date).days - 1

        if self.verbose:
            print(
                f"Getting {days_to_get} days of consumption data starting from {newest_consumption_data_date + pd.Timedelta(days=1)}"
            )

        # if newest_consumption date is today, do nothing
        if days_to_get == 0:
            return

        # Get the newest consumption data
        new_consumption_data = consumption_data.get_consumption_data(
            start_date=newest_consumption_data_date + pd.Timedelta(days=1)
        )

        # Update the database
        if "historic_consumption_data" not in db:
            db["historic_consumption_data"] = {}

        new_consumption_data.index = new_consumption_data.index.strftime("%Y-%m-%d")
        db["historic_consumption_data"].update(new_consumption_data.to_dict())

        # Write the database to db.json
        self.__write_json_db(db)

    def __get_newest_temperature_data(self) -> None:

        # Check what the newest temperature data is in the database
        # If there is no temperature data in the database, get the data two years + 1 week back

        db = self.__get_json_db()

        if "historic_temperature_data" in db:
            newest_temperature_data_date = pd.to_datetime(
                max(db["historic_temperature_data"].keys())
            )
        else:
            # If no temperature data is in the database, get the data two years + 1 week back
            newest_temperature_data_date = pd.Timestamp.now() - pd.Timedelta(weeks=104)

        # Calculate how many days of data we need to get (the -1 because we will never have todays value)
        days_to_get = (pd.Timestamp.now() - newest_temperature_data_date).days - 1

        if self.verbose:
            print(
                f"Getting {days_to_get} days of temperature data starting from {newest_temperature_data_date + pd.Timedelta(days=1)}"
            )

        # if newest_temperature date is today, do nothing
        if days_to_get == 0:
            return

        # Get the newest temperature data
        new_temperature_data = temperature_data.get_temperature_data(
            start_date=newest_temperature_data_date + pd.Timedelta(days=1),
            end_date=pd.Timestamp.now(),
        )

        # Update the database
        if "historic_temperature_data" not in db:
            db["historic_temperature_data"] = {}

        new_temperature_data.index = new_temperature_data.index.strftime("%Y-%m-%d")
        db["historic_temperature_data"].update(new_temperature_data.to_dict())

        # Write the database to db.json
        self.__write_json_db(db)

    def __create_new_temperature_forecast(self) -> None:

        temperature_forecasting_instance = (
            temperature_forecaster.TemperatureForecaster()
        )
        loaded = temperature_forecasting_instance.load_model()

        if not loaded:
            temperature_forecasting_instance.fit()

        forecast = temperature_forecasting_instance.cobmined_forecast()

        # Update the database
        db = self.__get_json_db()

        if "temperature_forecast" not in db:
            db["temperature_forecast"] = {}

        forecast.index = forecast.index.strftime("%Y-%m-%d")
        db["temperature_forecast"].update(forecast.to_dict(orient="index"))

        # Write the database to db.json
        self.__write_json_db(db)

    def __create_new_consumption_forecast(self) -> None:

        # Get the last two years of consumption data
        db = self.__get_json_db()
        consumption_data = pd.DataFrame.from_dict(
            db["historic_consumption_data"], orient="index"
        ).squeeze()

        # Index to datetime
        consumption_data.index = pd.to_datetime(consumption_data.index)

        # Filter for the last two years
        consumption_data = consumption_data[
            consumption_data.index > pd.Timestamp.now() - pd.Timedelta(weeks=104)
        ]

        # Get the last two years of temperature data
        temperature_data = pd.DataFrame.from_dict(
            db["historic_temperature_data"], orient="index"
        ).squeeze()

        # Index to datetime
        temperature_data.index = pd.to_datetime(temperature_data.index)

        # Filter for the last two years
        temperature_data = temperature_data[
            temperature_data.index > pd.Timestamp.now() - pd.Timedelta(weeks=104)
        ]

        # Create the consumption forecasting instance
        consumption_forecasting_instance = (
            consumption_forecaster.ConsumptionForecaster()
        )

        # Train the model
        consumption_forecasting_instance.train(
            historic_consumption=consumption_data,
            historic_temperatures=temperature_data,
        )

        # Get temperature values for the 365 days after the training period
        forecast_horizon_start = min(
            [consumption_data.index.max(), temperature_data.index.max()]
        ) + pd.Timedelta(days=1)

        forecasted_temperatures = pd.DataFrame.from_dict(
            db["temperature_forecast"], orient="index"
        )
        forecasted_temperatures.index = pd.to_datetime(
            forecasted_temperatures.index
        )  # Index from string to datetime

        # Use actual historic temperatures if possible (will only be possible if consumption data lags behind)
        historic_temperatures_in_forecast_horizon = temperature_data[
            temperature_data.index >= forecast_horizon_start
        ]

        if historic_temperatures_in_forecast_horizon.empty:
            forecasted_temperatures_used_from = forecast_horizon_start
        else:
            forecasted_temperatures_used_from = (
                historic_temperatures_in_forecast_horizon.index.max()
                + pd.Timedelta(days=1)
            )

        forecast_horizon_temperatures = pd.concat(
            [
                historic_temperatures_in_forecast_horizon,
                forecasted_temperatures.loc[
                    forecasted_temperatures.index >= forecasted_temperatures_used_from,
                    "temperature",
                ],
            ]
        )

        # Limit to 365day time horizon
        forecast_horizon_temperatures = forecast_horizon_temperatures.iloc[:365]

        # Forecast natural gas consumption
        forecasted_gas_consumption = consumption_forecasting_instance.forecast(
            forecast_horizon_temperatures
        )

        # Save forecasted natural gas consumption in database
        if "natural_gas_consumption_forecast" not in db:
            db["natural_gas_consumption_forecast"] = {}

        forecasted_gas_consumption.index = forecasted_gas_consumption.index.strftime(
            "%Y-%m-%d"
        )
        db["natural_gas_consumption_forecast"].update(
            forecasted_gas_consumption.to_dict()
        )
        self.__write_json_db(db)

    def run(self) -> None:
        self.__get_newest_consumption_data()
        self.__get_newest_temperature_data()
        self.__create_new_temperature_forecast()
        self.__create_new_consumption_forecast()


if __name__ == "__main__":
    print("Debugging consumption_forecast_pipeline.py")
    pipeline = ConsumptionForecastPipeline(storage_backend="json", verbose=True)
    pipeline.run()
    exit()
