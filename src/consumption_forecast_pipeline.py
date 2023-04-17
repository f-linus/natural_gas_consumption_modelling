import os
import pandas as pd
import pickle as pkl
from src.data_handling import consumption_data
from src.data_handling import temperature_data
from src.models import temperature_forecaster
from src.models import consumption_forecaster


class ConsumptionForecastPipeline:
    def __init__(self, db: dict, verbose=False) -> None:
        self.db = db
        self.verbose = verbose

    def __persist_db(self):
        with open("db.pkl", "wb") as f:
            pkl.dump(self.db, f)

    def __get_newest_consumption_data(self) -> None:

        # Check what the newest consumption data is in the database
        # If there is no consumption data in the database, get the data two years + 1 week back

        if "historic_consumption_data" in self.db:
            newest_consumption_data_date = max(
                self.db["historic_consumption_data"].keys()
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
        if "historic_consumption_data" not in self.db:
            self.db["historic_consumption_data"] = {}

        self.db["historic_consumption_data"].update(new_consumption_data.to_dict())

        # Persist database
        self.__persist_db()

    def __get_newest_temperature_data(self) -> None:

        # Check what the newest temperature data is in the database
        # If there is no temperature data in the database, get the data two years + 1 week back

        if "historic_temperature_data" in self.db:
            newest_temperature_data_date = max(
                self.db["historic_temperature_data"].keys()
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
        if "historic_temperature_data" not in self.db:
            self.db["historic_temperature_data"] = {}

        self.db["historic_temperature_data"].update(new_temperature_data.to_dict())

        # Persist the database
        self.__persist_db()

    def __create_new_temperature_forecast(self) -> None:

        temperature_forecasting_instance = (
            temperature_forecaster.TemperatureForecaster()
        )
        loaded = temperature_forecasting_instance.load_model()

        if not loaded:
            temperature_forecasting_instance.fit()

        forecast = temperature_forecasting_instance.cobmined_forecast()

        if "temperature_forecast" not in self.db:
            self.db["temperature_forecast"] = {}

        self.db["temperature_forecast"].update(forecast.to_dict(orient="index"))

        # Persist database
        self.__persist_db()

    def __create_new_consumption_forecast(self) -> None:

        # Get the last two years of consumption data
        consumption_data = pd.DataFrame.from_dict(
            self.db["historic_consumption_data"], orient="index"
        ).squeeze()

        # Filter for the last two years
        consumption_data = consumption_data[
            consumption_data.index > pd.Timestamp.now() - pd.Timedelta(weeks=104)
        ]

        # Get the last two years of temperature data
        temperature_data = pd.DataFrame.from_dict(
            self.db["historic_temperature_data"], orient="index"
        ).squeeze()

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
            self.db["temperature_forecast"], orient="index"
        )

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
        if "natural_gas_consumption_forecast" not in self.db:
            self.db["natural_gas_consumption_forecast"] = {}

        self.db["natural_gas_consumption_forecast"].update(
            forecasted_gas_consumption.to_dict()
        )

        self.db["last_consumption_forecast"] = pd.Timestamp.now()

        # Persist database
        self.__persist_db()

    def run(self) -> None:
        self.__get_newest_consumption_data()
        self.__get_newest_temperature_data()
        self.__create_new_temperature_forecast()
        self.__create_new_consumption_forecast()


if __name__ == "__main__":
    print("Debugging consumption_forecast_pipeline.py")

    # Check if persisted database exists
    if os.path.exists("db.pkl"):
        db = pkl.load(open("db.pkl", "rb"))
    else:
        db = {}

    pipeline = ConsumptionForecastPipeline(db=db, verbose=True)
    pipeline.run()
    exit()
