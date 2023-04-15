import json
import os
import pandas as pd
from src.data_handling import consumption_data
from src.data_handling import temperature_data


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
            newest_consumption_data_date = pd.Timestamp.now() - pd.Timedelta(
                weeks=104
            )

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
        new_consumption_data = consumption_data.get_consumption_data(start_date=newest_consumption_data_date + pd.Timedelta(days=1))

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
            newest_temperature_data_date = pd.Timestamp.now() - pd.Timedelta(
                weeks=104
            )

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
        new_temperature_data = temperature_data.get_temperature_data(start_date=newest_temperature_data_date + pd.Timedelta(days=1), end_date=pd.Timestamp.now())

        # Update the database
        if "historic_temperature_data" not in db:
            db["historic_temperature_data"] = {}

        new_temperature_data.index = new_temperature_data.index.strftime("%Y-%m-%d")
        db["historic_temperature_data"].update(new_temperature_data.to_dict())

        # Write the database to db.json
        self.__write_json_db(db)


    def run(self) -> None:
        self.__get_newest_consumption_data()
        self.__get_newest_temperature_data()
        # self.__create_new_temperature_forecast()
        # self.__create_new_consumption_forecast()


if __name__ == "__main__":
    print("Debugging consumption_forecast_pipeline.py")
    pipeline = ConsumptionForecastPipeline(storage_backend="json", verbose=True)
    pipeline.run()
    exit()