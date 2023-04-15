import requests
import pandas as pd

endpoint = "https://archive-api.open-meteo.com/v1/archive"
cities = [
    (52.520008, 13.404954),  # Berlin
    (51.339695, 12.373075),  # Dresden
    (50.937531, 6.960279),  # Cologne
    (48.135125, 11.581981),  # Munich
    (53.551086, 9.993682),  # Hamburg
]


def get_temperature_data(
    start_date: pd.DatetimeIndex = None, end_date: pd.DatetimeIndex = None
) -> pd.Series:
    """Get temperature data from the Open-Meteo API (ERRA5-Land model) and return it as a Pandas
    Series.
    """

    # Get temperature data for all cities (each city is one column in the resulting dataframe)
    hourly_temperatures = pd.DataFrame()

    for city in cities:
        params = {
            "latitude": city[0],
            "longitude": city[1],
            "hourly": "temperature_2m",
            "models": "era5_land",
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d") 
        }
        response = requests.get(endpoint, params=params)
        data = response.json()

        temps = pd.Series(
            data["hourly"]["temperature_2m"], index=data["hourly"]["time"]
        )
        hourly_temperatures[city] = temps
    
    # Average over all cities
    hourly_temperatures = hourly_temperatures.mean(axis="columns")

    # Make index a datetime object
    hourly_temperatures.index = pd.to_datetime(hourly_temperatures.index)

    # Group hourly temperatures by day
    daily_temperatures = hourly_temperatures.groupby(pd.Grouper(freq="D")).mean()

    return daily_temperatures.dropna()


if __name__ == "__main__":
    print("Debugging realtime_consumption_data.py")

    data = get_temperature_data(pd.Timestamp("2021-01-01"), pd.Timestamp("2022-01-02"))
    print(data)
    exit()
