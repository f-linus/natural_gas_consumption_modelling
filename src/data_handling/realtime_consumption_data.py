import requests
import pandas as pd

endpoint = "https://datenservice.tradinghub.eu/XmlInterface/getXML.ashx?ReportId=AggregatedConsumptionData"


def get_consumption_data(
    start_date: pd.DatetimeIndex = None, end_date: pd.DatetimeIndex = None
) -> pd.Series:
    """Get consumption data from the API and return it as a Pandas Series."""

    # Download the data from the Trading Hub Europe endpoint
    response = requests.get(endpoint)

    # Check if the request was successful
    if response.status_code != 200:
        print("Error: Request to API failed.")
        return None

    # Parse the XML file into a Pandas Dataframe
    xml = response.content
    consumption = pd.read_xml(xml)

    # Filter the data to the required columns
    required_columns = [
        "Gasday",
        "HGasSLPsyn",
        "HGasSLPana",
        "LGasSLPsyn",
        "LGasSLPana",
        "HGasRLMmT",
        "LGasRLMmT",
        "HGasRLMoT",
        "LGasRLMoT",
    ]

    consumption = consumption[required_columns]

    # Drop na rows
    consumption = consumption.dropna()

    # Convert the Gasday column to a datetime object
    consumption["Gasday"] = pd.to_datetime(consumption["Gasday"], format="%Y-%m-%d")

    # Set the Gasday column as the index
    consumption = consumption.set_index("Gasday")

    # Filter the data to the required time period
    if start_date is not None:
        consumption = consumption[start_date:]
    
    if end_date is not None:
        consumption = consumption[:end_date]

    # Sum over columns
    consumption_aggregated = consumption.sum(axis="columns").rename("consumption_mwh")

    # Convert kWh to MWh
    consumption_aggregated = consumption_aggregated / 1000

    consumption_aggregated.index.name = "date"

    return consumption_aggregated


if __name__ == "__main__":
    print("Debugging realtime_consumption_data.py")

    data = get_consumption_data(pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02"))
    print(data)
    exit()
