import pandas as pd


def read_temperatures(
    file: str = "data/raw/H_ERA5_ECMW_T639_TA-_0002m_Euro_NUT0_S197901010000_E202212312300_INS_TIM_01d_NA-_noc_org_NA_NA---_NA---_NA---.csv",
) -> pd.Series:
    """Reads ERA5 temperature data in degree Celsius from a CSV file and returns a pandas dataframe."""

    copernicus_reanalysis = pd.read_csv(
        file,
        header=52,
        index_col=0,
        parse_dates=True,
    )

    # Convert Kelvin to Celsius
    return copernicus_reanalysis["DE"] - 273.15


def read_imbalance_prices(
    netconnect_file: str = "data/raw/NetConnect Germany imbalance prices.xml",
    gaspool_file: str = "data/raw/GASPOOL imbalance prices.csv",
    the_file: str = "data/raw/Trading Hub Europe imbalance prices.csv",
) -> tuple([pd.Series, pd.Series, pd.Series]):
    """Reads NetConnect, GASPOOL and THE imbalance prices in EUR/MWh and returns them as a tuple of
    pandas series."""

    # Read NetConnect XML file
    netconnect_data_raw = pd.read_xml(netconnect_file)
    netconnect_data = netconnect_data_raw.loc[
        netconnect_data_raw["Gasday"].notna()
    ].set_index("Gasday")[1:]

    netconnect_data.index = pd.to_datetime(netconnect_data.index)
    netconnect_data.index.name = "Date"

    # Read GASPOOL CSV file
    gaspool_data_raw = pd.read_csv(gaspool_file, sep=";", index_col="Date")

    gaspool_data = gaspool_data_raw[84:]
    gaspool_data.index = pd.to_datetime(gaspool_data.index, format="%d.%m.%Y")

    # Note that the column name is not correct, the price is actually in Euro/MWh
    gaspool_data = gaspool_data[
        "Price for pos. compensation energy [Eurocent/kWh]"
    ].astype(float)

    # Read Trading Hub Europe CSV file
    the_data_raw = pd.read_csv(the_file, sep=";", index_col="Gastag", decimal=",")

    the_data = the_data_raw
    the_data.index.name = "Date"
    the_data.index = pd.to_datetime(the_data.index, format="%d.%m.%Y")

    return tuple(
        [
            netconnect_data["NegativeEnergyImbalanceFee"],
            gaspool_data,
            the_data["Positiver Ausgleichsenergiepreis (EUR/MWh)"],
        ]
    )


def read_crude_oil_prices(
    file: str = "data/raw/Crude oil prices Brent - Europe.csv",
) -> pd.Series:
    """Reads historic BRENT - EUROPE crude oil prices in USD/bbl and returns them as a pandas
    series."""

    crude_oil_prices = pd.read_csv(
        file, parse_dates=["DATE"], index_col="DATE", decimal="."
    )

    crude_oil_prices.index.name = "Date"

    # Convert values to float if possible, otherwise to nan
    crude_oil_prices = crude_oil_prices.applymap(
        lambda x: float(x) if x.replace(".", "", 1).isdigit() else float("nan")
    )

    return crude_oil_prices["DCOILBRENTEU"]
