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


def read_electricity_prices(
    file: str = "data/raw/European wholesale electricity prices.csv",
) -> pd.Series:
    """Reads historic wholesale electricity prices in Germany in EUR/MWh and returns them as a
    pandas series."""

    electricity_prices_raw = pd.read_csv(file)

    # Filter for Germany
    electricity_prices_germany = electricity_prices_raw.loc[
        electricity_prices_raw["ISO3 Code"] == "DEU"
    ]
    electricity_prices_germany.set_index("Date", inplace=True)
    electricity_prices_germany.index = pd.to_datetime(electricity_prices_germany.index)

    return electricity_prices_germany["Price (EUR/MWhe)"]


def read_eua_auctions(
    files_style_1: list = [
        "data/raw/EEX EUA Auctions/emission-spot-primary-market-auction-report-2012-data.xls",
        "data/raw/EEX EUA Auctions/emission-spot-primary-market-auction-report-2013-data.xls",
        "data/raw/EEX EUA Auctions/emission-spot-primary-market-auction-report-2014-data.xls",
        "data/raw/EEX EUA Auctions/emission-spot-primary-market-auction-report-2015-data.xls",
    ],
    files_style_2: list = [
        "data/raw/EEX EUA Auctions/emission-spot-primary-market-auction-report-2016-data.xls",
    ],
    files_style_3: list = [
        "data/raw/EEX EUA Auctions/emission-spot-primary-market-auction-report-2017-data.xls",
        "data/raw/EEX EUA Auctions/emission-spot-primary-market-auction-report-2018-data.xls",
        "data/raw/EEX EUA Auctions/emission-spot-primary-market-auction-report-2019-data.xls",
        "data/raw/EEX EUA Auctions/emission-spot-primary-market-auction-report-2020-data.xlsx",
        "data/raw/EEX EUA Auctions/emission-spot-primary-market-auction-report-2021-data.xlsx",
        "data/raw/EEX EUA Auctions/emission-spot-primary-market-auction-report-2022-data.xlsx",
        "data/raw/EEX EUA Auctions/primary_auction_report_20230214_39969994.xlsx",
    ],
) -> pd.Series:
    """Reads historic EUA auction prices in EUR/tCO2 and returns them as a pandas series."""

    eua_auctions = pd.Series(dtype="float64")
    for file in files_style_1:
        eua_auctions = pd.concat(
            [
                eua_auctions,
                pd.read_excel(file, header=2, index_col="Date", parse_dates=True)[
                    "Auction Price €/tCO2"
                ],
            ]
        )

    for file in files_style_2:
        eua_auctions = pd.concat(
            [
                eua_auctions,
                pd.read_excel(file, header=2, index_col="Date", parse_dates=True)[
                    "Auction Price EUR/tCO2"
                ],
            ]
        )

    for file in files_style_3:
        eua_auctions = pd.concat(
            [
                eua_auctions,
                pd.read_excel(file, header=5, index_col="Date", parse_dates=True)[
                    "Auction Price €/tCO2"
                ],
            ]
        )

    return eua_auctions


def read_storage_levels(
    file: str = "data/raw/StorageData_GIE_2011-01-01_2023-03-02.csv",
) -> pd.Series:
    """Reads historic storage levels in TWh and returns them as a pandas series."""

    storage_levels = pd.read_csv(
        file, sep=";", index_col="Gas Day Start", parse_dates=True, decimal="."
    )

    return storage_levels["Gas in storage (TWh)"]


def read_consumption(
    file_netconnect: str = "data/raw/NetConnect Germany natural gas consumption.csv",
    file_gaspool: str = "data/raw/GASPOOL natural gas consumption.csv",
    file_the: str = "data/raw/Trading Hub Europe  Publications  Transparency  Aggregated consumption data.csv",
) -> tuple([pd.Series, pd.Series, pd.Series]):
    """Reads historic natural gas consumption in MWh and returns them as a tuple of pandas series."""

    # Read NetConnect Germany CSV file
    ncg_consumption = pd.read_csv(file_netconnect, sep=";", index_col="DayOfUse")

    ncg_consumption.index = pd.to_datetime(ncg_consumption.index, format="%d.%m.%Y")

    # Convert kWh to MWh and aggregate different measurement types
    ncg_consumption = ncg_consumption.select_dtypes("number") / 1000
    ncg_consumption_aggregated = ncg_consumption.sum(axis="columns")

    # Read GASPOOL CSV file
    gaspool_consumption = pd.read_csv(file_gaspool, sep=";", index_col="Datum")

    gaspool_consumption.index = pd.to_datetime(
        gaspool_consumption.index, format="%d.%m.%Y"
    )
    gaspool_consumption_aggregated = gaspool_consumption.sum(axis="columns")

    # Read Trading Hub Europe CSV file
    the_consumption = pd.read_csv(file_the, sep=";", thousands=",", index_col="Gasday")

    the_consumption.index = pd.to_datetime(the_consumption.index, format="%d/%m/%Y")

    # Convert kWh to MWh and aggregate different measurement types
    the_consumption = the_consumption.select_dtypes("number") / 1000
    the_consumption_aggregated = the_consumption.sum(axis="columns")

    return tuple(
        [
            ncg_consumption_aggregated.sort_index(),
            gaspool_consumption_aggregated.sort_index(),
            the_consumption_aggregated.sort_index(),
        ]
    )
