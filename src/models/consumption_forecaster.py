import neuralprophet as nprophet
import pandas as pd

class ConsumptionForecaster:
    def __init__(self) -> None:
        pass

    def train(self, historic_consumption: pd.Series, historic_temperatures: pd.Series) -> None:

        # Cut consumption and temperature data to there index intersection
        historic_consumption = historic_consumption.loc[
            historic_consumption.index.intersection(historic_temperatures.index)
        ]

        historic_temperatures = historic_temperatures.loc[
            historic_temperatures.index.intersection(historic_consumption.index)
        ]

        # Cap the temperatures at 18 degrees
        temperatures_capped = historic_temperatures.apply(lambda x: min(18.0, x))

        # Create a dataframe with the consumption data and the temperature data
        train_df = pd.DataFrame(
            {
                "ds": historic_consumption.index,
                "y": historic_consumption.values,
                "temperature_capped": temperatures_capped.values,
            }
        )

        # Add weekend column
        train_df["weekend"] = (historic_consumption.index.weekday > 4).astype(float)

        # Create model
        self.model = nprophet.NeuralProphet(n_forecasts=365, n_lags=15)
        self.model.add_future_regressor("temperature_capped", mode="additive")
        self.model.add_future_regressor("weekend", mode="additive")
        self.model.add_country_holidays(country_name="DE")

        # Fit model
        return self.model.fit(train_df, epochs=400, progress="bar")