import neuralprophet as nprophet
import pandas as pd


class ConsumptionForecaster:
    def __init__(self) -> None:
        self.train_df = None
        self.model = None

    def train(
        self, historic_consumption: pd.Series, historic_temperatures: pd.Series
    ) -> None:

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
        self.train_df = pd.DataFrame(
            {
                "ds": historic_consumption.index,
                "y": historic_consumption.values,
                "temperature_capped": temperatures_capped.values,
            }
        )

        # Add weekend column
        self.train_df["weekend"] = (historic_consumption.index.weekday > 4).astype(
            float
        )

        # Create model
        self.model = nprophet.NeuralProphet(n_forecasts=365, n_lags=15)
        self.model.add_future_regressor("temperature_capped", mode="additive")
        self.model.add_future_regressor("weekend", mode="additive")
        self.model.add_country_holidays(country_name="DE")

        # Fit model
        return self.model.fit(self.train_df, epochs=400, progress="bar")

    def forecast(self, temperature_forecast: pd.Series) -> pd.Series:

        # Cap temperature at 18.0 degrees
        temperature_forecast = temperature_forecast.apply(lambda x: min(18.0, x))

        future_regressors_df = temperature_forecast.rename("temperature_capped").to_frame()
        future_regressors_df.index.name = "ds"
        future_regressors_df.reset_index()

        # Add weekend column to future regressors dataframe
        future_regressors_df["weekend"] = (
            temperature_forecast.index.weekday > 4
        ).astype(float)

        future = self.model.make_future_dataframe(
            self.train_df,
            periods=365,
            regressors_df=future_regressors_df,
        )
        prediction = self.model.predict(future)

        forecast = self.model.get_latest_forecast(prediction)
        forecast = forecast.set_index("ds")["origin-0"].rename("y")

        return forecast
