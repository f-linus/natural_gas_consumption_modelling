import pandas as pd

class GasConsumptionForecaster:
    def __init__(self, historic_data: pd.DataFrame, forecasted_temperature: pd.DataFrame) -> None:
        self.historic_data = historic_data
        self.forecasted_temperature = forecasted_temperature

    def fit(self) -> None:
        pass

    def __prepare_prediction(self) -> None:
        pass

    def predict(self) -> pd.DataFrame:
        return None
    

if __name__ == "__main__":    
    pass