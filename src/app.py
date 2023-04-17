from flask import Flask
from flask_cors import CORS
import pickle as pkl
import pandas as pd
from src import consumption_forecast_pipeline
import os

app = Flask(__name__)
CORS(app)
db = {}


@app.route("/model_run", methods=["GET"])
def model_run():

    # Check if last model run was at least 23h ago
    if "last_consumption_forecast" in db:
        if pd.Timestamp.now() - pd.to_datetime(
            db["last_consumption_forecast"]
        ) < pd.Timedelta(hours=23):

            # Return error message
            return {
                "status": "error",
                "message": "Model has already run in the last 23h",
            }

    pipeline = consumption_forecast_pipeline.ConsumptionForecastPipeline(
        db=db, verbose=True
    )
    pipeline.run()

    return {"status": "success"}


@app.route("/get", methods=["GET"])
def get():
    # Check if model has been run yet
    if "last_consumption_forecast" not in db:
        return {"status": "error", "message": "Model has not been run yet"}

    # Get data from database and return it
    return db


if __name__ == "__main__":
    # Check if persisted database exists
    if os.path.exists("db.pkl"):
        db = pkl.load(open("db.pkl", "rb"))

    app.run(debug=True, host="localhost", port=int(os.environ.get("PORT", 8080)))
