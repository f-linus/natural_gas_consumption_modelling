from google.cloud import storage
import json
from src import consumption_forecast_pipeline


storage_backend = "google_cloud_storage"

# Connect to Google Cloud Storage
storage_client = storage.Client()
bucket = storage_client.get_bucket("natural_gas_consumption_modelling")

# Check if persisted database exists
if storage.Blob("db", bucket).exists():
    db_json = storage.Blob("db", bucket).download_as_string()
    db = json.loads(db_json)
else:
    db = {}

# Run pipeline
pipeline = consumption_forecast_pipeline.ConsumptionForecastPipeline(
    db=db, storage_backend=storage_backend, verbose=True
)

pipeline.run()
