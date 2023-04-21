
# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.10-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME

# Copy all files from src to the container image.
COPY ./models/temperature_model.pkl ./models/temperature_model.pkl
COPY ./src/ ./src/
COPY ./requirements.txt ./requirements.txt

# Install production dependencies.
RUN pip install --no-cache-dir -r requirements.txt

# Entrypoint
CMD ["python", "m", "src.daily_model_run"]