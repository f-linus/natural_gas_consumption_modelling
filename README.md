# Natural Gas Consumption Modelling

Germany's dependence on natural gas highlights the need for accurate forecasting methods. Despite advances, many traditional forecasting models lack reproducibility due to limited data access. This project addresses this gap by providing open-source, publicly available forecasts of Germany's natural gas consumption. Our focus is on using publicly available data, using state-of-the-art models for time-efficient forecast implementation, and ensuring continuous publication and producibility of these forecasts.

This repository is dedicated to the backend of this project, hosting the forecast pipeline and data analysis notebooks.

## Setup

### 1. Clone the repository

```bash
git clone git@github.com:f-linus/natural_gas_consumption_modelling.git
cd natural_gas_consumption_modelling
```

### 2. Python version

This project requires a Python version of `3.9` or `3.10`. To install Python, follow the instructions on the [Python website](https://www.python.org/downloads/).

**pyenv** is a convenient tool to manage multiple Python versions on your machine. To install **pyenv**, follow the instructions on the [pyenv GitHub page](https://github.com/pyenv/pyenv). (There is also a fork for Windows available [here](https://pyenv-win.github.io/pyenv-win/))

### 3. Dependency management: Poetry

This project uses [Poetry](https://python-poetry.org/) for dependency management. To install Poetry, follow the instructions on the [Poetry website](https://python-poetry.org/docs/#installation).

To install the project dependencies, run

```bash
poetry install
```

This will create a virtual environment and install all dependencies necessary and specified in `pyproject.toml`.

The environment can be activated by running

```bash
poetry shell
```

or when using **VS Code** by selecting the environment.

Installing the dependencies allows you to run all Notebooks in the notebooks folder which cover
the Data Analysis part of the project.

To run the full forecasting pipeline a **Google Cloud Storage** backend is required.

### 4. Google Cloud Storage

The project uses **Google Cloud Storage** as a backend for the forecasting pipeline. To use the
pipeline you need to create a **Google Cloud Storage** bucket.

#### 4.1: Create a Google Cloud Platform (GCP) account

To create a **Google Cloud Storage** bucket you need a **Google Cloud Platform** account. You can
create one [here](https://cloud.google.com/).

#### 4.2: Create or select a project

First, create a new project or select an existing one. You can do this by following the instructions
[here](https://cloud.google.com/resource-manager/docs/creating-managing-projects).

#### 4.2: Activate the Google Cloud Storage API

To use the **Google Cloud Storage** API you need to activate it. You can do this by following the
instructions [here](https://cloud.google.com/storage/docs/quickstart-console).

#### 4.3: Create a bucket

To create a bucket follow the instructions [here](https://cloud.google.com/storage/docs/creating-buckets).

Name the bucket `natural-gas-consumption-modelling` and select a region close to you.

#### 4.4: Connecting locally to GCP

To use the storage backend from a local version of the pipeline you need to connect to GCP. You can
do this by using the `gcloud` command line tool. To install `gcloud` follow the instructions
[here](https://cloud.google.com/sdk/docs/install).

If not already done, initialise `gcloud` by running

```bash
gcloud init
```

and follow the instructions.

This will allow all GCP python dependencies to connect to GCP and to use the storage backend.

## Usage

The repository encompasses both Notebooks that cover Data Analysis and Model discussion as well as
the full forecasting pipeline in the `src` folder.

### Notebooks

The Notebooks are located in the `notebooks` folder. Running them requires the dependencies to be
installed as described in the Setup section. However, the Notebooks **do not** require a GCP connection.

The Notebooks can be run natively in **Jupyter** or **JupyterLab** or in **VS Code**.

To run through **Jupyter** in browser, run

```bash
cd notebooks
poetry shell
jupyter notebook
```

From the browser, notebooks in all subfolders `data_overview`, `data_analysis` and `modelling` can
be run.

### Forecasting Pipeline

The forecasting pipeline is located in the `src` folder. It can be run locally or deployed to GCP.
It requires the dependencies to be installed as described in the Setup section and a GCP connection
as described in the GCP section.

The pipeline can be run locally by running

```bash
poetry shell
python -m src.daily_model_run
```

## Deploying a new pipeline version to GCP

For the example frontend at https://linusfolkerts.com, the pipeline is deployed on GCP.

The model is run daily as a **Cloud Run** Job, triggered through **Cloud Scheduler**. From there,
the model stores the results in a **Cloud Storage** bucket.

### 1. Activate Google Cloud Run

To use **Cloud Run** you need to activate it. You can do this by following the instructions
[here](https://cloud.google.com/run/docs/quickstarts/prebuilt-deploy).

### 2. Activate Google Cloud Build

To use **Cloud Build** you need to activate it. You can do this by following the instructions
[here](https://cloud.google.com/build/docs/quickstart-build).

### 3. Connect repository to Google Cloud Build

To connect the repository to **Cloud Build** follow the instructions
[here](https://cloud.google.com/build/docs/automating-builds/create-github-app-triggers).

Here, you select this or your forked repository and create a new Cloud Build trigger. This will build
a new Docker image of the pipeline whenever a new commit is pushed to the repository.

### 4. Create a Cloud Run Job

To create a new **Cloud Run** Job, follow the instructions
[here](https://cloud.google.com/run/docs/quickstarts/prebuilt-deploy).

Here, you select the Docker image created in the previous step to create a new **Cloud Run** Job that
executes the whole forecasting pipeline and stores the results in a **Cloud Storage** bucket.

### 5. Create a Cloud Scheduler Job

To trigger the **Cloud Run** Job daily, you can create a **Cloud Scheduler** Job. Follow the
instructions [here](https://cloud.google.com/scheduler/docs/quickstart).

Here, you select the **Cloud Run** Job created in the previous step and set the frequency to daily.
For specific instructions on how to trigger the created job, see [here](https://cloud.google.com/run/docs/execute/jobs-on-schedule).

### 6. Make the results bucket public

Now the pipeline is deployed and will run daily. Results are stored in the **Cloud Storage** bucket
and can be accessed through the **Cloud Storage** console.

To make them accessible for a frontend, you can make the bucket public. To do this, follow the
instructions [here](https://cloud.google.com/storage/docs/access-control/making-data-public).

An example frontend is provided at https://linusfolkerts.com ([GitHub](https://github.com/f-linus/natural_gas_consumption_modelling_frontend)).

## Data

Historical data is already partly included in the repository. Data is provided through following sources.

### Weather

#### Historic daily mean temperature in Germany

Copernicus Climate Change Service (2020): Climate and energy indicators for Europe from 1979 to present derived from reanalysis. Copernicus Climate Change Service (C3S) Climate Data Store (CDS). 10.24381/cds.4bd77450 (Accessed on 02-Mar-2023)

https://cds.climate.copernicus.eu/cdsapp#!/dataset/sis-energy-derived-reanalysis

Open-Meteo: Historical Weather API

https://open-meteo.com/

License: https://open-meteo.com/en/features#terms

#### Current weather and current weather forecasts

Open-Meteo: Free Weather API

https://open-meteo.com/

License: https://open-meteo.com/en/features#terms

### Energy commodity prices

#### Natural gas

##### Imbalance prices for the Trading Hub Europe market area

Trading Hub Europe GmbH (2023): Imbalance prices. Format: CSV (Accessed on 03-Mar-2023)

https://www.tradinghub.eu/en-gb/Publications/Prices/Imbalance-Prices.

##### Imbalance prices for the historic GASPOOL market area

Trading Hub Europe GmbH (2023): Archive of publications for the former GASPOOL market area. Prices for compensation energy. Format: CSV (Accessed on 03-Mar-2023)

https://www.tradinghub.eu/en-gb/Download/Archive-GASPOOL#1301100-prices-fees-and-charges

##### Imbalance prices for the historic NetConnect Germany market area

Trading Hub Europe GmbH (2023): Archive of publications for the former NCG market area. Imbalance prices according to GaBi Gas 2.0. Format: XML (Accessed on 03-Mar-2023)

https://www.tradinghub.eu/en-gb/Download/Archive-NetConnect-Germany#1306111-conversion

#### Crude oil

U.S. Energy Information Administration, Crude Oil Prices: Brent - Europe [DCOILBRENTEU], retrieved from FRED, Federal Reserve Bank of St. Louis; March 1, 2023.

https://fred.stlouisfed.org/series/DCOILBRENTEU

#### Electricity

Ember (2023): European wholesale electricity price data. Wholesale day-ahead electricity price data for European countries, sources from ENTSO-e and cleaned. Format: CSV (Accessed 03-Mar-2023)

https://ember-climate.org/data-catalogue/european-wholesale-electricity-price-data/

### Emission allowance prices

#### Auction results for European Emission Allowances (EUAs) at the European Energy Exchange (EEX)

European Energy Exchange AG (2023): EUA Emission Spot Primary Market Auction Report - History. Format: XLS/XLSX (Accessed 04-Mar-2023)

https://www.eex.com/en/market-data/environmental-markets/eua-primary-auction-spot-download

### Natural gas storage levels in Germany

GIE (Gas Infrastructure Europe): GIE AISBL 2022. Aggregated Gas Storage Inventory (AGSI): Germany. Format: CSV (Accessed 03-Mar-2023)

https://agsi.gie.eu/data-overview/DE

### Natural gas consumption in Germany

#### Daily natural gas consumption in the Trading Hub Europe market area

Trading Hub Europe GmbH (2023): Publication of the aggregate consumption data: Aggregated consumption data. Format: CSV (Accessed on 04-Mar-2023)

https://www.tradinghub.eu/en-gb/Publications/Transparency/Aggregated-consumption-data

#### Daily natural gas consumption in the historic GASPOOL market area

Trading Hub Europe GmbH (2023): Archive of publications for the former GASPOOL market area. Other: Aggregated Consumption Date Market Area GASPOOL (CSV File). Format: CSV (Accessed on 04-Mar-2023)

https://www.tradinghub.eu/en-gb/Download/Archive-GASPOOL#1301161-other

#### Daily natural gas consumption in the historic NetConnect Germany market area

Trading Hub Europe GmbH (2023): Archive of publications for the former NCG market area. Other: Aggregated consumption data (CSV File). Format: CSV (Accessed on 04-Mar-2023)

https://www.tradinghub.eu/en-gb/Download/Archive-NetConnect-Germany#1306157-other
