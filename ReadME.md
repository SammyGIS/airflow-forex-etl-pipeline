# ETL Pipeline with Airflow and Docker

This project demonstrates the implementation of an ETL (Extract, Transform, Load) pipeline using Apache Airflow and Docker. The pipeline extracts data from a Forex API, processes it, and loads it into a PostgreSQL database.
## Prerequisites

- [Docker](https://www.docker.com/get-started) installed on your machine.
- [Docker Compose](https://docs.docker.com/compose/install/) installed on your machine.
- [Fixer](https://fixer.io/) Obtain an API key from Forex API for accessing the data.

## Setup
1. Clone the repository:

    ```bash
    git clone https://github.com/SammyGISforex-etl-airflow-docker.git
    ```

2. Change into the project directory:

    ```bash
    cd forex-etl-airflow-docker
    ```

3. Build and run the Docker containers:

    ```bash
    docker-compose up -d
    ```

4. Access the Airflow web interface:

    Open your browser and go to [http://localhost:8080](http://localhost:8080). Log in using the default credentials (username: `airflow`, password: `airflow`).

5. Trigger the `forex_pipeline` DAG in the Airflow web interface.

##  Workflow Overview (DAG)

### 1. `is_api_available` - HTTP Sensor

- Task to check if the Forex API is available.
- Uses an HTTP sensor to verify the presence of 'EUR' in the API response.

### 2. `create_table` - Postgres Operator

- Task to create a PostgreSQL table named `rates`.
- Defines columns for the table: `rate` (float) and `symbol` (text).

### 3. `extract_data` - Simple HTTP Operator

- Task to extract data from the Forex API.
- Uses a Simple HTTP Operator to make a GET request and retrieve data.

### 4. `transform_data` - Python Operator

- Task to transform the extracted data.
- Utilizes a Python function to process and normalize the data, saving the result as a CSV file.

### 5. `load_data` - Python Operator

- Task to load the processed data into the PostgreSQL database.
- Uses the Postgres Hook to copy data from the CSV file into the `rates` table.

### Dependencies

- Tasks are linked in the following order: `is_api_available` >> `create_table` >> `extract_data` >> `transform_data` >> `load_data`.

## Resources 
(https://blog.devgenius.io/etl-process-using-airflow-and-docker-226aa5c7a41a) reference tutorial