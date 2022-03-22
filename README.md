# Solution for Alkemy Challenge - Data Analytics - Python

## Introduction

This repository consists in a solution for the challenge required for applying to [Alkemy](https://www.alkemy.org/) *Data Analytics - Python* acceleration program.

The project consumes 3 datasets from [datos.gob.ar](https://datos.gob.ar) CKAN API with information of libraries, museums and cinemas from Argentina, saves them into csv files, and after normalizing them, populates a PostgreSQL database with 3 different tables:

- **Sitios:** A merge of the 3 datasets with selected columns formerly normalized.
- **Totales:** A table with unique value counts of selected columns from the original data: category, source, and province and category merged.
- **Cines:** A table with the sum of cinemas, screen, seats and registered INCAA spaces, aggregated by province.

## Requirements

- Python 3 (tested on Python 3.8.5)
- Git (tested on Git 2.31.1.windows.1)
- PostgreSQL (tested on PostgreSQL 14.1)
- PgAdmin 4 (optional)

## Installation (Windows 10)

Open CMD and execute the following commands:

1. Clone this repository into desired directory

    ```codetype
    git clone https://github.com/facundofacio/alkemy-python-challenge-solution
    ```

2. Go to project directory

    ```codetype
    cd alkemy-python-challenge-solution
    ```

3. Create virtual environment

    ```codetype
    python -m venv env
    ```

4. Activate virtual environment

    ```codetype
    env\Scripts\activate
    ```

5. Install dependencies

    ```codetype
    pip install -r requirements.txt
    ```

6. Configure SQLalchemy database settings

    Edit lines 17-21 in **\components\settings.ini** file as needed:

    ```codetype
    USER = postgres
    PASS = admin
    DB_HOST = localhost
    PORT = 5432
    DB_NAME = cultura
    ```

    - **USER:** PostgreSQL user
    - **PASS:** PostgreSQL password
    - **DB_HOST:** Database host ip (Default localhost)
    - **PORT:** Database port (Default 5432)
    - **DB_NAME:** Database name to be created and populated with downloaded data.

## Running

In CMD, go to project directory and execute script.py

```codetype
python script.py
```
