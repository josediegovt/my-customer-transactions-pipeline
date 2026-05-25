Customer Transactions Pipeline - Ebury

PIPELINE DESCRIPTION

Dockerized data pipeline in charge of ingesting a local .csv file, outputting intermediary and final results onto a local PostgreSQL database. Airflow is used to orchestrate the different stages of the data pipeline, while dbt is used to transform, clean and model the data.

---
DATA QUALITY

Implemented "Quarantine" table at the silver layer:
- Allows clear tracking of why rows were rejected and not appearing downstream
- Allows upstream SHs to review this, correct them and provide correct values
- How they provide this depends on whether our syncing is full or incremental
    - If full: provide full CSV, even with existing records but now with also the cleaned records
    - If incremental: only provide a CSV file with the corrected records

Data cleaning decisions per column:

- transaction_id and product_id:
    decided to "clean" the data for demonstration purposes. however, since this columns would represent primary keys, I would be stricter in production environment and reject the column altogether if strings or nulls are provided.
- quantity, price, tax:
    any string value is quarantined. Mapping "Two Hundred" to its integer or float equivalent is doable but the possibilities of strings here make it impossible to map values. Also, seeing that values are floats, "Two Hundred" is in itself an incomplete and unrealistic value. Nulls are also quarantined, as they have no value in downstream report query aggregations.
- transaction_date:
    leveraged PostgreSQL's paradigm of outputting different date formats into the YYYY-MM-DD format. Check if it is a valid date, if not then quarantined.
- product_name:
    although "Product" seems to be unnecessary and only memory consuming, we can keep it as we are normalizing the customer_transactions table by creating a dedicated product dimensions table. In there, I decide to take the latest name of a product, in case we see a duplicate product_id with different product_names.


---
USEFUL docker COMMANDS

First time docker commands:
docker compose up --build airflow-init && docker compose up -d

Check that everything is correct:
docker compose ps

Open interactive psql session:
docker compose exec postgres psql -U airflow -d airflow

Full database wipe:
docker compose down -v
OR
Stop containers while keeping data in Postgres:
docker compose down

Full cleanup:
docker compose down -v --rmi local
