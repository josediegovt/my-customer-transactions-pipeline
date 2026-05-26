Customer Transactions Pipeline - Ebury

PIPELINE DESCRIPTION

Dockerized data pipeline in charge of ingesting a local .csv file, outputting intermediary and final results onto a local PostgreSQL database. Airflow is used to orchestrate the different stages of the data pipeline, while dbt is used to transform, clean and model the data.

---
DATA QUALITY

Implemented "Quarantine" table at the silver layer:
- Allows clear tracking of why rows were rejected and not appearing downstream
- Allows upstream SHs to review this, correct them and provide correct values
- How they provide this depends on whether our syncing is full or incremental
    - With the current design, the "corrected" file can include only the adjusted records or adjusted records + new and already existing records.
    - The reason for this is that the gold layer is incremental merge, meaning that it is idempotent. If it sees already existing transaction_ids, it makes sure to "update" any values that have changed. If nothing new, row stays the same.

Data cleaning decisions per column:

- transaction_id and product_id:
    Decided to "clean" the data for demonstration purposes. However, since these columns would represent primary keys, I would be stricter in production environment and reject the column altogether if strings or nulls are provided.
- quantity, price, tax:
    Any string value is quarantined. Mapping "Two Hundred" to its integer or float equivalent is doable but the possibilities of strings here make it impossible to map values. Also, seeing that values are floats, "Two Hundred" is in itself an incomplete and suspicious value. Nulls are also quarantined, as they have no value in downstream report query aggregations.
- transaction_date:
    Leveraged PostgreSQL's paradigm of outputting different date formats into the YYYY-MM-DD format. Check if it is a valid date, if not then quarantined.
- product_name:
    although "Product" seems to be unnecessary and only memory consuming, we can keep it as we are normalizing the customer_transactions table by creating a dedicated product dimensions table. In there, I decide to take the latest name of a product, in case we see a duplicate product_id with different product_names.
---
TESTS

I decided to keep the test_silver_count_equals_bronze.sql file in order to discuss what I intended to do with it even though its behaviour is not what I expected.

My intention was to have this test keep the gold layer from materializing if it failed, but it is not working like that.

In reality, maybe the DAG file would have to decouple the dbt build to specific dbt run/test calls. I decided not to implement this for the sake of clarity.

---
NOTIFICATION

For the sake of clarity, I did not fully write the email template and logic that would send the email. That is beyond necessary I believe. However, I did make the check and notification as part of the DAG.

---
USEFUL docker COMMANDS

To start project:
1) Remove .example from profile.yml.example
2) Remove .example from .env.example
3) Run docker compose up --build airflow-init && docker compose up -d

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
