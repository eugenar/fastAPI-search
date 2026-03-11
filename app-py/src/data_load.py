import os
from typing import Final

import psycopg2
from psycopg2 import sql
from .db import DB_CONFIG

DATA_DIR: Final[str] = "data"
FOOD_DATA_TYPE: Final[str] = "foundation_food"
DELETE_QUERY: Final[str] = "DELETE from food;" # activate delete trigger for pgsync (TRUNCATE does NOT activate delete triggers)
TRUNCATE_QUERY: Final[str] = "TRUNCATE TABLE food_nutrient;"
TRUNCATE_STAGING_QUERY: Final[str] = "TRUNCATE TABLE food_staging, nutrient_staging, food_nutrient_staging;"

file_table_mapping = {
    "food.csv": "food_staging",
    "nutrient.csv": "nutrient_staging",
    "food_nutrient.csv": "food_nutrient_staging",
}

nutrient_name_type_mapping = {
    "Energy (Atwater Specific Factors)": "calories",
    "Protein": "protein",
    "Total lipid (fat)": "fat",
    "Carbohydrate, by difference": "carbs",
}


async def load_csv_files():
    """Load data from CSV files into the database staging tables and process it."""
    for csv_file, table_name in file_table_mapping.items():
        csv_file_path = os.path.join(DATA_DIR, csv_file)
        print(f"Loading {csv_file_path} into {table_name}...")
        await load_csv_table(table_name, csv_file_path)

    await process_data()


async def load_csv_table(table_name, csv_file_path):
    """Load a single CSV file into the specified database table."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position;
        """
            )
            columns = [row[0] for row in cur.fetchall()]
            force_null_clause = ", ".join(columns)

            with open(csv_file_path, "r") as f:
                query = f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',', NULL '', FORCE_NULL ({force_null_clause}))"
                cur.copy_expert(query, f)


async def process_data():
    """Process data from staging tables and insert it into the main tables."""
    print("Processing data...")

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(DELETE_QUERY)  # Clear existing data before loading new data
            cur.execute(TRUNCATE_STAGING_QUERY)  # Clear staging tables after processing

            query = """
                INSERT INTO food (id, name)
                SELECT fs.fdc_id, fs.description
                FROM food_staging AS fs
                WHERE fs.data_type = %s;
            """

            cur.execute(query, (FOOD_DATA_TYPE,))

            case_statements = []
            for key, value in nutrient_name_type_mapping.items():
                case_statements.append(
                    sql.SQL("WHEN {} THEN {}").format(
                        sql.Literal(key), sql.Literal(value)
                    )
                )

            case_when_clause = sql.SQL(" ").join(case_statements)

            case_when_full = sql.SQL("CASE ns.name {} ELSE NULL END").format(
                case_when_clause
            )

            query = sql.SQL(
                """
                INSERT INTO food_nutrient (food_id, nutrient_type, amount)
                SELECT fs.fdc_id,
                {} AS nutrient_type,
                fns.amount
                FROM food_nutrient_staging AS fns
                JOIN nutrient_staging AS ns ON fns.nutrient_id = ns.id and ns.name IN %s
                JOIN food_staging AS fs ON fns.fdc_id = fs.fdc_id
                WHERE fs.data_type = %s;
            """
            ).format(case_when_full)

            cur.execute(
                query,
                (tuple(nutrient_name_type_mapping.keys()), FOOD_DATA_TYPE),
            )

            cur.execute(TRUNCATE_STAGING_QUERY)  # Clear staging tables after processing


if __name__ == "__main__":
    load_csv_files()
