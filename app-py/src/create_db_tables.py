import os

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import AsIs
from .db import DB_CONFIG


table_definitions = [
    {
        'table_name': 'food',
        'columns': {
            'id': 'INTEGER PRIMARY KEY',
            'name': 'TEXT'
        }
    },
    {
        'table_name': 'food_nutrient',
        'columns': {
            'food_id': 'INTEGER',
            'nutrient_type': 'VARCHAR(64)',
            'amount': 'DECIMAL(10, 3)',
        }
    },
    {
        'table_name': 'food_staging',
        'columns': {
            'fdc_id': 'INTEGER PRIMARY KEY',
            'data_type': 'VARCHAR(64)',
            'description': 'TEXT',
            'food_category_id': 'VARCHAR(64)',
            'publication_date': 'DATE',
        }
    },
    {
        'table_name': 'nutrient_staging',
        'columns': {
            'id': 'INTEGER PRIMARY KEY',
            'name': 'VARCHAR(128)',
            'unit_name': 'VARCHAR(8)',
            'nutrient_nbr': 'VARCHAR(64)',
            "rank": 'VARCHAR(64)',
        }
    },
    {
        'table_name': 'food_nutrient_staging',
        'columns': {
            'id': 'INTEGER',
            'fdc_id': 'INTEGER',
            'nutrient_id': 'INTEGER',
            'amount': 'DECIMAL(10, 3)',
            'data_points': 'VARCHAR(64)',
            'derivation_id': 'VARCHAR(64)',
            'min': 'VARCHAR(64)',
            'max': 'VARCHAR(64)',
            'median': 'VARCHAR(64)',
            'footnote': 'VARCHAR(2000)',
            'min_year_acquired': 'VARCHAR(64)',
        }
    }
]


async def create_tables():
    """Create database tables based on the defined schema."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:

            for table_def in table_definitions:
                table_name = table_def['table_name']
                columns_def = table_def['columns']

                # Generate column definitions
                columns_sql = sql.SQL(', ').join(
                    sql.SQL(' ').join([
                        sql.Identifier(col_name),
                        sql.SQL(data_type)
                    ]) for col_name, data_type in columns_def.items()
                )

                # Generate CREATE TABLE statement
                create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {table_name} ({columns})").format(
                    table_name=sql.Identifier(table_name),
                    columns=columns_sql
                )
                
                cur.execute(create_table_query)
                print(f"Table '{table_name}' created.")


if __name__ == "__main__":
    create_tables(DB_CONFIG)
