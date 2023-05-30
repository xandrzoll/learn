import os
import psycopg2

from faker import Faker
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

load_dotenv()

fake = Faker('ru_RU')
Faker.seed(999)

conn_str = {
    'db_name': os.environ.get('DB_NAME'),
    'db_host': os.environ.get('DB_HOST'),
    'db_user': os.environ.get('DB_USER'),
    'db_pass': os.environ.get('DB_PASS'),
    'db_port': os.environ.get('DB_PORT'),
}


def generate_data(attrs: list):
    data = dict()
    for attr in attrs:
        f = fake.__getattr__(attr[0])
        data[attr[0]] = sorted(set([f() for _ in range(attr[1])]))
    return data


def create_staging_table(cursor) -> None:
    cursor.execute("""
        DROP TABLE IF EXISTS fake_data.first_names;
        CREATE TABLE fake_data.first_names (
            id serial primary key,
            name TEXT,
            male boolean
        );
    """)
    cursor.execute("""
            DROP TABLE IF EXISTS fake_data.last_names;
            CREATE TABLE fake_data.last_names (
                id serial primary key,
                name TEXT,
                male boolean
            );
        """)
    cursor.execute("""
                DROP TABLE IF EXISTS fake_data.companies;
                CREATE TABLE fake_data.companies (
                    id serial primary key,
                    company_name TEXT
                );
            """)
    cursor.execute("""
                    DROP TABLE IF EXISTS fake_data.cities;
                    CREATE TABLE fake_data.cities (
                        id serial primary key,
                        city_name TEXT
                    );
                """)
    cursor.execute("""
                    DROP TABLE IF EXISTS fake_data.streets;
                    CREATE TABLE fake_data.streets (
                        id serial primary key,
                        street_name TEXT
                    );
                """)
    cursor.execute("""
                    DROP TABLE IF EXISTS fake_data.buildings;
                    CREATE TABLE fake_data.buildings (
                        id serial primary key,
                        building_number TEXT
                    );
                """)


def insert_execute_batch(connection, table_name, insert_data) -> None:
    keys = list(insert_data[0].keys())
    keys_str = ', '.join([f'%({key})s' for key in keys])
    with connection.cursor() as cursor:
        execute_batch(cursor, f'INSERT INTO {table_name}({", ".join(keys)}) VALUES ({keys_str});', insert_data)
        print(f'INSERT {len(insert_data)} rows into {table_name}')


def insert_data_to_db(data: dict):
    conn = psycopg2.connect(database=conn_str['db_name'],
                            host=conn_str['db_host'],
                            user=conn_str['db_user'],
                            password=conn_str['db_pass'],
                            port=conn_str['db_port'])
    with conn.cursor() as cursor:
        create_staging_table(cursor)
        conn.commit()
        insert_data = [{'name': d, 'male': 'true'} for d in data['first_name_male']]
        insert_execute_batch(conn, 'fake_data.first_names', insert_data)

        insert_data = [{'name': d, 'male': 'true'} for d in data['last_name_male']]
        insert_execute_batch(conn, 'fake_data.last_names', insert_data)
        conn.commit()
        insert_data = [{'name': d, 'male': 'false'} for d in data['first_name_female']]
        insert_execute_batch(conn, 'fake_data.first_names', insert_data)

        insert_data = [{'name': d, 'male': 'false'} for d in data['last_name_female']]
        insert_execute_batch(conn, 'fake_data.last_names', insert_data)

        insert_data = [{'company_name': d} for d in data['company']]
        insert_execute_batch(conn, 'fake_data.companies', insert_data)

        insert_data = [{'city_name': d} for d in data['city']]
        insert_execute_batch(conn, 'fake_data.cities', insert_data)

        insert_data = [{'street_name': d} for d in data['street_name']]
        insert_execute_batch(conn, 'fake_data.streets', insert_data)

        insert_data = [{'building_number': d} for d in data['building_number']]
        insert_execute_batch(conn, 'fake_data.buildings', insert_data)

        conn.commit()
    conn.close()


if __name__ == '__main__':
    params = [
        ('first_name_male', 2000),
        ('last_name_male', 2000),
        ('first_name_female', 2000),
        ('last_name_female', 2000),
        ('city', 5000),
        ('street_name', 5000),
        ('building_number', 2000),
        ('company', 300000),
    ]
    data = generate_data(params)
    insert_data_to_db(data)

