'''
Система Управления SQL Базой Данных
Разработчик: Urban Egor
Version: 0.0.78 a
Разработано в России
'''

# I M P O R T s
import psycopg2
import os
import subprocess
import time
from psycopg2 import sql

# G L O B A L  V A R s



# C L A S S E s
class TableControl:
	def __init__(self, database: str, user: str, password: str, host: str = "localhost", port: str = "5432") -> None:
		try:
			self.con = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
			self.cur = self.con.cursor()
			print("INIT -> Успешно")
		except Exception as e:
			print(f"!ERROR! => Ошибка инициализации значений ({e})")
			exit()

	def output_db_table(self, table_name: str) -> None:
		try:
			self.cur.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name)))
			rows = self.cur.fetchall()
			for row in rows:
				print(row)
			print("OUTPUTDBTABLE -> Успешно")
		except Exception as e:
			print(f"!ERROR! => Ошибка при выводе таблицы {table_name} ({e})")

	def create_table(self, table_name: str, columns: dict) -> None:
		try:
			columns_with_types = ", ".join([f"{col} {data_type}" for col, data_type in columns.items()])
			query = sql.SQL("CREATE TABLE {} ({})").format(sql.Identifier(table_name), sql.SQL(columns_with_types))
			self.cur.execute(query)
			self.con.commit()
			print("CREATETABLE -> Успешно")
		except Exception as e:
			self.con.rollback()
			print(f"!ERROR! => Ошибка создания таблицы {table_name}, ({e})")

	def add_column(self, table_name: str, column_name: str, data_type: str, constraints: str = "") -> None:
		try:
			query = sql.SQL("ALTER TABLE {} ADD COLUMN {} {} {}").format(
				sql.Identifier(table_name), sql.Identifier(column_name), sql.SQL(data_type), sql.SQL(constraints)
			)
			self.cur.execute(query)
			self.con.commit()
			print("ADDCOLUMN -> Успешно")
		except Exception as e:
			self.con.rollback()
			print(f"!ERROR! => Ошибка при создании столбца {column_name} в {table_name} ({e})")

	def del_column(self, table_name: str, column_name: str) -> None:
		try:
			query = sql.SQL("ALTER TABLE {} DROP COLUMN {}").format(sql.Identifier(table_name), sql.Identifier(column_name))
			self.cur.execute(query)
			self.con.commit()
			print("DELCOLUMN -> Успешно")
		except Exception as e:
			self.con.rollback()
			print(f"!ERROR! => Ошибка при удалении столбца {column_name} из {table_name} ({e})")

	def add_row(self, table_name: str, data: dict) -> None:
		try:
			columns = data.keys()
			values = data.values()
			query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
				sql.Identifier(table_name),
				sql.SQL(', ').join(map(sql.Identifier, columns)),
				sql.SQL(', ').join(sql.Placeholder() * len(values))
			)
			self.cur.execute(query, list(values))
			self.con.commit()
			print("ADDROW -> Успешно")
		except (psycopg2.DatabaseError, ValueError) as e:
			self.con.rollback()
			print(f"!ERROR! => Ошибка при создании строки в {table_name} ({e})")
		except Exception as e:
			self.con.rollback()
			print(f"!ERROR! => Внезапная ошибка: {e}")

	def del_all_columns(self, table_name: str) -> None:
		try:
			self.cur.execute(sql.SQL("""
				DO $$ 
				DECLARE 
					columns TEXT;
				BEGIN
					SELECT string_agg('DROP COLUMN ' || column_name, ', ')
					INTO columns
					FROM information_schema.columns
					WHERE table_name = %s;

					EXECUTE 'ALTER TABLE ' || %s || ' ' || columns;
				END $$;
			"""), [table_name, table_name])
			self.con.commit()
			print("DELALLCOLUMNS -> Успешно")
		except Exception as e:
			self.con.rollback()
			print(f"!ERROR! => Ошибка при удалении столбцов в {table_name} ({e})")

	def del_row(self, table_name: str, key_name: str, key_value) -> None:
		try:
			query = sql.SQL("DELETE FROM {} WHERE {} = %s").format(sql.Identifier(table_name), sql.Identifier(key_name))
			self.cur.execute(query, [key_value])
			self.con.commit()
			print("DELROW -> Успешно")
		except Exception as e:
			self.con.rollback()
			print(f"!ERROR! => Ошибка при удалении строки из {table_name} ({e})")


	def close_db(self) -> None:
		try:
			self.cur.close()
			self.con.close()
			print("CLOSEDB -> Успешно")
		except Exception as e:
			print(f"!ERROR! => Ошибка завершения ({e})")

class databaseControl:
	def __init__(self):
		pass

	def create_database(self, database: str, user: str, password: str, host: str = "localhost", port: str = "5432") -> None:
		try:
			con = psycopg2.connect(database="postgres", user=user, password=password, host=host, port=port)
			con.autocommit = True
			cur = con.cursor()

			cur.execute(f"CREATE DATABASE {database}")
			print(f"База данных {database} успешно создана")

			cur.close()
			con.close()
		except Exception as e:
			print(f"Ошибка при создании базы данных {database}: {e}")

if __name__ == "__main__":
	database = databaseControl()
	database.create_database("test_DB_N", "postgres", "g403rgvh2")










r'''class Server:
    def __init__(self, host, port, db_name, user_name, postgres_password):
        self.POSTGRES_BIN_PATH = r'C:\Program Files\PostgreSQL\16\bin'  
        self.DATA_DIRECTORY = r'C:\Users\Urban\Documents\sql\1'  
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user_name = user_name
        self.postgres_password = postgres_password

    def run_command(self, command):
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            print(f"Error: {stderr.decode()}")
        else:
            print(stdout.decode())

    def initialize_database(self):
        os.makedirs(self.DATA_DIRECTORY, exist_ok=True)
        initdb_command = f'"{os.path.join(self.POSTGRES_BIN_PATH, "initdb.exe")}" -D "{self.DATA_DIRECTORY}"'
        print("Initializing database...")
        self.run_command(initdb_command)

    def start_postgresql_server(self):
        pg_ctl_command = f'"{os.path.join(self.POSTGRES_BIN_PATH, "pg_ctl.exe")}" -D "{self.DATA_DIRECTORY}" -o "-h {self.host} -p {self.port}" -l logfile start'
        print("Starting PostgreSQL server...")
        self.run_command(pg_ctl_command)
        time.sleep(5)  # Wait for the server to start

    def create_database_and_user(self):
        os.environ['PGPASSWORD'] = self.postgres_password

        create_db_command = f'"{os.path.join(self.POSTGRES_BIN_PATH, "createdb.exe")}" -h {self.host} -p {self.port} -U postgres {self.db_name}'
        create_user_command = f'"{os.path.join(self.POSTGRES_BIN_PATH, "createuser.exe")}" -h {self.host} -p {self.port} -U postgres --superuser {self.user_name}'
        print("Creating database and user...")
        self.run_command(create_db_command)	
        self.run_command(create_user_command)'''
