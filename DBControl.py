import psycopg2

class TableControl:
	def __init__(self, database: str, user: str, password: str, host: str = "localhost", port: str = "5432") -> None:
		try:
			self.con = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
			self.cur = self.con.cursor()
			self.rows = None
			print("INIT -> Успешно")
		except Exception as e:
			print(f"!ERROR! => Ошибка инициализации значений ({e})")
			exit()


	def outputDbTable(self, tableName: str) -> None:
		try:
			self.cur.execute(f"SELECT * FROM {tableName}")
			self.rows = self.cur.fetchall()
			for row in self.rows:
				print(row)
			print("OUTPUTDBTABLE -> Успешно")
		except Exception as e:
			print(f"!ERROR! => Ошибка при выводе таблицы {tableName} ({e})")


	def createTable(self, tableName):
		try:
			self.cur.execute(f"""
							CREATE TABLE {tableName} (
								id SERIAL PRIMARY KEY
							);
			""")
			print("CREATETABLE -> Успешно")
		except Exception as e:
			print(f"!ERROR! => Ошибка создания таблицы {tableName}, ({e})")


	def addColumn(self, tableName: str, columnName: str, data_type: str, dop:str = "") -> None:
		try:
			self.cur.execute(f"""
							ALTER TABLE {tableName}
							ADD COLUMN {columnName} {data_type} {dop};
			""")
			print("ADDCOLUMN -> Успешно")
		except Exception as e:
			print(f"!ERROR! => Ошибка при создании столбца {columnName} в {tableName} ({e})")


	def delColumn(self, tableName: str, columnName: str) -> None:
		try:
			self.cur.execute(f"""
							ALTER TABLE {tableName}
							DROP COLUMN {columnName};
			""")
			print("DELCOLUMN -> Успешно")
		except Exception as e:
			print(f"!ERROR! => Ошибка при удалении столбца {columnName} из {tableName} ({e})")


	def addRow(self, tableName: str, columns: str, values: tuple) -> None:
			try:
				if not tableName or not columns or not values:
					raise ValueError("!ERROR! => Аргументы не могут быть пусстыми")

				columns_list = columns.split(',')
				if len(columns_list) != len(values):
					raise ValueError("Number of columns and values must match")
				values_placeholders = ', '.join(['%s'] * len(values))
				query = f"INSERT INTO {tableName} ({columns}) VALUES ({values_placeholders})"

				self.cur.execute(query, values)
				self.con.commit()

				print("ADDROW -> Успешно")
			except (psycopg2.DatabaseError, ValueError) as e:
				if self.con:
					self.con.rollback()
				print(f"!ERROR! => Ошибка при создании строки в {tableName} ({e})")
			except Exception as e:
				if self.con:
					self.con.rollback()
				print(f"!ERROR! => Внезапная ошибка: {e}")


	def delAllColumns(self, tableName: str) -> None:
		try:
			self.cur.execute(f"""
							DO $$ 
							DECLARE 
								columns TEXT;
							BEGIN
								SELECT string_agg('DROP COLUMN ' || column_name, ', ')
								INTO columns
								FROM information_schema.columns
								WHERE table_name = {tableName};

								EXECUTE 'ALTER TABLE {tableName} ' || columns;
							END $$;
			""") 
			print("DELALLCOLUMNS -> Успешно")
		except Exception as e:
			print(f"!ERROR! => Ошибка при удалении столбцов в {tableName} ({e})")

	def delRow(self, tableName, keyName, key) -> None:
		try:
			self.cur.execute(f"""
							DELETE FROM {tableName} WHERE {keyName} = {key};
			""")
		
			print("DELROW -> Успешно")
		except Exception as e:
			print(f"!ERROR! => Ошибка при удалении строки")

	def closeDb(self) -> None:
		try:
			self.con.commit()
			self.cur.close()
			self.con.close()

			print("CLOSEDB -> Успешно")
		except Exception as e:
			print(f"!ERROR! => Ошибка завершения ({e})")


if __name__ == "__main__":
	table = TableControl("database name", "Your name", "Your password", "host name")
	table.closeDb()



