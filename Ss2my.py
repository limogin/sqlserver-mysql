import subprocess 
import os 
import yaml # pip install pyyaml 
import argparse 
import pyodbc  # pip3 install pyodbc - SQL Server connection library
import mysql.connector  # pip3 install mysql-connector - MySQL/MariaDB connection library 
import hashlib

from Ddl import Ddl

class Ss2My: 
  """
    Ss2My is a class that facilitates the migration of data from a SQL Server database to 
    a MySQL or MariaDB database.

    Attributes:
        pdo (PDO): A PDO instance for connecting to the source SQL Server database.
        my (PDO): A PDO instance for connecting to the destination MySQL or MariaDB database.
        cfg (dict): A dictionary containing the configuration settings for the database connections and data handling.
    
    Methods:
        normalize_table(s: str) -> str:
            Sanitizes table names by removing invalid characters.

        normalize_field(s: str) -> str:
            Sanitizes field names by removing invalid characters.

        __init__() -> None:
            Initializes the Ss2My instance by loading the configuration from a YAML file,
            and establishing connections to both the source and destination databases.

        process() -> None:
            Executes the data migration process by retrieving tables and their structures from the 
            SQL Server database, generating the corresponding DDL statements for MySQL/MariaDB,
            and transferring data while applying any specified transformations or anonymizations.

        my_insert(table: str, d: dict, types: dict) -> bool:
            Inserts a record into the MySQL/MariaDB database while handling type conversions
            and potential errors during the insertion process.

        my_exec(sql: str, params: list) -> bool:
            Executes a given SQL statement on the MySQL/MariaDB database, handling errors and 
            returning the result of the operation.
  """
  DEBUG = True 
    
  def __init__ (self): 
    parser = argparse.ArgumentParser(description="SQLServer to MySQL/MariaDB database conversion")
    parser.add_argument ("cfg", nargs='?', default='ss2my.yml', help="path to yml config file, defaults ss2my.yml")
    args = parser.parse_args()
    self.parser = parser
    self.args = args 

    self.cfg = self.load_yml (args.cfg) 
    if self.cfg == None: 
        self.help() 
    
    # SQL Server connection 
    src = self.cfg['src']
    dsn = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={src['host']};DATABASE={src['dbname']};UID={src['user']};PWD={src['password']}"
    self.pdo = pyodbc.connect(dsn)
        
    # MySQL/MariaDB connection 
    dst = self.cfg['dst']
    self.my = mysql.connector.connect(
       host=dst['host'],
       port=dst['port'],
       database=dst['dbname'],
       user=dst['user'],
       password=dst['password']
    )


  def help (self):
      self.parser.print_help()

  
  def load_yml (self, fn): 
      """
      Loads a yml file 
      :param fn file name (string) 
      """ 
      try:
        with open (fn, "r") as fi:
           c = yaml.safe_load(fi)
        return c 
      except FileNotFoundError:
        debug ("File does not exists {fn}")
        return None 
      except yaml.YAMLError as e: 
        debug ("Error {e}")
        return None 

  
  def debug (self, s, e=False):
      if self.DEBUG:
        if e:
            print(s, file=sys.stderr)
            self.help()
            sys.exit(1) 
        else:
            print(s)


  def normalize_table(self, s):
        return ''.join(char for char in s if char.isalnum() or char == '_')

  def normalize_field(self, s):
        return ''.join(char for char in s if char.isalnum() or char == '_')

  def anonymize(self, value, key):  
        return hashlib.sha1((str(value) + key).encode()).hexdigest()[:20]        

  def my_insert(self, table, data, types):
        if not data:
            if self.DEBUG:
                print('array vacío')
            return False

        # Normalize fields 
        normalized_data = {self.normalize_field(k): v for k, v in data.items() if not (types.get(self.normalize_field(k), '').lower() != 'varchar' and v.strip() == '')}

        cols = list(normalized_data.keys())
        vals = list(normalized_data.values())

        placeholders = ', '.join(['%s'] * len(cols))
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"

        try:
            cursor = self.my.cursor()
            cursor.execute(sql, vals)
            self.my.commit()
            return cursor.lastrowid or True
        except Exception as e:
            if self.DEBUG:
                print(e)
                print(types)
            return False    

  def my_exec(self, sql, params=None):
        try:
            cursor = self.my.cursor()
            cursor.execute(sql, params or [])

            if sql.strip().lower().startswith('select'):
                return cursor.fetchall()
            else:
                self.my.commit()
                return True
        except Exception as e:
            if self.DEBUG:
                print(e)
            return False


  def process(self):
       cursor = self.pdo.cursor()
        
       if 'tables' in self.cfg and self.cfg['tables']:
           tables = self.cfg['tables']
       else:
           cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
           tables = [row[0] for row in cursor.fetchall()]

       errors = []
       ddl_generator = Ddl(self.pdo)

       for table in tables:
            _table = self.normalize_table(table)
            ddl_statement = ddl_generator.tbl(table)
            types = ddl_generator.types(table)
            self.my_exec(ddl_statement)
            self.my_exec(f"DELETE FROM {table}")
            p = self.cfg['path']
            if os.path.exists(p):              
              with open(f'{p}/{_table}.sql', 'w') as f:
                 f.write(ddl_statement)

       for table in tables:
            _table = self.normalize_table(table)

            if self.DEBUG:
                print(f"table: {table} ({_table})\n")

            try:
                sql = f"SELECT * FROM [{table}]"
                cursor.execute(sql)
            except Exception as e:
                errors.append({table: str(e)})
                continue

            for row in cursor.fetchall():
                row_dict = {cursor.description[i][0]: row[i] for i in range(len(row))}
                for k, v in row_dict.items():
                    _k = self.normalize_field(k)
                    if _table in self.cfg.get('anonymize', {}) and _k in self.cfg['anonymize'][_table]:
                        v = self.anonymize(v, self.cfg['key'])
                    row_dict[k] = str(v).strip()

                if not self.my_insert(_table, row_dict, types):
                    if self.DEBUG:
                        print(row_dict)


def main(): 
    ss2 = Ss2My()
    ss2.process() 

if __name__ == "__main__":
    main() 



