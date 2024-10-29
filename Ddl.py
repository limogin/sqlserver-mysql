import re
import pyodbc

class Ddl:

    def __init__(self, pdo):
        self.connection = pdo
        
    
    def _execute_query (self, query): 
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
            print(f"Error al ejecutar la consulta: {e}")
            return []

    
    def types(self, table):
        """
          returns a dictionary about the data types for each field in table 
          table: table name (string)
        """        
        sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'
        """
        
        columns = self._execute_query(sql) 

        types = {}
        for column in columns:
            # parse field name to be compatible with MySQL|MariaDB
            name = re.sub(r'[^a-zA-Z0-9_]', '', column.COLUMN_NAME)
            types[name] = column.DATA_TYPE

        return types


    def tbl(self, table):
        sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'
        """
        
        columns = self._execute_query(sql)

        if not columns:
             return False 
        
        pk = ''
        ddl = f"CREATE TABLE IF NOT EXISTS `{table}` (\n"
        
        for column in columns:
            # auto = ''
            column_name = re.sub(r'[^a-zA-Z0-9_]', '', column.COLUMN_NAME)
            length = column.CHARACTER_MAXIMUM_LENGTH
            data_type = self.convert_data_type(column.DATA_TYPE, length)

            # If the number of fields exceeds 100 fields, I consider the varchar type as text 
            # to reduce the length of the table, even if the performance worsens. 
            if len(columns) > 100 and re.search(r"VARCHAR", data_type, re.I):
                data_type = 'TEXT'
                
            nullable = 'NULL' if column.IS_NULLABLE == 'YES' else 'NOT NULL'
            default = f"DEFAULT {self.convert_default(column.COLUMN_DEFAULT)}" if column.COLUMN_DEFAULT else ''
            
            if re.search(r"BIGINT", data_type, re.I) and self.convert_default(column.COLUMN_DEFAULT) == "''":
                default = 'DEFAULT 0'

            if not pk and re.search(r"uniqueidentifier", column.DATA_TYPE, re.I):
                pk = column_name
                default = ''
                # if re.search(r"newid", column.COLUMN_DEFAULT, re.I):
                #    auto = 'AUTO_INCREMENT'
                    
            # $comment = " COMMENT 'type: ".$column['DATA_TYPE']." default: ".addslashes($column['COLUMN_DEFAULT'])."'";
            comment = ''
            ddl += f"    `{column_name}` {data_type} {nullable} {default} {comment},\n"
        
        if pk:
            ddl += f"    PRIMARY KEY (`{pk}`)\n) "
        else:
            ddl = ddl.rstrip(",\n") + "\n) "
            
        ddl += " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci ROW_FORMAT COMPRESSED;\n"

        return ddl

    def convert_data_type(self, type, length=0):
        type = type.lower()
        
        if length is None:
          length = 0
    
        if type in ['tinyint', 'smallint', 'int']:
          return 'INT'
        elif type == 'bigint':
          return 'BIGINT'
        elif type in ['nvarchar', 'varchar']:
          if length == -1:  # -1 es para VARCHAR(MAX) en SQL Server
            return 'TEXT'
          return f"VARCHAR({length})" if length > 0 else "VARCHAR(255)"  # Valor por defecto si no se proporciona longitud
        elif type == 'datetime':
          return 'DATETIME'
        elif type == 'date':
          return 'DATE'
        elif type == 'time':
          return 'TIME'
        elif type == 'bit':
          return 'TINYINT(1)'
        elif type == 'money':
          return 'DECIMAL(19,4)'
        elif type == 'smallmoney':
          return 'DECIMAL(10,4)'
        elif type == 'float':
          return 'FLOAT'
        elif type == 'decimal':
          return f"DECIMAL({length}, 2)" if length > 0 else 'DECIMAL(10,2)'  # Valor por defecto
        elif type == 'numeric':
          return f"DECIMAL({length}, 2)" if length > 0 else 'DECIMAL(10,2)'  # Valor por defecto
        elif type == 'text':
          return 'TEXT'
        elif type == 'uniqueidentifier':
          return 'VARCHAR(50)'  # Almacena UUIDs como cadenas
        elif type == 'xml':
          return 'TEXT'  # MariaDB no tiene un tipo XML, se almacena como TEXT
        elif type == 'json':
          return 'JSON'  # MariaDB tiene soporte para JSON
        elif type == 'binary':
          return f"BINARY({length})" if length > 0 else 'BLOB'  # Longitud por defecto
        elif type == 'varbinary':
          return f"VARBINARY({length})" if length > 0 else 'BLOB'  # Longitud por defecto
        elif type == 'hierarchyid':
          return 'VARCHAR(100)'  # MariaDB no tiene este tipo, lo representamos como VARCHAR
        else:
          return f"VARCHAR({length})"  # Valor por defecto para tipos desconocidos


    def convert_default(self, df):
        if '(getdate())' in df:
            return 'CURRENT_TIMESTAMP'
        if re.search(r"newid", df, re.I):
            return "''"
        if 'datetime' in df:
            return "'0000-00-00'"
        if re.search(r"\{00", df):
            return "'0'"
        df = df.replace("(", "").replace(")", "")
        return df
