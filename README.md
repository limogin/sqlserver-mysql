# Transferencia SQLServer a MySQL/MariaDB 

Transfiere el contenido de una base de datos de SQL Server a su equivalente en MariaDB 

## Uso 

s2my [-h] [cfg]

Siendo 
  - [cfg]  ruta al archivo yml de configuración 

Otros argumentos 
  - [-h, --help]  muestra un mensaje de ayuda 


## Configuración (yml) 

```
key:  "clave para generar hash si son necesarios  "
path: "ruta a una carpeta donde generará arhcivos sql con la información ddl"
src: 
  host: .. 
  dbname: ..
  user: ..
  password: ..
dst:
  host: .. 
  dbname: ..
  user: .. 
  password: ..
  port: ..
tables:
  - Tabla1
  - Tabla2
  - ..  
anonymize:
  Tabla1:
    - Campo1
    - Campo2 
    - .. 
  Tabla2:
    - Campo1
    - Campo2
    - ..
```

## Creación de un binario para facilitar la distribución 

pyinstaller debe de estar previamente instalado en el equipo

```
 pyinstaller --onefile ss2my.py 
```  


