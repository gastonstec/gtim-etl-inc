# Importación de las bibliotecas necesarias
import psycopg2  # Biblioteca para conectarse a la base de datos PostgreSQL
import pandas as pd  # Biblioteca para manejar datos tabulares, como archivos CSV

# Leer el archivo CSV usando pandas y almacenar los datos en un DataFrame
df = pd.read_csv("C:/Users/negro/Documents/gtim-etl-inc-1/incident_limpio.csv")

# Intentar establecer una conexión a la base de datos PostgreSQL
try:
    # Conectar a la base de datos con los parámetros proporcionados (host, usuario, contraseña, base de datos)
    connection = psycopg2.connect(
        host='localhost',  # Dirección del servidor de la base de datos
        user='postgres',   # Usuario para acceder a la base de datos
        password='31102003',  # Contraseña de acceso a la base de datos
        database='proyecto-incident'  # Nombre de la base de datos a la que se va a conectar
    )

    # Si la conexión es exitosa, imprimir un mensaje
    print("Conexión exitosa")
    
    # Crear un cursor para ejecutar comandos SQL en la base de datos
    cursor = connection.cursor()

    # Iterar sobre las filas del DataFrame (df) para insertar cada fila en la tabla 'incidents'
    for index, row in df.iterrows():
        # Definir la consulta SQL para insertar los datos de cada fila en la tabla
        query = """
            INSERT INTO incidents (number, state, created, last_update, incident_ci_type, affected_user,
                                  user_location, assignment_group, assigned_to, urgency, severity, created_by, updated_by)
            VALUES (%s, %s, %s::TIMESTAMP, %s::TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Ejecutar la consulta SQL, pasando los valores de la fila como parámetros
        # Se convierte la fila a una tupla para que coincida con el formato de la consulta SQL
        cursor.execute(query, tuple(row))

    # Realizar un commit para guardar los cambios en la base de datos
    connection.commit()

    # Imprimir un mensaje de éxito si los datos se importaron correctamente
    print("Datos importados exitosamente")

# Si ocurre un error durante la conexión o la ejecución del código, se captura y muestra el error
except Exception as ex:
    print(f"Error: {ex}")

finally:
    # Cerrar el cursor y la conexión a la base de datos en el bloque finally para asegurar que se cierre incluso si ocurre un error
    if cursor:
        cursor.close()
    if connection:
        connection.close()
