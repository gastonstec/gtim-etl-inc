import psycopg2
import pandas as pd

# Leer el archivo CSV usando pandas
df = pd.read_csv("C:/Users/negro/Documents/gtim-etl-inc-1/incident_limpio.csv")

# Conectar a la base de datos PostgreSQL
try:
    connection = psycopg2.connect(
        host='localhost',
        user='postgres',
        password='31102003',
        database='proyecto-incident'
    )

    print("Conexión exitosa")
    
    # Crear un cursor para interactuar con la base de datos
    cursor = connection.cursor()

    # Iterar sobre el DataFrame y cargar los datos en la tabla 'incident'
    for index, row in df.iterrows():
        query = """
            INSERT INTO incidents (number, state, created, last_update, incident_ci_type, affected_user,
                                  user_location, assignment_group, assigned_to, urgency, severity, created_by, updated_by)
            VALUES (%s, %s, %s::TIMESTAMP, %s::TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Asegúrate de que las columnas en la tupla coincidan con el orden de la consulta SQL
        cursor.execute(query, tuple(row))

    # Commit para guardar los cambios en la base de datos
    connection.commit()

    print("Datos importados exitosamente")

except Exception as ex:
    print(f"Error: {ex}")

finally:
    # Cerrar el cursor y la conexión
    if cursor:
        cursor.close()
    if connection:
        connection.close()
