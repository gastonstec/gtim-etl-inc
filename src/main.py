from flask import Flask, request, jsonify  # Importa las librerías necesarias de Flask para crear la app, manejar solicitudes HTTP y devolver respuestas JSON
import os  # Importa la librería para interactuar con el sistema de archivos
import pandas as pd  # Importa la librería Pandas para manipulación y análisis de datos
import psycopg2  # Importa psycopg2 para interactuar con la base de datos PostgreSQL
from psycopg2.extras import RealDictCursor  # Importa un cursor especial que devuelve resultados como diccionarios
from config import config  # Importa la configuración de la base de datos (probablemente de un archivo config.py)

app = Flask(__name__)  # Crea una instancia de la aplicación Flask

# Configurar la conexión a la base de datos
app.config.from_object(config['development'])  # Carga la configuración de la base de datos para el entorno de desarrollo desde el archivo de configuración

# Función para establecer la conexión con la base de datos
def obtener_conexion():
    try:
        return psycopg2.connect(
            host="localhost",
            database="proyecto-incident",
            user="postgres",
            password="31102003"
        )  # Establece la conexión con la base de datos PostgreSQL
    except Exception as ex:
        print(f"Error de conexión a la base de datos: {ex}")  # Si ocurre un error, se imprime en la consola
        return None  # Si no se puede conectar, se retorna None

# Ruta para listar todos los incidentes (GET)
@app.route('/incidentes', methods=['GET'])
def listar_incidentes():
    conexion = obtener_conexion()  # Obtiene la conexión a la base de datos
    if conexion is None:
        return jsonify({'mensaje': "Error de conexión a la base de datos"}), 500  # Si no se pudo conectar, devuelve un error

    try:
        # Se crea un cursor para ejecutar la consulta y obtener los datos en formato diccionario
        with conexion.cursor(cursor_factory=RealDictCursor) as cursor:
            sql = """
            SELECT number, state, created, last_update, incident_ci_type, affected_user, 
                   user_location, assignment_group, assigned_to, urgency, severity, 
                   created_by, updated_by 
            FROM incidents
            """
            cursor.execute(sql)  # Ejecuta la consulta SQL
            datos = cursor.fetchall()  # Recupera todos los registros de la consulta

        conexion.close()  # Cierra la conexión a la base de datos

        # Devuelve los datos obtenidos en formato JSON
        return jsonify({'incidentes': datos, 'mensaje': "Incidentes listados"})

    except Exception as ex:
        # Si ocurre un error durante la ejecución de la consulta, se captura y se retorna un mensaje de error
        return jsonify({'error': str(ex), 'mensaje': "Error al obtener los datos"}), 500

# Ruta para obtener los detalles de un incidente específico (GET)
@app.route('/incidentes/<string:number>', methods=['GET'])
def leer_incidente(number):
    conexion = obtener_conexion()  # Obtiene la conexión a la base de datos
    if conexion is None:
        return jsonify({'mensaje': "Error de conexión a la base de datos"}), 500

    try:
        # Verificar si el incidente existe en la base de datos
        with conexion.cursor(cursor_factory=RealDictCursor) as cursor:
            sql = """
            SELECT number, state, created, last_update, incident_ci_type, affected_user, 
                   user_location, assignment_group, assigned_to, urgency, severity, 
                   created_by, updated_by 
            FROM incidents WHERE number = %s
            """
            cursor.execute(sql, (number,))  # Se usa un parámetro seguro para evitar inyecciones SQL
            datos = cursor.fetchone()  # Obtiene un solo registro

        # Si se encuentra el incidente, se retorna en formato JSON
        if datos:
            return jsonify({'incidente': datos, 'mensaje': "Incidente encontrado"}), 200
        else:
            return jsonify({'mensaje': "Incidente no encontrado"}), 404

    except Exception as ex:
        # Si ocurre un error, se captura y se retorna un mensaje de error
        return jsonify({'error': str(ex), 'mensaje': "Error al obtener el incidente"}), 500

# Ruta para agregar un nuevo incidente (POST)
@app.route('/incidentes', methods=['POST'])
def agregar_incidente():
    conexion = obtener_conexion()  # Obtiene la conexión a la base de datos
    if conexion is None:
        return jsonify({'mensaje': "Error de conexión a la base de datos"}), 500

    try:
        # Recibe los datos en formato JSON desde la solicitud
        datos = request.get_json()

        # Inserta un nuevo incidente en la base de datos
        with conexion.cursor() as cursor:
            sql = """
            INSERT INTO incidents (number, state, created, last_update, incident_ci_type, affected_user, 
                                  user_location, assignment_group, assigned_to, urgency, severity, 
                                  created_by, updated_by)
            VALUES (%s, %s, %s::TIMESTAMP, %s::TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                datos['number'], datos['state'], datos['created'], datos['last_update'], datos['incident_ci_type'],
                datos['affected_user'], datos['user_location'], datos['assignment_group'], datos['assigned_to'],
                datos['urgency'], datos['severity'], datos['created_by'], datos['updated_by']
            ))
            conexion.commit()  # Guarda los cambios en la base de datos

        conexion.close()  # Cierra la conexión
        # Responde con un mensaje de éxito
        return jsonify({'mensaje': "Incidente agregado exitosamente"}), 201

    except Exception as ex:
        if conexion:
            conexion.rollback()  # Si ocurre un error, deshace los cambios
            conexion.close()  # Cierra la conexión
        # Devuelve un mensaje de error
        return jsonify({'error': str(ex), 'mensaje': "Error al agregar el incidente"}), 500

# Ruta para actualizar un incidente existente (PUT)
@app.route('/incidentes/<string:number>', methods=['PUT'])
def actualizar_incidente(number):
    conexion = obtener_conexion()  # Obtiene la conexión a la base de datos
    if conexion is None:
        return jsonify({'mensaje': "Error de conexión a la base de datos"}), 500

    try:
        # Recibe los datos en formato JSON
        datos = request.get_json()

        # Verifica si el incidente existe en la base de datos antes de intentar actualizarlo
        with conexion.cursor() as cursor:
            sql_select = """
            SELECT number FROM incidents WHERE number = %s
            """
            cursor.execute(sql_select, (number,))
            incidente_existente = cursor.fetchone()  # Verifica si el incidente existe

            if not incidente_existente:
                return jsonify({'mensaje': "Incidente no encontrado"}), 404  # Si no existe, retorna un error

        # Si el incidente existe, se actualiza
        with conexion.cursor() as cursor:
            sql_update = """
            UPDATE incidents 
            SET state = %s, 
                created = %s::TIMESTAMP, 
                last_update = %s::TIMESTAMP, 
                incident_ci_type = %s, 
                affected_user = %s, 
                user_location = %s, 
                assignment_group = %s, 
                assigned_to = %s, 
                urgency = %s, 
                severity = %s, 
                created_by = %s, 
                updated_by = %s
            WHERE number = %s
            """
            cursor.execute(sql_update, (
                datos['state'], datos['created'], datos['last_update'], datos['incident_ci_type'],
                datos['affected_user'], datos['user_location'], datos['assignment_group'], datos['assigned_to'],
                datos['urgency'], datos['severity'], datos['created_by'], datos['updated_by'], number
            ))
            conexion.commit()  # Guarda los cambios

        conexion.close()  # Cierra la conexión
        return jsonify({'mensaje': "Incidente actualizado exitosamente"}), 200  # Responde con un mensaje de éxito

    except Exception as ex:
        if conexion:
            conexion.rollback()  # Deshace cambios en caso de error
            conexion.close()
        return jsonify({'error': str(ex), 'mensaje': "Error al actualizar el incidente"}), 500

# Ruta para eliminar un incidente (DELETE)
@app.route('/incidentes/<string:number>', methods=['DELETE'])
def eliminar_incidente(number):
    conexion = obtener_conexion()  # Obtiene la conexión a la base de datos
    if conexion is None:
        return jsonify({'mensaje': "Error de conexión a la base de datos"}), 500

    try:
        # Verifica si el incidente existe antes de intentar eliminarlo
        with conexion.cursor() as cursor:
            sql_select = """
            SELECT number FROM incidents WHERE number = %s
            """
            cursor.execute(sql_select, (number,))
            incidente_existente = cursor.fetchone()  # Verifica si el incidente existe

            if not incidente_existente:
                return jsonify({'mensaje': "Incidente no encontrado"}), 404  # Si no existe, retorna un error

        # Elimina el incidente
        with conexion.cursor() as cursor:
            sql_delete = """
            DELETE FROM incidents WHERE number = %s
            """
            cursor.execute(sql_delete, (number,))
            conexion.commit()  # Guarda los cambios

        conexion.close()  # Cierra la conexión
        return jsonify({'mensaje': "Incidente eliminado exitosamente"}), 200  # Responde con un mensaje de éxito

    except Exception as ex:
        if conexion:
            conexion.rollback()  # Deshace los cambios si ocurre un error
            conexion.close()
        return jsonify({'error': str(ex), 'mensaje': "Error al eliminar el incidente"}), 500

# Ruta para cargar el archivo CSV y procesarlo (POST)
@app.route('/incidentes/upload', methods=['POST'])
def upload_file():
    # Crear la carpeta uploads si no existe
    if not os.path.exists('uploads'):
        os.makedirs('uploads')  # Si no existe la carpeta uploads, la crea

    # Recibe el archivo desde la solicitud
    file = request.files['file']
    filepath = './uploads/incident.csv'  # Define la ruta donde se guardará el archivo
    
    # Guarda el archivo en el servidor
    file.save(filepath)

    try:
        # Lee el archivo CSV con Pandas
        df = pd.read_csv(filepath, delimiter=',', encoding='unicode_escape')

        # Renombra las columnas del archivo CSV
        df = df.rename(columns={
            'Number': 'number', 'State': 'state', 'Created': 'created', 'Last update': 'last_update',
            'Incident CI type': 'incident_ci_type', 'Affected User': 'affected_user', 'User location': 'user_location',
            'Assignment Group': 'assignment_group', 'Assigned to': 'assigned_to', 'Urgency': 'urgency',
            'Severity': 'severity', 'Created By': 'created_by', 'Updated By': 'updated_by'
        })

        # Convertir todas las columnas a tipo string y reemplazar valores vacíos por "NaN"
        df = df.astype(str)
        df = df.fillna("NaN")
        
        # Convierte las columnas de fecha a tipo datetime
        df['created'] = pd.to_datetime(df['created'], format='%m-%d-%Y %H:%M:%S', errors='coerce')
        df['last_update'] = pd.to_datetime(df['last_update'], format='%m-%d-%Y %H:%M:%S', errors='coerce')

        # Guarda el archivo CSV limpio
        clean_filepath = './uploads/incident_limpio.csv'
        df.to_csv(clean_filepath, index=False, encoding='utf-8')

        # Conexión a la base de datos
        connection = psycopg2.connect(
            host='localhost',
            user='postgres',
            password='31102003',
            database='proyecto-incident'
        )
        cursor = connection.cursor()

        # Inserta los datos procesados en la base de datos
        for index, row in df.iterrows():
            query = """
                INSERT INTO incidents (number, state, created, last_update, incident_ci_type, affected_user,
                                      user_location, assignment_group, assigned_to, urgency, severity, created_by, updated_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, tuple(row))  # Inserta cada fila del DataFrame en la base de datos
        connection.commit()  # Guarda los cambios

        return jsonify({'mensaje': 'Archivo procesado e incidentes importados exitosamente'}), 201

    except Exception as ex:
        return jsonify({'error': str(ex), 'mensaje': "Error al procesar el archivo CSV"}), 500

if __name__ == '__main__':
    app.run(debug=True)
