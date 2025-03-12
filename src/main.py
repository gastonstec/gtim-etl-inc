from flask import Flask, request, jsonify
import os
import pandas as pd
import psycopg2 
from psycopg2.extras import RealDictCursor  # Para trabajar con los resultados de la base de datos como diccionarios
from config import config

app = Flask(__name__)

# Configurar la conexión a la base de datos
app.config.from_object(config['development'])

# Función para establecer la conexión con la base de datos
def obtener_conexion():
    try:
        return psycopg2.connect(
            host="localhost",
            database="proyecto-incident",
            user="postgres",
            password="31102003"
        )
    except Exception as ex:
        print(f"Error de conexión a la base de datos: {ex}")
        return None
    
# Ruta para listar todos los incidentes (GET)
@app.route('/incidentes', methods=['GET'])
def listar_incidentes():
    conexion = obtener_conexion()  # Obtener la conexión a la base de datos
    if conexion is None:
        # Si no hay conexión, se retorna un error
        return jsonify({'mensaje': "Error de conexión a la base de datos"}), 500
    try:
        # Se crea un cursor para ejecutar la consulta y obtener los datos en formato diccionario
        with conexion.cursor(cursor_factory=RealDictCursor) as cursor:
            sql = """
            SELECT number, state, created, last_update, incident_ci_type, affected_user, 
                   user_location, assignment_group, assigned_to, urgency, severity, 
                   created_by, updated_by 
            FROM incidents
            """
            cursor.execute(sql)  # Ejecuta la consulta
            datos = cursor.fetchall()  # Recupera todos los registros

        conexion.close()  # Cierra la conexión a la base de datos

        # Retorna los datos en formato JSON
        return jsonify({'incidentes': datos, 'mensaje': "Incidentes listados"})

    except Exception as ex:
        # Si ocurre un error durante la ejecución de la consulta, se captura y se retorna un mensaje de error
        return jsonify({'error': str(ex), 'mensaje': "Error al obtener los datos"}), 500

# Ruta para obtener los detalles de un incidente específico (GET)
@app.route('/incidentes/<string:number>', methods=['GET'])
def leer_incidente(number):
    conexion = obtener_conexion()  # Obtener la conexión a la base de datos
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
            cursor.execute(sql, (number,))  # Se usa un parámetro seguro
            datos = cursor.fetchone()  # Obtener el primer registro (solo un incidente)

        # Si se encuentra el incidente, se retorna en formato JSON
        if datos:
            return jsonify({'incidente': datos, 'mensaje': "Incidente encontrado"}), 200
        else:
            return jsonify({'mensaje': "Incidente no encontrado"}), 404

    except Exception as ex:
        # Si ocurre un error al ejecutar la consulta, se retorna el mensaje de error
        return jsonify({'error': str(ex), 'mensaje': "Error al obtener el incidente"}), 500

# Ruta para agregar un nuevo incidente (POST)
@app.route('/incidentes', methods=['POST'])
def agregar_incidente():
    conexion = obtener_conexion()  # Obtener la conexión a la base de datos
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
            conexion.commit()  # Guardar los cambios en la base de datos

        conexion.close()
        # Responde con un mensaje de éxito
        return jsonify({'mensaje': "Incidente agregado exitosamente"}), 201

    except Exception as ex:
        if conexion:
            conexion.rollback()  # Si ocurre un error, deshacer los cambios
            conexion.close()
        # Retorna un error si algo falla
        return jsonify({'error': str(ex), 'mensaje': "Error al agregar el incidente"}), 500

# Ruta para actualizar un incidente existente (PUT)
@app.route('/incidentes/<string:number>', methods=['PUT'])
def actualizar_incidente(number):
    conexion = obtener_conexion()  # Obtener la conexión a la base de datos
    if conexion is None:
        return jsonify({'mensaje': "Error de conexión a la base de datos"}), 500

    try:
        # Recibe los datos en formato JSON
        datos = request.get_json()

        # Verifica si el incidente existe en la base de datos antes de intentar actualizarlo
        with conexion.cursor() as cursor:
            sql_select = """
            SELECT number FROM incident WHERE number = %s
            """
            cursor.execute(sql_select, (number,))
            incidente_existente = cursor.fetchone()

            if not incidente_existente:
                return jsonify({'mensaje': "Incidente no encontrado"}), 404

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
            conexion.commit()  # Guardar los cambios

        conexion.close()
        # Responde con un mensaje de éxito
        return jsonify({'mensaje': "Incidente actualizado exitosamente"}), 200

    except Exception as ex:
        if conexion:
            conexion.rollback()  # Deshacer cambios en caso de error
            conexion.close()
        return jsonify({'error': str(ex), 'mensaje': "Error al actualizar el incidente"}), 500

# Ruta para eliminar un incidente (DELETE)
@app.route('/incidentes/<string:number>', methods=['DELETE'])
def eliminar_incidente(number):
    conexion = obtener_conexion()  # Obtener la conexión a la base de datos
    if conexion is None:
        return jsonify({'mensaje': "Error de conexión a la base de datos"}), 500

    try:
        # Verifica si el incidente existe antes de intentar eliminarlo
        with conexion.cursor() as cursor:
            sql_select = """
            SELECT number FROM incidents WHERE number = %s
            """
            cursor.execute(sql_select, (number,))
            incidente_existente = cursor.fetchone()

            if not incidente_existente:
                return jsonify({'mensaje': "Incidente no encontrado"}), 404

        # Elimina el incidente
        with conexion.cursor() as cursor:
            sql_delete = """
            DELETE FROM incident WHERE number = %s
            """
            cursor.execute(sql_delete, (number,))
            conexion.commit()  # Guardar los cambios

        conexion.close()
        # Responde con un mensaje de éxito
        return jsonify({'mensaje': "Incidente eliminado exitosamente"}), 200

    except Exception as ex:
        if conexion:
            conexion.rollback()  # Deshacer cambios en caso de error
            conexion.close()
        return jsonify({'error': str(ex), 'mensaje': "Error al eliminar el incidente"}), 500

# Ruta para cargar el archivo CSV y procesarlo
@app.route('/incidentes/upload', methods=['POST'])
def upload_file():
    # Crear la carpeta uploads si no existe
    if not os.path.exists('uploads'):
        os.makedirs('uploads')

    # Recibir el archivo desde la solicitud
    file = request.files['file']
    filepath = './uploads/incident.csv'
    
    # Guardar el archivo en el servidor
    file.save(filepath)

    # Llamar al proceso de transformación
    try:
        # Transformar el archivo CSV
        df = pd.read_csv(filepath, delimiter=',', encoding='unicode_escape')

        # Renombrar las columnas y convertir las fechas
        df = df.rename(columns={
            'Number': 'number',
            'State': 'state',
            'Created': 'created',
            'Last update': 'last_update',
            'Incident CI type': 'incident_ci_type',
            'Affected User': 'affected_user',
            'User location': 'user_location',
            'Assignment Group': 'assignment_group',
            'Assigned to': 'assigned_to',
            'Urgency': 'urgency',
            'Severity': 'severity',
            'Created By': 'created_by',
            'Updated By': 'updated_by'
        })

        # Convertir las columnas 'created' y 'last_update' a tipo datetime
        df['created'] = pd.to_datetime(df['created'], format='%m-%d-%Y %H:%M:%S', errors='coerce')
        df['last_update'] = pd.to_datetime(df['last_update'], format='%m-%d-%Y %H:%M:%S', errors='coerce')

        # Reemplazar los valores NaN por None en todas las columnas
        df = df.where(pd.notnull(df), None)

        # Exportar el DataFrame limpio a un nuevo archivo CSV
        clean_filepath = './uploads/incident_limpio.csv'
        df.to_csv(clean_filepath, index=False, encoding='utf-8')

        # Llamar al proceso de carga
        connection = psycopg2.connect(
            host='localhost',
            user='postgres',
            password='31102003',
            database='proyecto-incident'
        )
        cursor = connection.cursor()

        for index, row in df.iterrows():
            query = """
                INSERT INTO incidents (number, state, created, last_update, incident_ci_type, affected_user,
                                      user_location, assignment_group, assigned_to, urgency, severity, created_by, updated_by)
                VALUES (%s, %s, %s::TIMESTAMP, %s::TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, tuple(row))

        connection.commit()
        cursor.close()
        connection.close()

        return jsonify({'mensaje': 'Archivo procesado y datos importados exitosamente'}), 200
    except Exception as ex:
        return jsonify({'error': str(ex), 'mensaje': 'Error al procesar el archivo'}), 500

if __name__ == '__main__':
    app.run(debug=True)
