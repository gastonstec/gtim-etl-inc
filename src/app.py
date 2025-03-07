from flask import Flask, jsonify, request
import psycopg2
import csv
import io
from psycopg2.extras import RealDictCursor
from config import config  # Asegúrate de que este archivo contiene la configuración correcta de PostgreSQL

app = Flask(__name__)
app.config.from_object(config['development'])  # Cargar configuración desde config.py

# Función para obtener una conexión a la base de datos PostgreSQL
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

@app.route('/incidentes', methods=['GET'])
def listar_incidentes():
    conexion = obtener_conexion()
    if conexion is None:
        return jsonify({'mensaje': "Error de conexión a la base de datos"}), 500
    try:
        with conexion.cursor(cursor_factory=RealDictCursor) as cursor:  # Usa cursor con formato diccionario
            sql = """
            SELECT number, state, created, last_update, incident_ci_type, affected_user, 
                   user_location, assignment_group, assigned_to, urgency, severity, 
                   created_by, updated_by 
            FROM incident
            """
            cursor.execute(sql)
            datos = cursor.fetchall()  # Obtener todos los registros
            
        conexion.close()  # Cierra la conexión después de la consulta

        return jsonify({'incidentes': datos, 'mensaje': "Incidentes listados"})  # Retorna los datos en JSON
    
    except Exception as ex:
        return jsonify({'error': str(ex), 'mensaje': "Error al obtener los datos"}), 500

@app.route('/incidentes/<string:number>', methods=['GET'])
def leer_incidente(number):
    conexion = obtener_conexion()
    if conexion is None:
        return jsonify({'mensaje': "Error de conexión a la base de datos"}), 500
    
    try:
        with conexion.cursor(cursor_factory=RealDictCursor) as cursor:  
            sql = """
            SELECT number, state, created, last_update, incident_ci_type, affected_user, 
                   user_location, assignment_group, assigned_to, urgency, severity, 
                   created_by, updated_by 
            FROM incident WHERE number = %s
            """  
            cursor.execute(sql, (number,))  # Se usa un parámetro seguro
            datos = cursor.fetchone()  # Obtener el registro

        if datos:
            return jsonify({'incidente': datos, 'mensaje': "Incidente encontrado"}), 200
        else:
            return jsonify({'mensaje': "Incidente no encontrado"}), 404
    
    except Exception as ex:
        return jsonify({'error': str(ex), 'mensaje': "Error al obtener el incidente"}), 500

@app.route('/incidentes', methods=['POST'])
def agregar_incidente():
    conexion = obtener_conexion()
    if conexion is None:
        return jsonify({'mensaje': "Error de conexión a la base de datos"}), 500
    
    try:
        datos = request.get_json()  # Recibir datos en formato JSON
        
        with conexion.cursor() as cursor:
            sql = """
            INSERT INTO incident (number, state, created, last_update, incident_ci_type, affected_user, 
                                  user_location, assignment_group, assigned_to, urgency, severity, 
                                  created_by, updated_by)
            VALUES (%s, %s, %s::TIMESTAMP, %s::TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                datos['number'], datos['state'], datos['created'], datos['last_update'], datos['incident_ci_type'],
                datos['affected_user'], datos['user_location'], datos['assignment_group'], datos['assigned_to'],
                datos['urgency'], datos['severity'], datos['created_by'], datos['updated_by']
            ))
            conexion.commit()  # Guardar cambios en la base de datos
        
        conexion.close()
        return jsonify({'mensaje': "Incidente agregado exitosamente"}), 201
    
    except Exception as ex:
        if conexion:
            conexion.rollback()  # Deshacer cambios en caso de error
            conexion.close()
        return jsonify({'error': str(ex), 'mensaje': "Error al agregar el incidente"}), 500
    
@app.route('/incidentes/<string:number>', methods=['PUT'])
def actualizar_incidente(number):
    conexion = obtener_conexion()
    if conexion is None:
        return jsonify({'mensaje': "Error de conexión a la base de datos"}), 500
    
    try:
        datos = request.get_json()  # Recibir datos en formato JSON
        
        # Verificar si el incidente existe antes de intentar actualizarlo
        with conexion.cursor() as cursor:
            sql_select = """
            SELECT number FROM incident WHERE number = %s
            """
            cursor.execute(sql_select, (number,))
            incidente_existente = cursor.fetchone()
            
            if not incidente_existente:
                return jsonify({'mensaje': "Incidente no encontrado"}), 404
        
        # Actualizar los datos del incidente
        with conexion.cursor() as cursor:
            sql_update = """
            UPDATE incident 
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
            conexion.commit()  # Guardar cambios en la base de datos
        
        conexion.close()
        return jsonify({'mensaje': "Incidente actualizado exitosamente"}), 200
    
    except Exception as ex:
        if conexion:
            conexion.rollback()  # Deshacer cambios en caso de error
            conexion.close()
        return jsonify({'error': str(ex), 'mensaje': "Error al actualizar el incidente"}), 500

@app.route('/incidentes/<string:number>', methods=['DELETE'])
def eliminar_incidente(number):
    conexion = obtener_conexion()
    if conexion is None:
        return jsonify({'mensaje': "Error de conexión a la base de datos"}), 500
    
    try:
        # Verificar si el incidente existe antes de intentar eliminarlo
        with conexion.cursor() as cursor:
            sql_select = """
            SELECT number FROM incident WHERE number = %s
            """
            cursor.execute(sql_select, (number,))
            incidente_existente = cursor.fetchone()
            
            if not incidente_existente:
                return jsonify({'mensaje': "Incidente no encontrado"}), 404
        
        # Eliminar el incidente
        with conexion.cursor() as cursor:
            sql_delete = """
            DELETE FROM incident WHERE number = %s
            """
            cursor.execute(sql_delete, (number,))
            conexion.commit()  # Guardar cambios en la base de datos
        
        conexion.close()
        return jsonify({'mensaje': "Incidente eliminado exitosamente"}), 200
    
    except Exception as ex:
        if conexion:
            conexion.rollback()  # Deshacer cambios en caso de error
            conexion.close()
        return jsonify({'error': str(ex), 'mensaje': "Error al eliminar el incidente"}), 500


# Manejo de error 404
@app.errorhandler(404)
def pagina_no_encontrada(error):
    return jsonify({'error': 'La página que estás buscando no existe'}), 404

if __name__ == '__main__':
    app.run(debug=True)  # Habilita modo debug para ver errores
