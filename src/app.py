from flask import Flask, jsonify
import psycopg2
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

# Manejo de error 404
@app.errorhandler(404)
def pagina_no_encontrada(error):
    return jsonify({'error': 'La página que estás buscando no existe'}), 404

if __name__ == '__main__':
    app.run(debug=True)  # Habilita modo debug para ver errores
