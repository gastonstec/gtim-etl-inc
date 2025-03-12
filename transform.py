# Importa la librería pandas para manipulación de datos
import pandas as pd  

# Lee el archivo CSV especificando el delimitador (coma) y el encoding 'unicode_escape' 
# para evitar errores con caracteres especiales en el archivo
df = pd.read_csv("C:/Users/negro/Documents/gtim-etl-inc-1/incident.csv", delimiter=',', encoding='unicode_escape')

# Se renombra las columnas del DataFrame para estandarizar los nombres y mejorar la compatibilidad con PostgreSQL
df = df.rename(columns={
    'Number': 'number',  # Convierte 'Number' a 'number' en minúsculas
    'State': 'state',  # Convierte 'State' a 'state' en minúsculas
    'Created': 'created',  # Convierte 'Created' a 'created' en minúsculas para uniformidad
    'Last update': 'last_update',  # Cambia 'Last update' a 'last_update', reemplazando espacios por guion bajo
    'Incident CI type': 'incident_ci_type',  # Convierte 'Incident CI type' a un formato compatible con bases de datos
    'Affected User': 'affected_user',  # Convierte 'Affected User' a 'affected_user' en minúsculas con guion bajo
    'User location': 'user_location',  # Convierte 'User location' a 'user_location' para evitar espacios
    'Assignment Group': 'assignment_group',  # Convierte 'Assignment Group' a 'assignment_group'
    'Assigned to': 'assigned_to',  # Convierte 'Assigned to' a 'assigned_to'
    'Urgency': 'urgency',  # Modifica 'Severity' a 'urgency' para reflejar mejor el significado de los datos
    'Severity': 'severity',  # Modifica 'Severity_1' a 'severity' para unificar nombres de columnas relacionadas
    'Created By': 'created_by',  # Convierte 'Created By' a 'created_by', estandarizando con guion bajo
    'Updated By': 'updated_by'  # Convierte 'Updated By' a 'updated_by', eliminando espacios
})

# Convierte la columna 'created' a tipo datetime (timestamp) con el formato especificado (MM-DD-YYYY HH:MM:SS)
df['created'] = pd.to_datetime(df['created'], format='%m-%d-%Y %H:%M:%S')

# Convierte la columna 'last_update' a tipo datetime (timestamp) con el mismo formato
df['last_update'] = pd.to_datetime(df['last_update'], format='%m-%d-%Y %H:%M:%S')

# Convierte todas las columnas de tipo 'object' (texto) a tipo string, lo que garantiza la uniformidad en las cadenas de texto
df = df.astype({col: "string" for col in df.select_dtypes(include=["object"]).columns})

# Muestra información general del DataFrame, incluyendo tipos de datos, cantidad de valores nulos y uso de memoria
df.info()

# Exporta el DataFrame limpio a un archivo CSV sin incluir el índice de las filas y utilizando el encoding 'utf-8'
df.to_csv("C:/Users/negro/Documents/gtim-etl-inc-1/incident_limpio.csv", index=False, encoding='utf-8')

# Imprime un mensaje confirmando que los datos se exportaron correctamente
print("Datos exportados con éxito a incident_limpio.csv")
