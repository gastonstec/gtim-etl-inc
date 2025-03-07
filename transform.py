import pandas as pd  # Importa la librería pandas para manipulación de datos

# Lee el archivo CSV, especificando el delimitador como coma y el encoding para evitar errores con caracteres especiales
df = pd.read_csv("C:/Users/negro/Documents/gtim-etl-inc-1/incident.csv", delimiter=',', encoding='unicode_escape')

# Renombra algunas columnas para que coincidan mejor con los estándares o necesidades de la base de datos
df = df.rename(columns={'severity': 'urgency',    # Cambia 'severity' a 'urgency'
                        'severity_1': 'severity', # Cambia 'severity_1' a 'severity'
                        'updated-by': 'updated_by'}) # Reemplaza 'updated-by' con 'updated_by' para mayor compatibilidad

# Convierte la columna 'created' a tipo datetime (timestamp) con el formato especificado (MM-DD-YYYY HH:MM:SS)
df['created'] = pd.to_datetime(df['created'], format='%m-%d-%Y %H:%M:%S')

# Convierte la columna 'last_update' a tipo datetime (timestamp) con el mismo formato
df['last_update'] = pd.to_datetime(df['last_update'], format='%m-%d-%Y %H:%M:%S')

df = df.astype({col: "string" for col in df.select_dtypes(include=["object"]).columns})

# Muestra información general del DataFrame: tipos de datos, valores nulos y uso de memoria
df.info()

# Exportar el DataFrame limpio a un archivo CSV
df.to_csv("C:/Users/negro/Documents/gtim-etl-inc-1/incident_limpio.csv", index=False, encoding='utf-8')

print("Datos exportados con éxito a incident_limpio.csv")
