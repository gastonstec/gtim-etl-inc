# Definición de la clase DevelopmentConfig que almacena la configuración para el entorno de desarrollo
class DevelopmentConfig:
    DEBUG = True  # Habilita el modo de depuración (debugging) en el entorno de desarrollo

# Creación de un diccionario de configuración con el entorno 'development' apuntando a la clase DevelopmentConfig
config = {
    'development': DevelopmentConfig  # Utiliza la configuración de desarrollo para este entorno
}
