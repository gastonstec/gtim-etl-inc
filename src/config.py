class DevelopmentConfig:
    DEBUG = True
    MYSQL_HOST = 'localhost'
    MYSQL_USER = 'postgres'
    MYSQL_PASSWORD = '31102003'
    MYSQL_DB = 'proyecto-incident'

config = {
    'development': DevelopmentConfig
}