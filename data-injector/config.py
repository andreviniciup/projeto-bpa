from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import PostgresDsn, validator
import os
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

class Settings:
    # Configurações da aplicação
    APP_NAME = "Data Injector"
    DEBUG = os.getenv('DEBUG', 'True').lower() == 'true'
    HOST = os.getenv('HOST', '0.0.0.0')
    PORT = int(os.getenv('PORT', 5000))
    
    # Configurações do banco de dados
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://bpa-teste:1336@db:5432/bpa-testes-local')
    DATABASE_SCHEMA = os.getenv('DATABASE_SCHEMA', 'public')
    
    # Configurações de upload
    UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', 'uploads')
    MAX_CONTENT_LENGTH = int(os.getenv('MAX_CONTENT_LENGTH', 16 * 1024 * 1024))  # 16MB
    
    # Configurações de cache
    CACHE_EXPIRE_TIME = int(os.getenv('CACHE_EXPIRE_TIME', 3600))  # 1 hora
    CACHE_CLEANUP_INTERVAL = int(os.getenv('CACHE_CLEANUP_INTERVAL', 300))  # 5 minutos
    
    # Configurações de processamento assíncrono
    ASYNC_WORKERS = int(os.getenv('ASYNC_WORKERS', 4))
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
    MAX_CONCURRENT_TASKS = int(os.getenv('MAX_CONCURRENT_TASKS', 10))
    
    # Configurações de logging
    LOG_FILE = os.getenv('LOG_FILE', 'logs/data_processor.log')
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'DEBUG')
    LOG_FORMAT = os.getenv('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
    LOG_MAX_SIZE = int(os.getenv('LOG_MAX_SIZE', 10485760))  # 10MB
    LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', 5))

# Instância global das configurações
settings = Settings()