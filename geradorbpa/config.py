import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Configurações do Banco de Dados
    DB_USER: str = os.getenv('DB_USER')
    DB_PASSWORD: str = os.getenv('DB_PASSWORD')
    DB_HOST: str = os.getenv('DB_HOST')
    DB_PORT: str = os.getenv('DB_PORT')
    DB_NAME: str = os.getenv('DB_NAME')
    DATABASE_SCHEMA: str = os.getenv('DATABASE_SCHEMA')
    
    # Configurações do Flask
    FLASK_PORT: str = os.getenv('FLASK_PORT', '5000')
    
    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    # Chave secreta para sessões
    SECRET_KEY: str = os.getenv('SECRET_KEY')
    
    # Configurações da Aplicação
    DEBUG: bool = os.getenv('DEBUG', 'True').lower() == 'true'
    
    class Config:
        env_file = '.env'

settings = Settings() 