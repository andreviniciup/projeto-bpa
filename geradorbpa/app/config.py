from pydantic_settings import BaseSettings
from typing import Optional
from functools import lru_cache


class Settings(BaseSettings):
    # Configurações do banco de dados
    DB_HOST: str = "localhost"
    DB_PORT: str = "5432"
    DB_NAME: str = "geradorbpa"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"
    DATABASE_SCHEMA: str = "public"

    # Configurações da aplicação
    SECRET_KEY: str = "your-secret-key"
    DEBUG: bool = True
    FLASK_PORT: str = "8080"

    # Configurações do BPA
    BPA_BATCH_SIZE: int = 1000
    BPA_MAX_LINES_PER_PAGE: int = 50

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "allow"  # Permite campos extras


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings() 