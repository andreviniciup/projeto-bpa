from app import create_app
from app.models.database import init_db
from config import settings
import logging
import os

# Garante que o diretório de logs existe
os.makedirs('logs', exist_ok=True)

# Configuração do logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format=settings.LOG_FORMAT,
    filename=settings.LOG_FILE,
    filemode='a'  # Modo append para não sobrescrever logs anteriores
)

# Configura também o logging para console
console_handler = logging.StreamHandler()
console_handler.setLevel(getattr(logging, settings.LOG_LEVEL))
console_handler.setFormatter(logging.Formatter(settings.LOG_FORMAT))
logging.getLogger().addHandler(console_handler)

# Cria a aplicação Flask
app = create_app()

# Inicializa o banco de dados
init_db()

if __name__ == '__main__':
    logging.info(f"Iniciando aplicação em http://{settings.HOST}:{settings.PORT}")
    app.run(
        host=settings.HOST,
        port=settings.PORT,
        debug=settings.DEBUG,
        use_reloader=True
    )