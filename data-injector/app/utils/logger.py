import logging
from logging.handlers import RotatingFileHandler
import os
from config import settings

def setup_logger(name: str) -> logging.Logger:
    """
    Configura e retorna um logger com rotação de arquivos.
    
    Args:
        name: Nome do logger
        
    Returns:
        logging.Logger: Logger configurado
    """
    # Cria o diretório de logs se não existir
    log_dir = os.path.dirname(settings.LOG_FILE)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    
    # Cria o logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, settings.LOG_LEVEL))
    
    # Configura o formato do log
    formatter = logging.Formatter(settings.LOG_FORMAT)
    
    # Handler para arquivo com rotação
    file_handler = RotatingFileHandler(
        settings.LOG_FILE,
        maxBytes=settings.LOG_MAX_SIZE,
        backupCount=settings.LOG_BACKUP_COUNT
    )
    file_handler.setFormatter(formatter)
    
    # Handler para console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Adiciona os handlers ao logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Logger global para a aplicação
app_logger = setup_logger("data_injector") 