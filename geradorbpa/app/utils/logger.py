import logging
from logging.handlers import RotatingFileHandler
import json
import os
from datetime import datetime

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.now().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        if hasattr(record, 'extra'):
            log_record.update(record.extra)
        return json.dumps(log_record)

class BPALogger:
    def __init__(self):
        self.logger = logging.getLogger('bpa')
        self.setup_logging()
    
    def setup_logging(self):
        self.logger.setLevel(logging.INFO)
        
        # Criar diretório de logs se não existir
        os.makedirs('logs', exist_ok=True)
        
        # Handler para arquivo
        file_handler = RotatingFileHandler(
            'logs/bpa.log',
            maxBytes=10485760,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(JSONFormatter())
        
        # Handler para console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(JSONFormatter())
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def log_error(self, error, context=None):
        """Registra um erro no log"""
        self.logger.error(f"Erro: {error}", extra=context or {})
    
    def log_info(self, message, context=None):
        """Registra uma informação no log"""
        self.logger.info(message, extra=context or {})
    
    def log_warning(self, message, context=None):
        """Registra um aviso no log"""
        self.logger.warning(message, extra=context or {})
    
    def log_debug(self, message, context=None):
        """Registra uma mensagem de debug no log"""
        self.logger.debug(message, extra=context or {})

# Instância global do logger
logger = BPALogger() 