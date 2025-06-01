from config import settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_configuration():
    """Verifica as configurações atuais"""
    try:
        logger.info("🔍 CONFIGURAÇÕES ATUAIS:")
        logger.info(f"  - DATABASE_URL: {settings.DATABASE_URL}")
        logger.info(f"  - DATABASE_SCHEMA: {settings.DATABASE_SCHEMA}")
        
        # Verifica se há outras variáveis relacionadas ao banco
        for attr in dir(settings):
            if 'DATABASE' in attr.upper() or 'DB' in attr.upper():
                value = getattr(settings, attr)
                logger.info(f"  - {attr}: {value}")
                
    except Exception as e:
        logger.error(f"❌ Erro ao verificar configurações: {e}")

if __name__ == "__main__":
    debug_configuration()
