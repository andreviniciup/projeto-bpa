from sqlalchemy import text
from app.models.database import SessionLocal
from config import settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_table_query():
    """Debug da consulta que está sendo usada no DataValidator"""
    with SessionLocal() as session:
        try:
            # A mesma consulta que está sendo usada
            query = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = :table 
                AND table_schema = :schema
            """)
            
            table_name = 'rl_procedimento_origem'
            schema = settings.DATABASE_SCHEMA
            
            logger.info(f"🔍 Executando consulta:")
            logger.info(f"  - table_name: {table_name}")
            logger.info(f"  - schema: {schema}")
            logger.info(f"  - DATABASE_SCHEMA: {settings.DATABASE_SCHEMA}")
            
            result = session.execute(query, {'table': table_name, 'schema': schema})
            columns = [row.column_name for row in result]
            
            logger.info(f"🎯 Resultado: {columns}")
            
            # Teste com schema diferente
            logger.info("\n🔍 Testando com outros schemas:")
            schemas_to_test = ['public', 'postgres', settings.DATABASE_SCHEMA]
            
            for test_schema in schemas_to_test:
                result = session.execute(query, {'table': table_name, 'schema': test_schema})
                cols = [row.column_name for row in result]
                logger.info(f"  - Schema '{test_schema}': {cols}")
                
        except Exception as e:
            logger.error(f"❌ Erro: {e}")

if __name__ == "__main__":
    debug_table_query()
