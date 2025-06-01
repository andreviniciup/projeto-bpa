from sqlalchemy import text
from app.models.database import SessionLocal
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_real_database_structure():
    """Verifica a estrutura REAL da tabela no banco"""
    with SessionLocal() as session:
        try:
            # Consulta 1: Information Schema
            query1 = text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'rl_procedimento_origem' 
                AND table_schema = 'public'
                ORDER BY ordinal_position
            """)
            result1 = session.execute(query1)
            
            logger.info("🔍 COLUNAS VIA INFORMATION_SCHEMA:")
            for row in result1:
                logger.info(f"  - {row.column_name} ({row.data_type}) - Nullable: {row.is_nullable}")
            
            # Consulta 2: Descrição direta da tabela
            query2 = text("SELECT * FROM public.rl_procedimento_origem LIMIT 0")
            result2 = session.execute(query2)
            
            logger.info("\n🔍 COLUNAS VIA DESCRIBE:")
            for column in result2.keys():
                logger.info(f"  - {column}")
                
        except Exception as e:
            logger.error(f"❌ Erro: {e}")

if __name__ == "__main__":
    check_real_database_structure()
