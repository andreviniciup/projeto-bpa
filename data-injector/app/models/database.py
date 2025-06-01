from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import settings

# Cria o engine do SQLAlchemy
engine = create_engine(settings.DATABASE_URL)

# Cria a sessão
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base para modelos
Base = declarative_base(metadata=MetaData(schema=settings.DATABASE_SCHEMA))

# Modelo que reflete a estrutura real da tabela
class ProcedimentoOrigem(Base):
    __tablename__ = "rl_procedimento_origem"
    __table_args__ = {'schema': settings.DATABASE_SCHEMA}
    
    id = Column(Integer, primary_key=True, index=True)
    co_procedimento = Column(String(255))
    co_procedimento_origem = Column(String(255))
    dt_competencia = Column(Date)

def get_db():
    """
    Função para obter uma sessão do banco de dados.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    """
    Cria todas as tabelas no banco de dados conforme os modelos definidos.
    """
    Base.metadata.create_all(bind=engine)