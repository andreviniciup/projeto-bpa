from typing import Generic, TypeVar, List, Optional, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select as async_select
from sqlalchemy import update, delete
from app.utils.logger import app_logger

T = TypeVar('T')

class AsyncBaseRepository(Generic[T]):
    """
    Classe base para repositórios assíncronos que implementa operações CRUD básicas.
    
    Args:
        Generic[T]: Tipo genérico que representa a entidade do repositório
    """
    
    def __init__(self, session: AsyncSession, model_class: type[T]):
        """
        Inicializa o repositório assíncrono.
        
        Args:
            session: Sessão assíncrona do SQLAlchemy
            model_class: Classe do modelo SQLAlchemy
        """
        self.session = session
        self.model_class = model_class
        self.logger = app_logger

    async def get(self, id: Any) -> Optional[T]:
        """
        Busca uma entidade pelo ID de forma assíncrona.
        
        Args:
            id: ID da entidade
            
        Returns:
            Optional[T]: Entidade encontrada ou None
        """
        try:
            query = async_select(self.model_class).where(self.model_class.id == id)
            result = await self.session.execute(query)
            return result.scalar_one_or_none()
        except Exception as e:
            self.logger.error(f"Erro ao buscar entidade {self.model_class.__name__} com ID {id}: {str(e)}")
            raise

    async def get_all(self, skip: int = 0, limit: int = 100) -> List[T]:
        """
        Busca todas as entidades com paginação de forma assíncrona.
        
        Args:
            skip: Número de registros para pular
            limit: Limite de registros por página
            
        Returns:
            List[T]: Lista de entidades
        """
        try:
            query = async_select(self.model_class).offset(skip).limit(limit)
            result = await self.session.execute(query)
            return list(result.scalars().all())
        except Exception as e:
            self.logger.error(f"Erro ao buscar todas as entidades {self.model_class.__name__}: {str(e)}")
            raise

    async def create(self, obj_in: dict) -> T:
        """
        Cria uma nova entidade de forma assíncrona.
        
        Args:
            obj_in: Dicionário com os dados da entidade
            
        Returns:
            T: Entidade criada
        """
        try:
            db_obj = self.model_class(**obj_in)
            self.session.add(db_obj)
            await self.session.commit()
            await self.session.refresh(db_obj)
            return db_obj
        except Exception as e:
            await self.session.rollback()
            self.logger.error(f"Erro ao criar entidade {self.model_class.__name__}: {str(e)}")
            raise

    async def update(self, id: Any, obj_in: dict) -> Optional[T]:
        """
        Atualiza uma entidade existente de forma assíncrona.
        
        Args:
            id: ID da entidade
            obj_in: Dicionário com os dados a serem atualizados
            
        Returns:
            Optional[T]: Entidade atualizada ou None
        """
        try:
            query = update(self.model_class).where(self.model_class.id == id).values(**obj_in)
            await self.session.execute(query)
            await self.session.commit()
            return await self.get(id)
        except Exception as e:
            await self.session.rollback()
            self.logger.error(f"Erro ao atualizar entidade {self.model_class.__name__} com ID {id}: {str(e)}")
            raise

    async def delete(self, id: Any) -> bool:
        """
        Remove uma entidade de forma assíncrona.
        
        Args:
            id: ID da entidade
            
        Returns:
            bool: True se a entidade foi removida, False caso contrário
        """
        try:
            query = delete(self.model_class).where(self.model_class.id == id)
            result = await self.session.execute(query)
            await self.session.commit()
            return result.rowcount > 0
        except Exception as e:
            await self.session.rollback()
            self.logger.error(f"Erro ao deletar entidade {self.model_class.__name__} com ID {id}: {str(e)}")
            raise 