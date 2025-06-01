from typing import List, Dict, Any, AsyncGenerator
import asyncio
import aiofiles
import pandas as pd
from app.utils.logger import app_logger
from app.services.data_comparator import DataComparator
from app.repositories.base import BaseRepository
from config import settings

class AsyncFileProcessor:
    """
    Processador assíncrono de arquivos.
    """
    
    def __init__(self, repository: BaseRepository, key_fields: List[str]):
        """
        Inicializa o processador.
        
        Args:
            repository: Repositório para acesso ao banco de dados
            key_fields: Campos que identificam unicamente um registro
        """
        self.logger = app_logger
        self.batch_size = settings.BATCH_SIZE
        self.max_concurrent_tasks = settings.MAX_CONCURRENT_TASKS
        self.comparator = DataComparator(repository)
        self.key_fields = key_fields
        self.semaphore = asyncio.Semaphore(self.max_concurrent_tasks)

    async def process_file(self, file_path: str) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Processa um arquivo de forma assíncrona.
        
        Args:
            file_path: Caminho do arquivo a ser processado
            
        Yields:
            Dict[str, Any]: Resultado do processamento de cada lote
        """
        try:
            async with aiofiles.open(file_path, mode='r') as file:
                batch = []
                async for line in file:
                    processed_line = self._process_line(line)
                    if processed_line:
                        batch.append(processed_line)
                        
                        if len(batch) >= self.batch_size:
                            result = await self.process_batch(batch)
                            yield result
                            batch = []
                            
                if batch:
                    result = await self.process_batch(batch)
                    yield result
                    
        except Exception as e:
            self.logger.error(f"Erro ao processar arquivo {file_path}: {str(e)}")
            raise

    def _process_line(self, line: str) -> Dict[str, Any]:
        """
        Processa uma linha do arquivo.
        
        Args:
            line: Linha do arquivo
            
        Returns:
            Dict[str, Any]: Dados processados
        """
        try:
            # Implemente aqui a lógica específica de processamento da linha
            # Este é um exemplo genérico
            data = line.strip().split(',')
            return {
                'field1': data[0],
                'field2': data[1],
                # ... outros campos
            }
        except Exception as e:
            self.logger.error(f"Erro ao processar linha: {str(e)}")
            return None

    async def process_batch(self, batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Processa um lote de dados.
        
        Args:
            batch: Lista de dados a serem processados
            
        Returns:
            Dict[str, Any]: Resultado do processamento
        """
        try:
            async with self.semaphore:
                # Compara e atualiza os dados
                inserted, updated, unchanged = self.comparator.compare_and_update(
                    batch,
                    self.key_fields
                )
                
                return {
                    'status': 'success',
                    'inserted': inserted,
                    'updated': updated,
                    'unchanged': unchanged,
                    'total': len(batch)
                }
                
        except Exception as e:
            self.logger.error(f"Erro ao processar lote: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'total': len(batch)
            }

# Instância global do processador
async_processor = None  # Será inicializado com o repositório apropriado 