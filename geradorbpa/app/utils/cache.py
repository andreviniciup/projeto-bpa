import time
from typing import Any, Optional
from app.utils.logger import logger

class BPACache:
    def __init__(self, ttl_seconds: int = 300):
        """
        Inicializa o cache
        
        Args:
            ttl_seconds: Tempo de vida dos itens em cache em segundos (padrão: 5 minutos)
        """
        self._cache = {}
        self._ttl = ttl_seconds
    
    def get(self, key: str) -> Optional[Any]:
        """
        Obtém um valor do cache
        
        Args:
            key: Chave do item no cache
            
        Returns:
            O valor armazenado ou None se não existir ou estiver expirado
        """
        if key in self._cache:
            item = self._cache[key]
            if not self._is_expired(item['timestamp']):
                logger.log_debug(f"Cache hit para chave: {key}")
                return item['value']
            else:
                logger.log_debug(f"Cache expirado para chave: {key}")
                self.remove(key)
        return None
    
    def set(self, key: str, value: Any) -> None:
        """
        Armazena um valor no cache
        
        Args:
            key: Chave para armazenar o valor
            value: Valor a ser armazenado
        """
        self._cache[key] = {
            'value': value,
            'timestamp': time.time()
        }
        logger.log_debug(f"Item armazenado no cache: {key}")
    
    def remove(self, key: str) -> None:
        """
        Remove um item do cache
        
        Args:
            key: Chave do item a ser removido
        """
        if key in self._cache:
            del self._cache[key]
            logger.log_debug(f"Item removido do cache: {key}")
    
    def clear(self) -> None:
        """Limpa todo o cache"""
        self._cache.clear()
        logger.log_debug("Cache limpo")
    
    def _is_expired(self, timestamp: float) -> bool:
        """
        Verifica se um timestamp está expirado
        
        Args:
            timestamp: Timestamp a ser verificado
            
        Returns:
            True se estiver expirado, False caso contrário
        """
        return time.time() - timestamp > self._ttl

# Instância global do cache
cache = BPACache() 