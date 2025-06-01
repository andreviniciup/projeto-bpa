from typing import Any, Optional, Dict
import time
from threading import Lock
from app.utils.logger import app_logger

class Cache:
    """
    Classe para gerenciamento de cache em memória.
    """
    
    def __init__(self):
        """
        Inicializa o cache em memória.
        """
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()
        self.logger = app_logger

    def get(self, key: str) -> Optional[Any]:
        """
        Obtém um valor do cache.
        
        Args:
            key: Chave do cache
            
        Returns:
            Optional[Any]: Valor armazenado ou None
        """
        try:
            with self._lock:
                if key not in self._cache:
                    return None
                    
                item = self._cache[key]
                if item['expires_at'] and time.time() > item['expires_at']:
                    del self._cache[key]
                    return None
                    
                return item['value']
                
        except Exception as e:
            self.logger.error(f"Erro ao obter valor do cache para chave {key}: {str(e)}")
            return None

    def set(self, key: str, value: Any, expire: int = 3600) -> bool:
        """
        Armazena um valor no cache.
        
        Args:
            key: Chave do cache
            value: Valor a ser armazenado
            expire: Tempo de expiração em segundos
            
        Returns:
            bool: True se o valor foi armazenado com sucesso
        """
        try:
            with self._lock:
                self._cache[key] = {
                    'value': value,
                    'expires_at': time.time() + expire if expire else None
                }
                return True
                
        except Exception as e:
            self.logger.error(f"Erro ao armazenar valor no cache para chave {key}: {str(e)}")
            return False

    def delete(self, key: str) -> bool:
        """
        Remove um valor do cache.
        
        Args:
            key: Chave do cache
            
        Returns:
            bool: True se o valor foi removido com sucesso
        """
        try:
            with self._lock:
                if key in self._cache:
                    del self._cache[key]
                    return True
                return False
                
        except Exception as e:
            self.logger.error(f"Erro ao remover valor do cache para chave {key}: {str(e)}")
            return False

    def clear_pattern(self, pattern: str) -> bool:
        """
        Remove todos os valores que correspondem a um padrão.
        
        Args:
            pattern: Padrão de chaves a serem removidas
            
        Returns:
            bool: True se os valores foram removidos com sucesso
        """
        try:
            with self._lock:
                keys_to_delete = [
                    key for key in self._cache.keys()
                    if pattern in key
                ]
                for key in keys_to_delete:
                    del self._cache[key]
                return True
                
        except Exception as e:
            self.logger.error(f"Erro ao limpar cache com padrão {pattern}: {str(e)}")
            return False

    def clear_expired(self) -> None:
        """
        Remove todos os valores expirados do cache.
        """
        try:
            with self._lock:
                current_time = time.time()
                expired_keys = [
                    key for key, item in self._cache.items()
                    if item['expires_at'] and current_time > item['expires_at']
                ]
                for key in expired_keys:
                    del self._cache[key]
                    
        except Exception as e:
            self.logger.error(f"Erro ao limpar cache expirado: {str(e)}")

# Instância global do cache
cache = Cache() 