from typing import List, Dict, Any, Tuple, Set
import pandas as pd
from app.utils.logger import app_logger
from app.repositories.base import BaseRepository

class DataComparator:
    """
    Serviço para comparar e atualizar dados de forma eficiente.
    """
    
    def __init__(self, repository: BaseRepository):
        """
        Inicializa o comparador de dados.
        
        Args:
            repository: Repositório para acesso ao banco de dados
        """
        self.repository = repository
        self.logger = app_logger

    def compare_and_update(self, new_data: List[Dict[str, Any]], key_fields: List[str]) -> Tuple[int, int, int]:
        """
        Compara os novos dados com os existentes e atualiza conforme necessário.
        
        Args:
            new_data: Lista de novos dados a serem comparados
            key_fields: Lista de campos que identificam unicamente um registro
            
        Returns:
            Tuple[int, int, int]: (registros_inseridos, registros_atualizados, registros_iguais)
        """
        try:
            # Converte os novos dados para DataFrame
            df_new = pd.DataFrame(new_data)
            
            # Busca dados existentes
            existing_data = self.repository.get_all()
            df_existing = pd.DataFrame([vars(item) for item in existing_data])
            
            if df_existing.empty:
                # Se não há dados existentes, insere todos
                self._insert_all(new_data)
                return len(new_data), 0, 0
            
            # Identifica registros a serem inseridos, atualizados ou mantidos
            to_insert, to_update, unchanged = self._identify_changes(
                df_new, 
                df_existing, 
                key_fields
            )
            
            # Processa as alterações
            inserted = self._insert_all(to_insert)
            updated = self._update_changed(to_update)
            
            return inserted, updated, len(unchanged)
            
        except Exception as e:
            self.logger.error(f"Erro ao comparar e atualizar dados: {str(e)}")
            raise

    def _identify_changes(
        self, 
        df_new: pd.DataFrame, 
        df_existing: pd.DataFrame, 
        key_fields: List[str]
    ) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """
        Identifica registros a serem inseridos, atualizados ou mantidos.
        
        Args:
            df_new: DataFrame com novos dados
            df_existing: DataFrame com dados existentes
            key_fields: Campos chave para comparação
            
        Returns:
            Tuple[List[Dict], List[Dict], List[Dict]]: (inserir, atualizar, manter)
        """
        try:
            # Cria chaves compostas para comparação
            df_new['_key'] = df_new[key_fields].apply(
                lambda x: '|'.join(x.astype(str)), axis=1
            )
            df_existing['_key'] = df_existing[key_fields].apply(
                lambda x: '|'.join(x.astype(str)), axis=1
            )
            
            # Identifica registros novos
            new_keys = set(df_new['_key'])
            existing_keys = set(df_existing['_key'])
            to_insert_keys = new_keys - existing_keys
            
            # Identifica registros existentes
            to_update_keys = new_keys & existing_keys
            
            # Separa os dados
            to_insert = df_new[df_new['_key'].isin(to_insert_keys)].to_dict('records')
            
            # Compara registros existentes
            to_update = []
            unchanged = []
            
            for key in to_update_keys:
                new_row = df_new[df_new['_key'] == key].iloc[0]
                existing_row = df_existing[df_existing['_key'] == key].iloc[0]
                
                # Compara campos não-chave
                non_key_fields = [f for f in new_row.index if f not in key_fields + ['_key']]
                changes = {
                    field: new_row[field]
                    for field in non_key_fields
                    if new_row[field] != existing_row[field]
                }
                
                if changes:
                    # Se há mudanças, adiciona aos registros a atualizar
                    to_update.append({
                        'key': {k: new_row[k] for k in key_fields},
                        'changes': changes
                    })
                else:
                    # Se não há mudanças, adiciona aos registros inalterados
                    unchanged.append(new_row.to_dict())
            
            return to_insert, to_update, unchanged
            
        except Exception as e:
            self.logger.error(f"Erro ao identificar mudanças: {str(e)}")
            raise

    def _insert_all(self, data: List[Dict[str, Any]]) -> int:
        """
        Insere todos os registros novos.
        
        Args:
            data: Lista de registros a serem inseridos
            
        Returns:
            int: Número de registros inseridos
        """
        try:
            inserted = 0
            for item in data:
                self.repository.create(item)
                inserted += 1
            return inserted
            
        except Exception as e:
            self.logger.error(f"Erro ao inserir registros: {str(e)}")
            raise

    def _update_changed(self, updates: List[Dict[str, Any]]) -> int:
        """
        Atualiza apenas os campos que mudaram.
        
        Args:
            updates: Lista de atualizações a serem feitas
            
        Returns:
            int: Número de registros atualizados
        """
        try:
            updated = 0
            for update in updates:
                # Busca o registro pelo ID
                key_values = update['key']
                record = self.repository.get(key_values)
                
                if record:
                    # Atualiza apenas os campos que mudaram
                    self.repository.update(record.id, update['changes'])
                    updated += 1
                    
            return updated
            
        except Exception as e:
            self.logger.error(f"Erro ao atualizar registros: {str(e)}")
            raise 