import pandas as pd
import re
import json
import logging
import tempfile
import os
from typing import List, Dict, Any, Optional
from sqlalchemy import text, inspect
from app.models.database import engine, SessionLocal
from config import settings

logger = logging.getLogger("DataValidator")

class DataValidator:
    def __init__(self):
        self.logger = logging.getLogger("DataValidator")
    
    def _normalize_column_name(self, column_name: str) -> str:
        """
        Converte nomes de colunas do layout (MAIÚSCULO) para o formato do banco (snake_case)
        """
        return column_name.lower().replace(' ', '_')

    def _get_column_mapping(self, layout_columns: List[str], db_columns: List[str]) -> Dict[str, str]:
        """
        Cria um mapeamento entre colunas do layout e colunas do banco
        """
        mapping = {}
        for layout_col in layout_columns:
            normalized = self._normalize_column_name(layout_col)
            if normalized in db_columns:
                mapping[layout_col] = normalized
        return mapping

    def _get_simplified_table_columns(self, session, table_name: str, schema: str) -> List[str]:
        """
        Obtém as colunas da tabela usando consulta simplificada
        """
        query = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = :table 
            AND table_schema = :schema
        """)
        result = session.execute(query, {'table': table_name, 'schema': schema})
        return [row.column_name for row in result]

    def parse_layout_file(self, layout_file_path: str) -> Dict[str, Any]:
        """
        Analisa o arquivo de layout TXT e retorna as informações das colunas
        """
        try:
            columns = []
            with open(layout_file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()
                
                # Pula a primeira linha (header)
                for line in lines[1:]:
                    line = line.strip()
                    if line:  # Ignora linhas vazias
                        # Split por vírgula (formato CSV simples)
                        parts = line.split(',')
                        if len(parts) >= 1:
                            column_name = parts[0].strip()
                            if column_name:
                                columns.append(column_name)
            
            return {'columns': columns}
            
        except Exception as e:
            self.logger.error(f"Erro ao analisar arquivo de layout: {str(e)}")
            return {'error': str(e)}

    def validate_table_structure(self, table_name: str, layout_file_path: str) -> Dict[str, Any]:
        """
        Valida se a estrutura da tabela no banco corresponde ao layout do arquivo
        """
        try:
            layout_data = self.parse_layout_file(layout_file_path)
            if 'error' in layout_data:
                return layout_data

            layout_columns = layout_data['columns']
            self.logger.info(f"Layout carregado com {len(layout_columns)} colunas")

            with SessionLocal() as session:
                try:
                    # Busca colunas do banco (excluindo 'id')
                    db_columns = self._get_simplified_table_columns(session, table_name, settings.DATABASE_SCHEMA)
                    db_columns = [col for col in db_columns if col != 'id']
                    
                    self.logger.info(f"Colunas do banco de dados: {db_columns}")
                    self.logger.info(f"Colunas do layout: {layout_columns}")
                    
                    # Cria mapeamento entre layout e banco
                    column_mapping = self._get_column_mapping(layout_columns, db_columns)
                    self.logger.info(f"Mapeamento de colunas: {column_mapping}")
                    
                    # Verifica se todas as colunas do layout têm correspondência
                    missing_columns = []
                    for layout_col in layout_columns:
                        if layout_col not in column_mapping:
                            missing_columns.append(layout_col)
                    
                    if missing_columns:
                        self.logger.error(f"Colunas do layout sem correspondência no banco: {missing_columns}")
                        return {
                            'valid': False, 
                            'error': f"Colunas do layout não encontradas no banco: {missing_columns}",
                            'missing_columns': missing_columns
                        }

                    return {
                        'valid': True, 
                        'layout_columns': layout_columns,
                        'db_columns': db_columns,
                        'column_mapping': column_mapping
                    }
                    
                except Exception as e:
                    self.logger.error(f"Erro ao acessar tabela {table_name}: {str(e)}")
                    return {'valid': False, 'error': f"Erro ao validar tabela {table_name}: {str(e)}"}
                    
        except Exception as e:
            self.logger.error(f"Erro ao validar estrutura da tabela {table_name}: {str(e)}")
            return {'valid': False, 'error': str(e)}


def validate_database_schema(table_name: str, layout_file: str) -> bool:
    """
    Valida se o schema da tabela corresponde ao layout do arquivo.
    
    Args:
        table_name: Nome da tabela
        layout_file: Caminho do arquivo de layout
        
    Returns:
        bool: True se o schema é válido, False caso contrário
    """
    try:
        # Usa a nova função de validação que já está implementada corretamente
        return validate_database_schema_new(table_name, layout_file)
            
    except Exception as e:
        logger.error(f"Erro na validação do schema: {str(e)}")
        return False

def parse_layout_file(layout_file_path: str) -> List[Dict[str, Any]]:
    """
    Analisa o arquivo de layout em formato CSV e retorna uma lista de dicionários com as configurações.
    
    Args:
        layout_file_path: Caminho do arquivo de layout
        
    Returns:
        Lista de dicionários com as configurações das colunas
    """
    try:
        columns = []
        with open(layout_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
            # Pula a primeira linha (header)
            for line in lines[1:]:
                line = line.strip()
                if line:  # Ignora linhas vazias
                    # Split por vírgula (formato CSV simples)
                    parts = line.split(',')
                    if len(parts) >= 5:  # Garante que tem todas as colunas necessárias
                        column = {
                            'Coluna': parts[0].strip(),
                            'Tamanho': int(parts[1].strip()) if parts[1].strip().isdigit() else 255,
                            'Inicio': int(parts[2].strip()) if parts[2].strip().isdigit() else 1,
                            'Fim': int(parts[3].strip()) if parts[3].strip().isdigit() else 255,
                            'Tipo': parts[4].strip() if len(parts) > 4 else 'CHAR'
                        }
                        columns.append(column)
        
        logger.info(f"Layout carregado com {len(columns)} colunas")
        return columns
    except Exception as e:
        logger.error(f"Erro ao analisar arquivo de layout: {str(e)}")
        raise

def check_table_exists(table_name: str) -> bool:
    """
    Valida se a tabela existe no banco de dados.
    
    Args:
        table_name: Nome da tabela
        
    Returns:
        Booleano indicando se a tabela existe
    """
    try:
        with SessionLocal() as session:
            # Verifica se a tabela existe
            query = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = :schema 
                    AND table_name = :table
                )
            """)
            result = session.execute(query, {
                'schema': settings.DATABASE_SCHEMA,
                'table': table_name
            }).scalar()
            
            if not result:
                logger.error(f"Tabela {table_name} não encontrada no banco de dados")
                return False
                
            return True
            
    except Exception as e:
        logger.error(f"Erro ao validar esquema do banco de dados: {str(e)}")
        return False

def validate_fixed_width_data(data_file_path: str, layout_columns: List[Dict[str, Any]], encoding: str = None) -> bool:
    """
    Valida se os dados do arquivo correspondem ao layout especificado.
    
    Args:
        data_file_path: Caminho do arquivo de dados
        layout_columns: Lista de dicionários com as configurações das colunas
        encoding: Codificação do arquivo (opcional)
        
    Returns:
        Booleano indicando se os dados são válidos
    """
    try:
        # Lê o arquivo de dados
        with open(data_file_path, 'r', encoding=encoding or 'utf-8') as f:
            data = f.read()
        
        # Processa os dados usando o layout
        from app.utils.fixed_width import parse_fixed_width_data
        records = parse_fixed_width_data(data, layout_columns)
        
        if not records:
            logger.error("Nenhum registro encontrado no arquivo")
            return False
        
        logger.info(f"Validados {len(records)} registros do arquivo")
        return True
        
    except Exception as e:
        logger.error(f"Erro na validação dos dados: {str(e)}")
        return False

def parse_fixed_width_data(data: str, layout_file: str) -> List[Dict[str, Any]]:
    """
    Converte dados de largura fixa para lista de dicionários.
    
    Args:
        data: String contendo os dados
        layout_file: Caminho do arquivo de layout
        
    Returns:
        Lista de dicionários com os registros
    """
    try:
        # Carrega o layout
        layout_columns = parse_layout_file(layout_file)
        logger.info(f"Layout carregado com {len(layout_columns)} colunas")
        
        # Verifica se o layout está no formato correto
        if not isinstance(layout_columns, list):
            raise ValueError("Layout deve ser uma lista de dicionários")
            
        for col in layout_columns:
            if not isinstance(col, dict):
                raise ValueError("Cada coluna do layout deve ser um dicionário")
            required_keys = ['Coluna', 'Tamanho', 'Tipo']
            for key in required_keys:
                if key not in col:
                    raise ValueError(f"Coluna do layout deve ter a chave '{key}'")
        
        records = []
        lines = data.strip().split('\n')
        
        logger.info(f"Processando {len(lines)} linhas de dados")
        
        for line_num, line in enumerate(lines, 1):
            if not line.strip():
                continue
                
            record = {}
            current_pos = 0
            
            for col in layout_columns:
                field_name = col['Coluna']
                field_size = int(col['Tamanho'])
                
                if current_pos + field_size > len(line):
                    raise ValueError(f"Linha {line_num} muito curta para o campo {field_name}")
                
                value = line[current_pos:current_pos + field_size].strip()
                
                # Conversão de tipos consistente
                if col['Tipo'] == 'NUMBER':
                    # Trata valores vazios como None
                    if not value:
                        value = None
                    else:
                        try:
                            # Remove caracteres não numéricos
                            clean_value = re.sub(r'[^0-9.-]', '', value)
                            value = float(clean_value) if clean_value else None
                        except ValueError:
                            logger.warning(f"Linha {line_num}, Coluna {field_name}: Valor não numérico '{value}', convertendo para None")
                            value = None
                elif col['Tipo'] == 'CHAR':
                    # Remove espaços extras para campos CHAR
                    value = value.strip()
                
                record[field_name] = value
                current_pos += field_size
            
            if current_pos != len(line):
                raise ValueError(f"Linha {line_num} mais longa que o esperado: {len(line)} vs {current_pos}")
            
            records.append(record)
        
        logger.info(f"Total de registros processados: {len(records)}")
        return records
        
    except Exception as e:
        logger.error(f"Erro ao processar dados: {str(e)}")
        raise ValueError(f"Erro ao processar dados: {str(e)}")

# Funções de compatibilidade para usar a nova validação com mapeamento
def validate_database_schema_new(table_name: str, layout_file_path: str) -> bool:
    """
    Nova função de validação que usa mapeamento de colunas.
    Substitui a função antiga validate_database_schema()
    """
    try:
        validator = DataValidator()
        result = validator.validate_table_structure(table_name, layout_file_path)
        
        if result.get('valid', False):
            logger.info(f"✅ Validação passou para tabela {table_name}")
            logger.info(f"Mapeamento de colunas: {result.get('column_mapping', {})}")
            return True
        else:
            logger.error(f"❌ Validação falhou para tabela {table_name}: {result.get('error', 'Erro desconhecido')}")
            return False
            
    except Exception as e:
        logger.error(f"Erro na validação da tabela {table_name}: {str(e)}")
        return False

def get_column_mapping_for_table(table_name: str, layout_file_path: str) -> Dict[str, str]:
    """
    Retorna o mapeamento de colunas para uma tabela
    """
    try:
        validator = DataValidator()
        result = validator.validate_table_structure(table_name, layout_file_path)
        
        if result.get('valid', False):
            return result.get('column_mapping', {})
        else:
            return {}
            
    except Exception as e:
        logger.error(f"Erro ao obter mapeamento de colunas para {table_name}: {str(e)}")
        return {}