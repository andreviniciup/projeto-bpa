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


def validate_database_schema(table_name: str, layout_columns: List[Dict[str, Any]]) -> bool:
    """
    Valida se o layout corresponde à estrutura da tabela no banco de dados.
    Ignora colunas extras no banco (como chaves primárias autoincrementadas).
    
    Args:
        table_name: Nome da tabela
        layout_columns: Colunas do layout
        
    Returns:
        Booleano indicando se o layout é válido
    """
    try:
        # Mapeamento de tipos de dados de Oracle para PostgreSQL
        def map_oracle_to_postgres(oracle_type: str) -> str:
            type_mapping = {
                'VARCHAR2': 'character varying',
                'NUMBER': 'numeric',
                'CHAR': 'character',
                'DATE': 'date'
            }
            return type_mapping.get(oracle_type, oracle_type.lower())
        
        # Obtém colunas do banco de dados
        with SessionLocal() as session:
            # Consulta SQL simplificada para depuração
            query = text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = :table 
                AND table_schema = :schema
            """)
            
            # Log para debug: imprime a consulta SQL
            logger.info(f"Executando consulta SQL simplificada: {query}")
            
            result = session.execute(query, {'table': table_name, 'schema': settings.DATABASE_SCHEMA})
            db_columns = [(row.column_name.lower(), row.data_type) for row in result]  # Converte para minúsculo
        
        # Log para debug: imprime o nome da tabela e o schema
        logger.info(f"Validando tabela: {table_name}, schema: {settings.DATABASE_SCHEMA}")
        logger.info(f"Colunas do banco de dados (consulta simplificada): {[col[0] for col in db_columns]}")
        logger.info(f"Colunas do layout: {[col['Coluna'] for col in layout_columns]}")
        
        # Verifica se cada coluna do layout existe no banco (ignorando case)
        for layout_col in layout_columns:
            layout_col_name = layout_col['Coluna'].lower()  # Converte para minúsculo
            found = False
            for db_col, db_type in db_columns:
                if db_col == layout_col_name:  # Compara em minúsculo
                    found = True
                    # Mapeia e compara tipos de dados
                    mapped_layout_type = map_oracle_to_postgres(layout_col['Tipo'])
                    if not db_type.startswith(mapped_layout_type):
                        logger.error(f"Tipo de dados incompatível para coluna {layout_col_name}: Layout ({layout_col['Tipo']}) vs DB ({db_type})")
                        return False
                    break
            if not found:
                logger.error(f"Coluna {layout_col['Coluna']} (layout) não encontrada no banco de dados ({[col[0] for col in db_columns]})")
                return False
        
        return True
    
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
    Valida o arquivo de dados de largura fixa.
    
    Args:
        data_file_path: Caminho do arquivo de dados
        layout_columns: Informações do layout
        encoding: Encoding do arquivo (opcional)
        
    Returns:
        Booleano indicando se os dados estão no formato correto
    """
    # Lista de possíveis encodings para tentar
    possible_encodings = [
        encoding,  # Primeiro, tenta o encoding fornecido
        'utf-8', 
        'iso-8859-1', 
        'windows-1252', 
        'latin1'
    ]
    
    # Remove None do início da lista
    possible_encodings = [enc for enc in possible_encodings if enc is not None]
    
    for current_encoding in possible_encodings:
        try:
            with open(data_file_path, 'r', encoding=current_encoding) as file:
                for line_num, line in enumerate(file, 1):
                    # Remove newline e verifica comprimento total
                    line = line.rstrip('\n')
                    total_expected_length = int(layout_columns[-1]['Fim'])
                    
                    if len(line) != total_expected_length:
                        logger.error(f"Linha {line_num}: Comprimento incorreto. Esperado {total_expected_length}, encontrado {len(line)}")
                        return False
                    
                    # Valida cada coluna
                    for col in layout_columns:
                        start = int(col['Inicio']) - 1
                        end = int(col['Fim'])
                        value = line[start:end]
                        
                        # Validações específicas por tipo
                        if col['Tipo'] == 'NUMBER':
                            try:
                                float(value.strip())
                            except ValueError:
                                logger.error(f"Linha {line_num}, Coluna {col['Coluna']}: Valor não numérico")
                                return False
            
            # Se chegou até aqui, a validação com este encoding foi bem-sucedida
            if current_encoding != encoding:
                logger.info(f"Usando encoding: {current_encoding}")
            return True
        
        except UnicodeDecodeError:
            # Continua para o próximo encoding se houver erro de decodificação
            continue
        except Exception as e:
            logger.error(f"Erro na validação dos dados: {str(e)}")
            return False
    
    # Se nenhum encoding funcionou
    logger.error("Não foi possível decodificar o arquivo com os encodings testados")
    return False

def parse_fixed_width_data(data_file_path: str, layout_columns: List[Dict[str, Any]], encoding: str = None) -> List[Dict[str, Any]]:
    """
    Converte arquivo de largura fixa para lista de dicionários.
    
    Args:
        data_file_path: Caminho do arquivo de dados
        layout_columns: Informações do layout
        encoding: Encoding do arquivo (opcional)
        
    Returns:
        Lista de dicionários com os registros
    """
    # Lista de possíveis encodings para tentar
    possible_encodings = [
        encoding,  # Primeiro, tenta o encoding fornecido
        'utf-8', 
        'iso-8859-1', 
        'windows-1252', 
        'latin1'
    ]
    
    # Remove None do início da lista
    possible_encodings = [enc for enc in possible_encodings if enc is not None]
    
    successful_encoding = None
    
    for current_encoding in possible_encodings:
        try:
            records = []
            
            with open(data_file_path, 'r', encoding=current_encoding) as file:
                for line_num, line in enumerate(file, 1):
                    # Remove quebras de linha e espaços extras
                    line = line.rstrip('\n')
                    
                    # Verifica se a linha tem o comprimento esperado
                    expected_length = int(layout_columns[-1]['Fim'])
                    if len(line) != expected_length:
                        logger.warning(f"Linha {line_num}: Comprimento incorreto. Esperado {expected_length}, encontrado {len(line)}")
                        # Ajusta a linha se for muito curta (preenche com espaços)
                        if len(line) < expected_length:
                            line = line.ljust(expected_length)
                        # Trunca se for muito longa
                        if len(line) > expected_length:
                            line = line[:expected_length]
                    
                    record = {}
                    for col in layout_columns:
                        start = int(col['Inicio']) - 1
                        end = int(col['Fim'])
                        
                        # Garante que os índices estão dentro dos limites
                        if start >= len(line):
                            value = ''
                        elif end > len(line):
                            value = line[start:].strip()
                        else:
                            value = line[start:end].strip()
                        
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
                                    logger.warning(f"Linha {line_num}, Coluna {col['Coluna']}: Valor não numérico '{value}', convertendo para None")
                                    value = None
                        elif col['Tipo'] == 'CHAR':
                            # Remove espaços extras para campos CHAR
                            value = value.strip()
                        
                        record[col['Coluna']] = value
                    
                    records.append(record)
            
            # Se chegou até aqui, a leitura com este encoding foi bem-sucedida
            successful_encoding = current_encoding
            logger.info(f"Arquivo lido com sucesso usando encoding: {current_encoding}")
            logger.info(f"Total de registros lidos: {len(records)}")
            return records
        
        except UnicodeDecodeError:
            # Continua para o próximo encoding se houver erro de decodificação
            continue
        except Exception as e:
            logger.error(f"Erro na interpretação dos dados com encoding {current_encoding}: {str(e)}")
            continue
    
    # Se nenhum encoding funcionou
    logger.error("Não foi possível decodificar o arquivo com os encodings testados")
    return []

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