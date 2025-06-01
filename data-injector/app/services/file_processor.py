import os
import zipfile 
import tempfile
import logging
import asyncio
from typing import Tuple, Optional, List, Dict, Any
from sqlalchemy import text
from app.models.database import SessionLocal
from app.utils.file_utils import create_temp_dir, remove_temp_dir, is_valid_zip, get_file_name
from app.services.data_validator import (
    DataValidator,
    validate_database_schema_new,
    get_column_mapping_for_table,
    parse_layout_file, 
    validate_database_schema, 
    validate_fixed_width_data,
    parse_fixed_width_data,
    check_table_exists
)
from app.services.database_service import insert_records_safely
from app.services.data_sync_service import sync_data_for_matched_tables
from werkzeug.utils import secure_filename
from app.utils.logger import app_logger
from config import settings
import pandas as pd

logger = logging.getLogger("FileProcessor")

def get_database_tables() -> List[str]:
    """
    Recupera a lista de tabelas do banco de dados.
    
    Returns:
        Lista de nomes de tabelas no esquema configurado.
    """
    try:
        with SessionLocal() as session:
            query = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = :schema
            """)
            
            result = session.execute(query, {'schema': settings.DATABASE_SCHEMA})
            tables = [row.table_name for row in result]
        
        logger.info(f"Tabelas encontradas no banco de dados: {tables}")
        return tables
    
    except Exception as e:
        logger.error(f"Erro ao recuperar tabelas do banco de dados: {str(e)}")
        return []

def match_files_to_tables(extracted_files: List[str], database_tables: List[str]) -> Dict[str, Optional[str]]:
    """
    Finds matches between files and database tables with improved precision.
    """
    data_files = [f for f in extracted_files if f.endswith('.txt') and '_layout' not in f]
    layout_files = [f for f in extracted_files if f.endswith('.txt') and '_layout' in f]
    
    table_file_matches = {}
    unmatched_files = []
    
    for table in database_tables:
        # More precise matching using exact table name and avoiding partial matches
        matching_data_file = next((f for f in data_files if f.lower() == f"{table.lower()}.txt"), None)
        matching_layout_file = next((f for f in layout_files if f.lower() == f"{table.lower()}_layout.txt"), None)
        
        if matching_data_file and matching_layout_file:
            table_file_matches[table] = {
                'data_file': matching_data_file,
                'layout_file': matching_layout_file
            }
        else:
            unmatched_files.append(table)
    
    # Additional logic to handle partial matches or provide better logging
    if not table_file_matches:
        logger.warning("No exact matches found. Attempting partial matching with more detailed logging.")
        # Add more sophisticated partial matching logic here
    
    return {
        'matched_tables': table_file_matches,
        'unmatched_files': unmatched_files
    }


def extract_zip_file(zip_path: str) -> Dict[str, Any]:
    """
    Extrai o conteúdo de um arquivo ZIP e identifica correspondências de tabelas.
    
    Returns:
        Dicionário com arquivos correspondidos e não correspondidos
    """
    temp_dir = None
    try:
        if not is_valid_zip(zip_path):
            logger.error(f"Arquivo ZIP inválido ou não encontrado: {zip_path}")
            return {'error': 'Invalid ZIP file'}
        
        # Cria diretório temporário para extração
        temp_dir = create_temp_dir()
        
        # Extrai o arquivo ZIP
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        # Lista arquivos extraídos
        extracted_files = os.listdir(temp_dir)
        
        # Recupera tabelas do banco de dados
        database_tables = get_database_tables()
        
        # Encontra correspondências
        matches = match_files_to_tables(extracted_files, database_tables)
        
        # Adiciona informações do diretório temporário
        matches['temp_dir'] = temp_dir
        
        logger.info(f"Correspondências encontradas: {matches}")
        return matches
    
    except Exception as e:
        logger.error(f"Erro ao extrair o arquivo ZIP: {str(e)}")
        if temp_dir:
            remove_temp_dir(temp_dir)
        return {'error': str(e)}

async def process_file_upload(file):
    """
    Processa o upload de um arquivo ZIP.
    
    Args:
        file: Arquivo ZIP enviado pelo usuário
        
    Returns:
        dict: Resultado do processamento
    """
    try:
        # Verifica se o arquivo é válido
        if not file or file.filename == '':
            return {
                'status': 'error',
                'message': 'Nenhum arquivo enviado'
            }
            
        # Verifica a extensão
        if not file.filename.lower().endswith('.zip'):
            return {
                'status': 'error',
                'message': 'Apenas arquivos ZIP são permitidos'
            }
            
        # Salva o arquivo
        filename = secure_filename(file.filename)
        filepath = os.path.join(settings.UPLOAD_FOLDER, filename)
        file.save(filepath)
        
        # Extrai e processa o arquivo ZIP
        extraction_result = extract_zip_file(filepath)
        
        if 'error' in extraction_result:
            return {
                'status': 'error',
                'message': f'Erro ao extrair arquivo ZIP: {extraction_result["error"]}'
            }
            
        # Processa os arquivos extraídos
        temp_dir = extraction_result['temp_dir']
        matched_tables = extraction_result['matched_tables']
        unmatched_files = extraction_result['unmatched_files']
        
        results = []
        for table, files in matched_tables.items():
            try:
                data_file = os.path.join(temp_dir, files['data_file'])
                layout_file = os.path.join(temp_dir, files['layout_file'])
                
                # Processa o arquivo
                result = await process_file(data_file, layout_file, table)
                results.append(result)
                
            except Exception as e:
                logger.error(f"Erro ao processar arquivo para tabela {table}: {str(e)}")
                results.append({
                    'table': table,
                    'status': 'error',
                    'message': str(e)
                })
        
        # Limpa arquivos temporários
        remove_temp_dir(temp_dir)
        os.remove(filepath)
        
        return {
            'status': 'success',
            'message': 'Arquivos processados com sucesso',
            'results': results,
            'unmatched_files': unmatched_files
        }
        
    except Exception as e:
        logger.error(f"Erro ao processar arquivo: {str(e)}")
        return {
            'status': 'error',
            'message': f'Erro ao processar arquivo: {str(e)}'
        }

async def process_file(data_file: str, layout_file: str, table_name: str):
    """
    Processa um arquivo de dados de acordo com seu layout.
    
    Args:
        data_file: Caminho do arquivo de dados
        layout_file: Caminho do arquivo de layout
        table_name: Nome da tabela no banco de dados
        
    Returns:
        dict: Resultado do processamento
    """
    try:
        # Lê o layout
        layout = parse_layout_file(layout_file)
        
        # Valida se a tabela existe
        if not check_table_exists(table_name):
            raise ValueError(f"Tabela {table_name} não encontrada no banco de dados")
            
        # Valida o schema da tabela usando a nova validação com mapeamento
        if not validate_database_schema_new(table_name, layout_file):
            raise ValueError(f"Schema da tabela {table_name} não corresponde ao layout")
        
        # Obtém o mapeamento de colunas para uso posterior se necessário
        column_mapping = get_column_mapping_for_table(table_name, layout_file)
        logger.info(f"Mapeamento de colunas obtido: {column_mapping}")
        
        # Lê e processa o arquivo de dados
        with open(data_file, 'r', encoding='utf-8') as f:
            data = f.read()
            
        if not validate_fixed_width_data(data, layout):
            raise ValueError(f"Dados não correspondem ao layout especificado")
            
        # Processa os dados
        records = parse_fixed_width_data(data, layout)
        
        # Insere os registros no banco
        result = await insert_records_safely(table_name, records)
        
        return {
            'table': table_name,
            'status': 'success',
            'message': f'Processado com sucesso: {len(records)} registros',
            'details': result
        }
        
    except Exception as e:
        logger.error(f"Erro ao processar arquivo para tabela {table_name}: {str(e)}")
        raise