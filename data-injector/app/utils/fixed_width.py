import re
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

def parse_fixed_width_data(data: str, layout_file: str) -> List[Dict[str, Any]]:
    """
    Processa dados em formato fixed-width usando um arquivo de layout.
    
    Args:
        data: String contendo os dados em formato fixed-width
        layout_file: Caminho do arquivo de layout
        
    Returns:
        Lista de dicionários com os dados processados
    """
    try:
        # Carrega o layout do arquivo
        with open(layout_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
        # Pula a primeira linha (header)
        layout_columns = []
        for line in lines[1:]:
            line = line.strip()
            if line:  # Ignora linhas vazias
                parts = line.split(',')
                if len(parts) >= 5:
                    column = {
                        'Coluna': parts[0].strip(),
                        'Tamanho': int(parts[1].strip()) if parts[1].strip().isdigit() else 255,
                        'Inicio': int(parts[2].strip()) if parts[2].strip().isdigit() else 1,
                        'Fim': int(parts[3].strip()) if parts[3].strip().isdigit() else 255,
                        'Tipo': parts[4].strip() if len(parts) > 4 else 'CHAR'
                    }
                    layout_columns.append(column)
        
        # Processa os dados
        records = []
        lines = data.strip().split('\n')
        
        for line in lines:
            if not line.strip():
                continue
                
            record = {}
            current_pos = 0
            
            for column in layout_columns:
                col_name = column['Coluna']
                col_size = column['Tamanho']
                
                # Extrai o valor da linha
                if current_pos + col_size <= len(line):
                    value = line[current_pos:current_pos + col_size].strip()
                else:
                    value = line[current_pos:].strip()
                
                # Converte o valor de acordo com o tipo
                if column['Tipo'] == 'NUMBER':
                    try:
                        value = float(value) if value else None
                    except ValueError:
                        value = None
                else:  # CHAR
                    value = value if value else None
                
                record[col_name] = value
                current_pos += col_size
            
            records.append(record)
        
        logger.info(f"Processados {len(records)} registros")
        return records
        
    except Exception as e:
        logger.error(f"Erro ao processar dados fixed-width: {str(e)}")
        raise 