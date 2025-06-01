from flask import Blueprint, request, jsonify
import os
from werkzeug.utils import secure_filename
from app.services.async_processor import AsyncFileProcessor
from app.repositories.base import BaseRepository
from config import settings
import asyncio

upload_bp = Blueprint('upload', __name__)

# Configuração do processador
key_fields = ['id', 'data']  # Ajuste conforme seus campos chave
async_processor = None  # Será inicializado quando o repositório estiver pronto

@upload_bp.route('/upload', methods=['POST'])
async def upload_file():
    """
    Endpoint para upload e processamento de arquivos.
    
    O arquivo deve ser enviado como 'file' no form-data.
    """
    try:
        # Verifica se o arquivo foi enviado
        if 'file' not in request.files:
            return jsonify({
                'status': 'error',
                'message': 'Nenhum arquivo enviado'
            }), 400
            
        file = request.files['file']
        
        # Verifica se o arquivo tem nome
        if file.filename == '':
            return jsonify({
                'status': 'error',
                'message': 'Nenhum arquivo selecionado'
            }), 400
            
        # Verifica a extensão do arquivo
        if not file.filename.endswith('.txt'):
            return jsonify({
                'status': 'error',
                'message': 'Apenas arquivos .txt são permitidos'
            }), 400
            
        # Salva o arquivo
        filename = secure_filename(file.filename)
        filepath = os.path.join(settings.UPLOAD_FOLDER, filename)
        file.save(filepath)
        
        # Processa o arquivo
        results = []
        async for result in async_processor.process_file(filepath):
            results.append(result)
            
        # Remove o arquivo após o processamento
        os.remove(filepath)
        
        # Calcula totais
        total_inserted = sum(r['inserted'] for r in results if r['status'] == 'success')
        total_updated = sum(r['updated'] for r in results if r['status'] == 'success')
        total_unchanged = sum(r['unchanged'] for r in results if r['status'] == 'success')
        
        return jsonify({
            'status': 'success',
            'message': 'Arquivo processado com sucesso',
            'results': {
                'total_inserted': total_inserted,
                'total_updated': total_updated,
                'total_unchanged': total_unchanged,
                'batches': results
            }
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Erro ao processar arquivo: {str(e)}'
        }), 500

def init_processor(repository: BaseRepository):
    """
    Inicializa o processador com o repositório.
    
    Args:
        repository: Repositório para acesso ao banco de dados
    """
    global async_processor
    async_processor = AsyncFileProcessor(repository, key_fields) 