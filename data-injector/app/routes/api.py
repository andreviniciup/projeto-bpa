from flask import Blueprint, request, jsonify, render_template
from app.services.file_processor import process_file_upload
from app.services.error_handler import ErrorHandler
import tempfile
import os
import asyncio
import logging

# Configuração do logger
logger = logging.getLogger(__name__)

# Cria um Blueprint para as rotas da API
api_bp = Blueprint('api', __name__)

error_handler = ErrorHandler()

@api_bp.route('/')
def index():
    """
    View para renderizar a página inicial (index.html).
    """
    logger.info("Acessando a rota raiz (/)")
    try:
        return render_template('index.html')
    except Exception as e:
        logger.error(f"Erro ao renderizar template: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'Erro ao carregar a página inicial'
        }), 500

@api_bp.route('/upload', methods=['POST'])
def upload_file():
    """
    Endpoint para upload de arquivos.
    """
    logger.info("Recebendo requisição de upload")
    if 'file' not in request.files:
        logger.warning("Nenhum arquivo enviado na requisição")
        return jsonify({
            'status': 'error',
            'message': 'Nenhum arquivo enviado'
        }), 400
        
    file = request.files['file']
    logger.info(f"Arquivo recebido: {file.filename}")
    
    try:
        # Executa o processamento de forma síncrona
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(process_file_upload(file))
        loop.close()
        
        logger.info(f"Resultado do processamento: {result}")
        
        if result['status'] == 'error':
            return jsonify(result), 400
            
        return jsonify(result)
    except Exception as e:
        logger.error(f"Erro ao processar arquivo: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Erro ao processar arquivo: {str(e)}'
        }), 500