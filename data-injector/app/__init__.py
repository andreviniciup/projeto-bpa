from flask import Flask
from config import settings
import os

def create_app():
    """
    Factory function para criar e configurar a aplicação Flask.
    """
    app = Flask(__name__, 
                template_folder='templates',
                static_folder='static')
    
    # Configurações
    app.config['MAX_CONTENT_LENGTH'] = settings.MAX_CONTENT_LENGTH
    app.config['UPLOAD_FOLDER'] = settings.UPLOAD_FOLDER
    
    # Garante que as pastas necessárias existem
    os.makedirs(settings.UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(os.path.join(app.root_path, 'templates'), exist_ok=True)
    os.makedirs(os.path.join(app.root_path, 'static'), exist_ok=True)

    # Registrar blueprints (rotas)
    from app.routes.api import api_bp
    app.register_blueprint(api_bp, url_prefix='/api')

    return app