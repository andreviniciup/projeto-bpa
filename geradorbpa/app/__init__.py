"""
Pacote principal do Gerador de BPA
"""

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from config import settings

# Inicializa o Flask
app = Flask(__name__)

# Configura o banco de dados
app.config['SQLALCHEMY_DATABASE_URI'] = settings.DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = settings.SECRET_KEY

# Inicializa o SQLAlchemy
db = SQLAlchemy(app)

# Importa as rotas após inicializar o app
from app import routes