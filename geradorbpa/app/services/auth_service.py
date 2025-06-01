from functools import wraps
from flask import session, redirect, url_for
import hashlib
from app.config import settings

class AuthService:
    def __init__(self):
        self._users = {
            'ADMIN': {
                'password': hashlib.md5('ADMIN'.encode()).hexdigest(),
                'name': 'Administrador',
                'cod_usuario': 1,
                'cod_tipo_usuario': 1
            }
        }
    
    def authenticate(self, username: str, password: str) -> dict:
        """Autentica um usuário"""
        username = username.strip().upper()
        password_md5 = hashlib.md5(password.encode()).hexdigest()
        
        if username in self._users and self._users[username]['password'] == password_md5:
            return {
                'username': username,
                'nom_usuario': self._users[username]['name'],
                'cod_usuario': self._users[username]['cod_usuario'],
                'cod_tipo_usuario': self._users[username]['cod_tipo_usuario']
            }
        return None

def login_required(f):
    """Decorator para proteger rotas que requerem autenticação"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function 