from flask_sqlalchemy import SQLAlchemy
import bcrypt

db = SQLAlchemy()

class User(db.Model):
    __tablename__ = 'usuario'
    cod_usuario = db.Column(db.Integer, primary_key=True)
    nom_usuario = db.Column(db.String, unique=True, nullable=False)
    senha = db.Column(db.String(60), nullable=False)  # bcrypt gera hash de 60 caracteres
    login = db.Column(db.String(20), unique=True, nullable=False)
    status = db.Column(db.String(1), nullable=False, default='A')
    cod_tipo_usuario = db.Column(db.Integer, nullable=False)

    def set_password(self, password: str):
        """Define a senha do usuário usando bcrypt"""
        salt = bcrypt.gensalt()
        self.senha = bcrypt.hashpw(password.encode(), salt).decode()

    def __repr__(self):
        return f'<User {self.login}>'
    
