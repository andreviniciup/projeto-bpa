import hashlib
from flask import request, render_template, redirect, url_for, session
from app import app
from app.utils.bpa_controller import BPAController
from app.utils.bpa_view import BPAView, send_file
from app.services.bpa_service import BPAService
from app.services.auth_service import AuthService, login_required

# Instancia serviços
bpa_controller = BPAController()
bpa_view = BPAView()
auth_service = AuthService()

# Credenciais fixas
USERNAME_PADRAO = "ADMIN"
SENHA_PADRAO_MD5 = hashlib.md5("ADMIN".encode()).hexdigest()

# Rota que redireciona para a página de login
@app.route("/", methods=["GET", "POST"])
def index():
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        login = request.form['username']
        senha = request.form['password']
        
        user = auth_service.authenticate(login, senha)
        if user:
            session.update(user)
            return redirect(url_for('menu'))
        else:
            return render_template('login.html', error='Credenciais inválidas')
    return render_template('login.html')

@app.route('/logout', methods=['GET', 'POST'])
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/menu')
@login_required
def menu():
    return render_template('menu.html')

@app.route('/dashboard')
@login_required
def dashboard():
    data = get_dashboard_data()
    return render_template('dashboard.html', data=data)

@app.route("/formulario_bpa", methods=["GET", "POST"])
@login_required
def formulario_bpa():
    if request.method == "POST":
        return bpa_controller.process_form(request.form)
    return bpa_view.render_form()


