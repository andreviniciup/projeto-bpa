# Data Injector

## Descrição
O Data Injector é um serviço responsável por processar e injetar dados no banco de dados PostgreSQL. Ele recebe arquivos, processa-os e insere os dados nas tabelas apropriadas.

## Funcionalidades Principais

### 1. Processamento de Arquivos
- Recebe arquivos .zip contendo dados estruturados
- Extrai automaticamente os arquivos do zip
- Identifica e separa arquivos de dados e seus respectivos layouts

### 2. Validação de Dados
- Valida a estrutura dos arquivos de layout
- Verifica compatibilidade com a estrutura do banco de dados
- Valida tipos de dados e formatos
- Identifica e reporta inconsistências nos dados

### 3. Processamento de Layout
- Lê arquivos de layout (ex: rl_procedimento_origem_layout.txt)
- Identifica nomes e tipos das colunas
- Mapeia campos do arquivo para colunas do banco
- Valida compatibilidade com a estrutura da tabela

### 4. Injeção de Dados
- Compara dados existentes com novos dados
- Identifica registros novos e atualizados
- Realiza inserções e atualizações no banco
- Mantém histórico de alterações

### 5. Logs e Monitoramento
- Registra todas as operações realizadas
- Mantém log de erros e exceções
- Monitora performance do processamento
- Gera relatórios de processamento

## Estrutura do Projeto
```
data-injector/
├── app/                    # Código principal da aplicação
├── db/                     # Scripts e configurações do banco de dados
├── logs/                   # Arquivos de log
├── uploads/               # Diretório para arquivos temporários
├── config.py              # Configurações da aplicação
├── main.py               # Ponto de entrada da aplicação
├── requirements.txt      # Dependências do projeto
└── docker-compose.yml    # Configuração do Docker
```

## Requisitos
- Python 3.8+
- PostgreSQL 13+
- Docker e Docker Compose (opcional)

## Instalação

### Usando Docker
1. Clone o repositório
2. Execute:
```bash
docker-compose up -d
```

### Instalação Manual
1. Clone o repositório
2. Crie um ambiente virtual:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```
3. Instale as dependências:
```bash
pip install -r requirements.txt
```

## Configuração
1. Copie o arquivo `config.py` para `config_local.py`
2. Ajuste as configurações conforme necessário:
   - Conexão com o banco de dados
   - Configurações de logging
   - Outras configurações específicas

## Uso
1. Inicie o serviço:
```bash
python main.py
```

2. O serviço estará disponível em `http://localhost:5000`

## Endpoints
- `POST /upload`: Upload de arquivos para processamento
- `GET /status`: Verificar status do processamento
- `GET /health`: Verificar saúde do serviço

## Logs
Os logs são armazenados em:
- `logs/data_processor.log`: Logs de processamento
- `logs/app.log`: Logs da aplicação

## Desenvolvimento
Para desenvolvimento local:
1. Instale as dependências de desenvolvimento:
```bash
pip install -r requirements-dev.txt
```

2. Execute os testes:
```bash
pytest
```

## Contribuição
1. Faça um fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -am 'Adiciona nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Crie um Pull Request

## Licença
Este projeto está sob a licença MIT.
