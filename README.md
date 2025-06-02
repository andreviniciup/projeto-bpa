# Projeto BPA

## Descrição
Este projeto consiste em dois serviços principais que trabalham em conjunto para processar e gerar relatórios BPA (Boletim de Produção Ambulatorial):

1. **Data Injector**: Responsável por processar e injetar dados no banco de dados PostgreSQL
2. **Gerador BPA**: Responsável por gerar relatórios e documentos BPA a partir dos dados processados

## Estrutura do Projeto
```
projeto-bpa/
├── data-injector/         # Serviço de injeção de dados
└── geradorbpa/           # Serviço de geração de relatórios
```

## Requisitos do Sistema
- Python 3.8+
- PostgreSQL 13+
- Docker e Docker Compose (opcional)

## Instalação

### Usando Docker
1. Clone o repositório
2. Execute em cada diretório:
```bash
cd data-injector
docker-compose up -d

cd ../geradorbpa
docker-compose up -d
```

### Instalação Manual
1. Clone o repositório
2. Configure cada serviço seguindo suas respectivas documentações:
   - [Data Injector](data-injector/README.md)
   - [Gerador BPA](geradorbpa/README.md)

## Fluxo de Dados
1. O Data Injector recebe arquivos de dados
2. Processa e valida os dados
3. Insere os dados no banco de dados PostgreSQL
4. O Gerador BPA acessa os dados processados
5. Gera relatórios e documentos BPA

## Portas dos Serviços
- Data Injector: `http://localhost:8000`
- Gerador BPA: `http://localhost:5000`

## Documentação Detalhada
- [Documentação do Data Injector](data-injector/README.md)
- [Documentação do Gerador BPA](geradorbpa/README.md)

## Desenvolvimento
Cada serviço possui seu próprio ambiente de desenvolvimento. Consulte a documentação específica de cada serviço para mais detalhes.

## Contribuição
1. Faça um fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -am 'Adiciona nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Crie um Pull Request

## Licença
Este projeto está sob a licença MIT. 