import pytest
import pandas as pd

def test_consolidado_generator_process_data(consolidado_generator, sample_consolidado_data):
    """Testa o processamento de dados do BPA Consolidado"""
    result = consolidado_generator.process_data(sample_consolidado_data)
    assert result
    assert isinstance(result, str)
    assert len(result) > 0
    
    # Verifica se a linha gerada tem o formato correto
    lines = result.strip().split('\r\n')
    assert len(lines) == len(sample_consolidado_data)
    
    # Verifica o formato da primeira linha
    first_line = lines[0]
    assert first_line.startswith('02')  # Tipo de linha
    assert first_line[2:9] == '1234567'  # CNES
    assert first_line[9:15] == '202401'  # Competência
    assert first_line[15:21] == '123456'  # CBO
    assert first_line[21:24] == '001'  # Folha
    assert first_line[24:27] == '001'  # Sequencial
    assert first_line[27:37] == '1234567890'  # Procedimento
    assert first_line[37:40] == '030'  # Idade
    assert first_line[40:43] == '001'  # Quantidade
    assert first_line[43:46] == 'EXT'  # Origem

def test_individualizado_generator_process_data(individualizado_generator, sample_individualizado_data):
    """Testa o processamento de dados do BPA Individualizado"""
    result = individualizado_generator.process_data(sample_individualizado_data)
    assert result
    assert isinstance(result, str)
    assert len(result) > 0
    
    # Verifica se a linha gerada tem o formato correto
    lines = result.strip().split('\r\n')
    assert len(lines) == len(sample_individualizado_data)
    
    # Verifica o formato da primeira linha
    first_line = lines[0]
    assert first_line.startswith('03')  # Tipo de linha
    assert first_line[2:9] == '1234567'  # CNES
    assert first_line[9:15] == '202401'  # Competência
    assert first_line[15:30] == '123456789012345'  # CNS Profissional
    assert first_line[30:36] == '123456'  # CBO
    assert first_line[36:44] == '20240101'  # Data Atendimento
    assert first_line[44:47] == '001'  # Folha
    assert first_line[47:50] == '001'  # Sequencial
    assert first_line[50:60] == '1234567890'  # Procedimento
    assert first_line[60:75] == '987654321098765'  # CNS Paciente
    assert first_line[75:76] == 'M'  # Sexo
    assert first_line[76:83] == '1234567'  # Código Município
    assert first_line[83:87] == 'A001'  # CID
    assert first_line[87:90] == '030'  # Idade
    assert first_line[90:93] == '001'  # Quantidade
    assert first_line[93:94] == '1'  # Caráter Atendimento

def test_consolidado_generator_empty_data(consolidado_generator):
    """Testa o processamento de dados vazios do BPA Consolidado"""
    empty_data = pd.DataFrame()
    result = consolidado_generator.process_data(empty_data)
    assert result == ""

def test_individualizado_generator_empty_data(individualizado_generator):
    """Testa o processamento de dados vazios do BPA Individualizado"""
    empty_data = pd.DataFrame()
    result = individualizado_generator.process_data(empty_data)
    assert result == ""

def test_consolidado_generator_invalid_data(consolidado_generator):
    """Testa o processamento de dados inválidos do BPA Consolidado"""
    invalid_data = pd.DataFrame({
        'cnes': ['123456'],  # CNES inválido
        'competencia': ['202401'],
        'cbo': ['123456'],
        'sequencial': ['001'],
        'procedimento': ['1234567890'],
        'idade': [30],
        'quantidade': [1]
    })
    result = consolidado_generator.process_data(invalid_data)
    assert result == ""

def test_individualizado_generator_invalid_data(individualizado_generator):
    """Testa o processamento de dados inválidos do BPA Individualizado"""
    invalid_data = pd.DataFrame({
        'cnes': ['1234567'],
        'competencia': ['202401'],
        'cns_profissional': ['12345678901234'],  # CNS inválido
        'cbo': ['123456'],
        'data_atendimento': ['20240101'],
        'sequencial': ['001'],
        'procedimento': ['1234567890'],
        'cns_paciente': ['987654321098765'],
        'sexo': ['X'],  # Sexo inválido
        'codigo_municipio': ['1234567'],
        'cid': ['A001'],
        'idade': [30],
        'quantidade': [1],
        'carater_atendimento': ['1'],
        'nome_paciente': ['PACIENTE TESTE'],
        'data_nascimento': ['19900101']
    })
    result = individualizado_generator.process_data(invalid_data)
    assert result == "" 