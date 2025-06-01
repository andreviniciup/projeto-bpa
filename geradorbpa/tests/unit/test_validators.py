import pytest
import pandas as pd

def test_consolidado_validator_required_fields(consolidado_validator):
    """Testa validação de campos obrigatórios do BPA Consolidado"""
    # Dados válidos
    valid_data = pd.Series({
        'cnes': '1234567',
        'competencia': '202401',
        'cbo': '123456',
        'sequencial': '001',
        'procedimento': '1234567890',
        'idade': 30,
        'quantidade': 1
    })
    assert consolidado_validator.validate(valid_data)
    
    # Dados inválidos - campo obrigatório ausente
    invalid_data = pd.Series({
        'cnes': '1234567',
        'competencia': '202401',
        'cbo': '123456',
        'sequencial': '001',
        'procedimento': '1234567890',
        'idade': 30
        # quantidade ausente
    })
    assert not consolidado_validator.validate(invalid_data)
    assert len(consolidado_validator.get_errors()) > 0

def test_consolidado_validator_field_formats(consolidado_validator):
    """Testa validação de formatos de campos do BPA Consolidado"""
    # CNES inválido
    invalid_cnes = pd.Series({
        'cnes': '123456',  # 6 dígitos ao invés de 7
        'competencia': '202401',
        'cbo': '123456',
        'sequencial': '001',
        'procedimento': '1234567890',
        'idade': 30,
        'quantidade': 1
    })
    assert not consolidado_validator.validate(invalid_cnes)
    assert any('CNES deve ter 7 dígitos' in error for error in consolidado_validator.get_errors())
    
    # Competência inválida
    invalid_competencia = pd.Series({
        'cnes': '1234567',
        'competencia': '2024',  # 4 dígitos ao invés de 6
        'cbo': '123456',
        'sequencial': '001',
        'procedimento': '1234567890',
        'idade': 30,
        'quantidade': 1
    })
    assert not consolidado_validator.validate(invalid_competencia)
    assert any('Competência deve ter 6 dígitos' in error for error in consolidado_validator.get_errors())

def test_consolidado_validator_field_values(consolidado_validator):
    """Testa validação de valores de campos do BPA Consolidado"""
    # Idade inválida
    invalid_idade = pd.Series({
        'cnes': '1234567',
        'competencia': '202401',
        'cbo': '123456',
        'sequencial': '001',
        'procedimento': '1234567890',
        'idade': 200,  # Idade inválida
        'quantidade': 1
    })
    assert not consolidado_validator.validate(invalid_idade)
    assert any('Idade inválida' in error for error in consolidado_validator.get_errors())
    
    # Quantidade inválida
    invalid_quantidade = pd.Series({
        'cnes': '1234567',
        'competencia': '202401',
        'cbo': '123456',
        'sequencial': '001',
        'procedimento': '1234567890',
        'idade': 30,
        'quantidade': 0  # Quantidade inválida
    })
    assert not consolidado_validator.validate(invalid_quantidade)
    assert any('Quantidade deve ser maior que zero' in error for error in consolidado_validator.get_errors())

def test_individualizado_validator_required_fields(individualizado_validator):
    """Testa validação de campos obrigatórios do BPA Individualizado"""
    # Dados válidos
    valid_data = pd.Series({
        'cnes': '1234567',
        'competencia': '202401',
        'cns_profissional': '123456789012345',
        'cbo': '123456',
        'data_atendimento': '20240101',
        'sequencial': '001',
        'procedimento': '1234567890',
        'cns_paciente': '987654321098765',
        'sexo': 'M',
        'codigo_municipio': '1234567',
        'cid': 'A001',
        'idade': 30,
        'quantidade': 1,
        'carater_atendimento': '1',
        'nome_paciente': 'PACIENTE TESTE',
        'data_nascimento': '19900101'
    })
    assert individualizado_validator.validate(valid_data)
    
    # Dados inválidos - campo obrigatório ausente
    invalid_data = pd.Series({
        'cnes': '1234567',
        'competencia': '202401',
        'cns_profissional': '123456789012345',
        'cbo': '123456',
        'data_atendimento': '20240101',
        'sequencial': '001',
        'procedimento': '1234567890',
        'cns_paciente': '987654321098765',
        'sexo': 'M',
        'codigo_municipio': '1234567',
        'cid': 'A001',
        'idade': 30,
        'quantidade': 1,
        'carater_atendimento': '1'
        # nome_paciente e data_nascimento ausentes
    })
    assert not individualizado_validator.validate(invalid_data)
    assert len(individualizado_validator.get_errors()) > 0

def test_individualizado_validator_field_formats(individualizado_validator):
    """Testa validação de formatos de campos do BPA Individualizado"""
    # CNS profissional inválido
    invalid_cns = pd.Series({
        'cnes': '1234567',
        'competencia': '202401',
        'cns_profissional': '12345678901234',  # 14 dígitos ao invés de 15
        'cbo': '123456',
        'data_atendimento': '20240101',
        'sequencial': '001',
        'procedimento': '1234567890',
        'cns_paciente': '987654321098765',
        'sexo': 'M',
        'codigo_municipio': '1234567',
        'cid': 'A001',
        'idade': 30,
        'quantidade': 1,
        'carater_atendimento': '1',
        'nome_paciente': 'PACIENTE TESTE',
        'data_nascimento': '19900101'
    })
    assert not individualizado_validator.validate(invalid_cns)
    assert any('CNS do profissional deve ter 15 dígitos' in error for error in individualizado_validator.get_errors())

def test_individualizado_validator_field_values(individualizado_validator):
    """Testa validação de valores de campos do BPA Individualizado"""
    # Sexo inválido
    invalid_sexo = pd.Series({
        'cnes': '1234567',
        'competencia': '202401',
        'cns_profissional': '123456789012345',
        'cbo': '123456',
        'data_atendimento': '20240101',
        'sequencial': '001',
        'procedimento': '1234567890',
        'cns_paciente': '987654321098765',
        'sexo': 'X',  # Sexo inválido
        'codigo_municipio': '1234567',
        'cid': 'A001',
        'idade': 30,
        'quantidade': 1,
        'carater_atendimento': '1',
        'nome_paciente': 'PACIENTE TESTE',
        'data_nascimento': '19900101'
    })
    assert not individualizado_validator.validate(invalid_sexo)
    assert any('Sexo deve ser' in error for error in individualizado_validator.get_errors())
    
    # Caráter de atendimento inválido
    invalid_carater = pd.Series({
        'cnes': '1234567',
        'competencia': '202401',
        'cns_profissional': '123456789012345',
        'cbo': '123456',
        'data_atendimento': '20240101',
        'sequencial': '001',
        'procedimento': '1234567890',
        'cns_paciente': '987654321098765',
        'sexo': 'M',
        'codigo_municipio': '1234567',
        'cid': 'A001',
        'idade': 30,
        'quantidade': 1,
        'carater_atendimento': '5',  # Caráter inválido
        'nome_paciente': 'PACIENTE TESTE',
        'data_nascimento': '19900101'
    })
    assert not individualizado_validator.validate(invalid_carater)
    assert any('Caráter de atendimento inválido' in error for error in individualizado_validator.get_errors()) 