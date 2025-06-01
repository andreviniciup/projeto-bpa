import pytest
import pandas as pd
from app.utils.bpa.consolidado_generator import BPAConsolidadoGenerator
from app.utils.bpa.individualizado_generator import BPAIndividualizadoGenerator
from app.utils.bpa.validators import BPAConsolidadoValidator, BPAIndividualizadoValidator

@pytest.fixture
def sample_consolidado_data():
    """Fixture com dados de exemplo para BPA Consolidado"""
    return pd.DataFrame({
        'cnes': ['1234567'],
        'competencia': ['202401'],
        'cbo': ['123456'],
        'sequencial': ['001'],
        'procedimento': ['1234567890'],
        'idade': [30],
        'quantidade': [1]
    })

@pytest.fixture
def sample_individualizado_data():
    """Fixture com dados de exemplo para BPA Individualizado"""
    return pd.DataFrame({
        'cnes': ['1234567'],
        'competencia': ['202401'],
        'cns_profissional': ['123456789012345'],
        'cbo': ['123456'],
        'data_atendimento': ['20240101'],
        'sequencial': ['001'],
        'procedimento': ['1234567890'],
        'cns_paciente': ['987654321098765'],
        'sexo': ['M'],
        'codigo_municipio': ['1234567'],
        'cid': ['A001'],
        'idade': [30],
        'quantidade': [1],
        'carater_atendimento': ['1'],
        'nome_paciente': ['PACIENTE TESTE'],
        'data_nascimento': ['19900101']
    })

@pytest.fixture
def consolidado_generator():
    """Fixture com instância do gerador de BPA Consolidado"""
    return BPAConsolidadoGenerator()

@pytest.fixture
def individualizado_generator():
    """Fixture com instância do gerador de BPA Individualizado"""
    return BPAIndividualizadoGenerator()

@pytest.fixture
def consolidado_validator():
    """Fixture com instância do validador de BPA Consolidado"""
    return BPAConsolidadoValidator()

@pytest.fixture
def individualizado_validator():
    """Fixture com instância do validador de BPA Individualizado"""
    return BPAIndividualizadoValidator() 