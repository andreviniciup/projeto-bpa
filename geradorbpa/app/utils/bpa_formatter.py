import pandas as pd
from datetime import datetime
from typing import Dict, Any

class BPAFormatter:
    """Classe responsável por formatar os dados para o BPA"""
    
    @classmethod
    def format_cnes(cls, value: str) -> str:
        return str(value).zfill(7)

    @classmethod
    def format_competencia(cls, value: str) -> str:
        return str(value).zfill(6)

    @classmethod
    def format_cns(cls, value: str) -> str:
        return str(value).zfill(15)

    @classmethod
    def format_cns_profissional(cls, value: str) -> str:
        return cls.format_cns(value)

    @classmethod
    def format_cns_paciente(cls, value: str) -> str:
        return cls.format_cns(value)

    @classmethod
    def format_cbo(cls, value: str) -> str:
        return str(value).ljust(6)

    @classmethod
    def format_data_atendimento(cls, value: str) -> str:
        try:
            return pd.to_datetime(value).strftime('%Y%m%d')
        except:
            # Fallback para formato atual se a conversão falhar
            return str(value)

    @classmethod
    def format_folha(cls, value: str) -> str:
        return str(value).zfill(3)

    @classmethod
    def format_sequencial(cls, value: str) -> str:
        return str(value).zfill(2)

    @classmethod
    def format_procedimento(cls, value: str) -> str:
        return str(value).zfill(10)

    @classmethod
    def format_codigo_municipio(cls, value: str) -> str:
        return str(value).zfill(6)

    @classmethod
    def format_cid(cls, value: str) -> str:
        return str(value).ljust(4)

    @classmethod
    def format_idade(cls, value: str) -> str:
        return str(value).zfill(3)

    @classmethod
    def format_quantidade(cls, value: str) -> str:
        return str(value).zfill(6)

    @classmethod
    def format_carater_atendimento(cls, value: str) -> str:
        return str(value).zfill(2)

    @classmethod
    def format_numero_autorizacao(cls, value: str) -> str:
        return str(value).ljust(13)

    @classmethod
    def format_nome_paciente(cls, value: str) -> str:
        return str(value).ljust(30)

    @classmethod
    def format_data_nascimento(cls, value: str) -> str:
        try:
            return pd.to_datetime(value).strftime('%Y%m%d')
        except:
            # Fallback para formato atual se a conversão falhar
            return str(value)

    @classmethod
    def format_raca(cls, value: str) -> str:
        return str(value).zfill(2)

    @classmethod
    def format_etnia(cls, value: str) -> str:
        return str(value).ljust(4)

    @classmethod
    def format_nacionalidade(cls, value: str) -> str:
        return str(value).zfill(3)

    @classmethod
    def format_servico(cls, value: str) -> str:
        return str(value).ljust(3)

    @classmethod
    def format_classificacao(cls, value: str) -> str:
        return str(value).ljust(3)

    @classmethod
    def format_equipe_seq(cls, value: str) -> str:
        return str(value).ljust(8)

    @classmethod
    def format_equipe_area(cls, value: str) -> str:
        return str(value).ljust(4)

    @classmethod
    def format_cnpj(cls, value: str) -> str:
        return str(value).ljust(14)

    @classmethod
    def format_cep(cls, value: str) -> str:
        return str(value).ljust(8)

    @classmethod
    def format_codigo_logradouro(cls, value: str) -> str:
        return str(value).ljust(3)

    @classmethod
    def format_endereco(cls, value: str) -> str:
        return str(value).ljust(30)

    @classmethod
    def format_complemento(cls, value: str) -> str:
        return str(value).ljust(10)

    @classmethod
    def format_numero(cls, value: str) -> str:
        return str(value).ljust(5)

    @classmethod
    def format_bairro(cls, value: str) -> str:
        return str(value).ljust(30)

    @classmethod
    def format_telefone(cls, value: str) -> str:
        return str(value).ljust(11)

    @classmethod
    def format_email(cls, value: str) -> str:
        return str(value).ljust(40)

    @classmethod
    def format_ine(cls, value: str) -> str:
        return str(value).ljust(10)

    @classmethod
    def format_data(cls, row: Dict[str, Any]) -> Dict[str, Any]:
        """Formata todos os campos do registro conforme os métodos disponíveis"""
        formatted_row = {}
        for key, value in row.items():
            formatter_method = f'format_{key}'
            if hasattr(cls, formatter_method):
                formatted_row[key] = getattr(cls, formatter_method)(value)
            else:
                formatted_row[key] = value
        return formatted_row
