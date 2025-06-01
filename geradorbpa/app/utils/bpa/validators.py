from abc import ABC, abstractmethod
import pandas as pd
from app.utils.logger import logger

class BaseBPAValidator(ABC):
    """Classe base para validação de BPA"""
    
    def __init__(self):
        self.errors = []
    
    def validate(self, row: pd.Series) -> bool:
        """
        Valida uma linha de BPA
        
        Args:
            row: Série do pandas com os dados a serem validados
            
        Returns:
            True se os dados são válidos, False caso contrário
        """
        self.errors = []
        self._validate_required_fields(row)
        self._validate_field_formats(row)
        self._validate_field_values(row)
        return len(self.errors) == 0
    
    def get_errors(self) -> list:
        """Retorna a lista de erros encontrados"""
        return self.errors
    
    def _add_error(self, field: str, message: str):
        """Adiciona um erro à lista"""
        self.errors.append(f"{field}: {message}")
        logger.log_warning(f"Erro de validação: {field} - {message}")
    
    @abstractmethod
    def _validate_required_fields(self, row: pd.Series):
        """Valida campos obrigatórios"""
        pass
    
    @abstractmethod
    def _validate_field_formats(self, row: pd.Series):
        """Valida formatos dos campos"""
        pass
    
    @abstractmethod
    def _validate_field_values(self, row: pd.Series):
        """Valida valores dos campos"""
        pass

class BPAConsolidadoValidator(BaseBPAValidator):
    """Validador para BPA Consolidado"""
    
    def _validate_required_fields(self, row: pd.Series):
        required_fields = ['cnes', 'competencia', 'cbo', 'sequencial', 
                         'procedimento', 'idade', 'quantidade']
        
        for field in required_fields:
            if field not in row or pd.isna(row[field]):
                self._add_error(field, "Campo obrigatório ausente")
    
    def _validate_field_formats(self, row: pd.Series):
        # Valida formato do CNES
        if 'cnes' in row and not pd.isna(row['cnes']):
            if not str(row['cnes']).isdigit() or len(str(row['cnes'])) != 7:
                self._add_error('cnes', "CNES deve ter 7 dígitos")
        
        # Valida formato da competência
        if 'competencia' in row and not pd.isna(row['competencia']):
            if not str(row['competencia']).isdigit() or len(str(row['competencia'])) != 6:
                self._add_error('competencia', "Competência deve ter 6 dígitos")
        
        # Valida formato do CBO
        if 'cbo' in row and not pd.isna(row['cbo']):
            if not str(row['cbo']).isdigit() or len(str(row['cbo'])) != 6:
                self._add_error('cbo', "CBO deve ter 6 dígitos")
    
    def _validate_field_values(self, row: pd.Series):
        # Valida idade
        if 'idade' in row and not pd.isna(row['idade']):
            try:
                idade = int(row['idade'])
                if idade < 0 or idade > 150:
                    self._add_error('idade', "Idade inválida")
            except ValueError:
                self._add_error('idade', "Idade deve ser um número")
        
        # Valida quantidade
        if 'quantidade' in row and not pd.isna(row['quantidade']):
            try:
                quantidade = int(row['quantidade'])
                if quantidade <= 0:
                    self._add_error('quantidade', "Quantidade deve ser maior que zero")
            except ValueError:
                self._add_error('quantidade', "Quantidade deve ser um número")

class BPAIndividualizadoValidator(BaseBPAValidator):
    """Validador para BPA Individualizado"""
    
    def _validate_required_fields(self, row: pd.Series):
        required_fields = [
            'cnes', 'competencia', 'cns_profissional', 'cbo', 'data_atendimento',
            'sequencial', 'procedimento', 'cns_paciente', 'sexo', 'codigo_municipio',
            'cid', 'idade', 'quantidade', 'carater_atendimento', 'nome_paciente',
            'data_nascimento'
        ]
        
        for field in required_fields:
            if field not in row or pd.isna(row[field]):
                self._add_error(field, "Campo obrigatório ausente")
    
    def _validate_field_formats(self, row: pd.Series):
        # Validações comuns com BPA Consolidado
        if 'cnes' in row and not pd.isna(row['cnes']):
            if not str(row['cnes']).isdigit() or len(str(row['cnes'])) != 7:
                self._add_error('cnes', "CNES deve ter 7 dígitos")
        
        if 'competencia' in row and not pd.isna(row['competencia']):
            if not str(row['competencia']).isdigit() or len(str(row['competencia'])) != 6:
                self._add_error('competencia', "Competência deve ter 6 dígitos")
        
        if 'cbo' in row and not pd.isna(row['cbo']):
            if not str(row['cbo']).isdigit() or len(str(row['cbo'])) != 6:
                self._add_error('cbo', "CBO deve ter 6 dígitos")
        
        # Validações específicas do BPA Individualizado
        if 'cns_profissional' in row and not pd.isna(row['cns_profissional']):
            if not str(row['cns_profissional']).isdigit() or len(str(row['cns_profissional'])) != 15:
                self._add_error('cns_profissional', "CNS do profissional deve ter 15 dígitos")
        
        if 'cns_paciente' in row and not pd.isna(row['cns_paciente']):
            if not str(row['cns_paciente']).isdigit() or len(str(row['cns_paciente'])) != 15:
                self._add_error('cns_paciente', "CNS do paciente deve ter 15 dígitos")
        
        if 'data_atendimento' in row and not pd.isna(row['data_atendimento']):
            if not str(row['data_atendimento']).isdigit() or len(str(row['data_atendimento'])) != 8:
                self._add_error('data_atendimento', "Data de atendimento inválida")
    
    def _validate_field_values(self, row: pd.Series):
        # Validações comuns com BPA Consolidado
        if 'idade' in row and not pd.isna(row['idade']):
            try:
                idade = int(row['idade'])
                if idade < 0 or idade > 150:
                    self._add_error('idade', "Idade inválida")
            except ValueError:
                self._add_error('idade', "Idade deve ser um número")
        
        if 'quantidade' in row and not pd.isna(row['quantidade']):
            try:
                quantidade = int(row['quantidade'])
                if quantidade <= 0:
                    self._add_error('quantidade', "Quantidade deve ser maior que zero")
            except ValueError:
                self._add_error('quantidade', "Quantidade deve ser um número")
        
        # Validações específicas do BPA Individualizado
        if 'sexo' in row and not pd.isna(row['sexo']):
            if str(row['sexo']).upper() not in ['M', 'F']:
                self._add_error('sexo', "Sexo deve ser 'M' ou 'F'")
        
        if 'carater_atendimento' in row and not pd.isna(row['carater_atendimento']):
            if str(row['carater_atendimento']).upper() not in ['1', '2', '3', '4']:
                self._add_error('carater_atendimento', "Caráter de atendimento inválido") 