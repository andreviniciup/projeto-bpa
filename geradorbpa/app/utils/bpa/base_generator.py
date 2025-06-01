from abc import ABC, abstractmethod

class BaseBPAGenerator(ABC):
    """Classe base abstrata para geradores de BPA"""
    
    @abstractmethod
    def generate(self, data):
        """
        Método abstrato que deve ser implementado por todas as classes filhas
        para gerar o BPA com base nos dados fornecidos
        
        Args:
            data: Dados necessários para gerar o BPA
            
        Returns:
            O BPA gerado
        """
        pass
    
    @abstractmethod
    def validate(self, data):
        """
        Método abstrato que deve ser implementado por todas as classes filhas
        para validar os dados antes da geração do BPA
        
        Args:
            data: Dados a serem validados
            
        Returns:
            bool: True se os dados são válidos, False caso contrário
        """
        pass 