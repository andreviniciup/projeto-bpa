from app.utils.bpa.consolidado_generator import BPAConsolidadoGenerator
from app.utils.bpa.individualizado_generator import BPAIndividualizadoGenerator
from app.utils.logger import logger

class BPAGeneratorFactory:
    """Fábrica para criação de geradores de BPA"""
    
    @staticmethod
    def create_generator(bpa_type: str):
        """
        Cria um gerador de BPA baseado no tipo
        
        Args:
            bpa_type: Tipo do BPA ('consolidado' ou 'individualizado')
            
        Returns:
            Instância do gerador apropriado
            
        Raises:
            ValueError: Se o tipo de BPA for inválido
        """
        bpa_type = bpa_type.lower()
        
        if bpa_type == 'consolidado':
            logger.log_info("Criando gerador de BPA Consolidado")
            return BPAConsolidadoGenerator()
        elif bpa_type == 'individualizado':
            logger.log_info("Criando gerador de BPA Individualizado")
            return BPAIndividualizadoGenerator()
        else:
            error_msg = f"Tipo de BPA inválido: {bpa_type}"
            logger.log_error(error_msg)
            raise ValueError(error_msg) 