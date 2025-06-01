from app.utils.bpa.base_generator import BaseBPAGenerator
from app.utils.bpa.validators import BPAConsolidadoValidator
from app.utils.logger import logger

class BPAConsolidadoGenerator(BaseBPAGenerator):
    """Gerador de BPA Consolidado"""
    
    def __init__(self):
        super().__init__()
        self.validator = BPAConsolidadoValidator()
        self.line_type = "02"  # Tipo de linha para BPA Consolidado
    
    def _generate_line(self, row) -> str:
        """
        Gera uma linha de BPA Consolidado
        
        Args:
            row: Série do pandas com os dados da linha
            
        Returns:
            String formatada da linha
        """
        # Valida os dados
        if not self.validator.validate(row):
            logger.log_error("Dados inválidos para BPA Consolidado", {"row": row.to_dict()})
            return ""
        
        # Formata os campos
        cnes = self._format_numeric(row['cnes'], 7)
        competencia = self._format_numeric(row['competencia'], 6)
        cbo = self._format_numeric(row['cbo'], 6)
        folha = self._format_numeric(self.current_page, 3)
        sequencial = self._format_numeric(row['sequencial'], 3)
        procedimento = self._format_numeric(row['procedimento'], 10)
        idade = self._format_numeric(row['idade'], 3)
        quantidade = self._format_numeric(row['quantidade'], 3)
        origem = "EXT"  # Origem fixa
        
        # Monta a linha
        line = (
            f"{self.line_type}"
            f"{cnes}"
            f"{competencia}"
            f"{cbo}"
            f"{folha}"
            f"{sequencial}"
            f"{procedimento}"
            f"{idade}"
            f"{quantidade}"
            f"{origem}\r\n"
        )
        
        return line 