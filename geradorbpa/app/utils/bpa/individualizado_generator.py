from app.utils.bpa.base_generator import BaseBPAGenerator
from app.utils.bpa.validators import BPAIndividualizadoValidator
from app.utils.logger import logger

class BPAIndividualizadoGenerator(BaseBPAGenerator):
    """Gerador de BPA Individualizado"""
    
    def __init__(self):
        super().__init__()
        self.validator = BPAIndividualizadoValidator()
        self.line_type = "03"  # Tipo de linha para BPA Individualizado
    
    def _generate_line(self, row) -> str:
        """
        Gera uma linha de BPA Individualizado
        
        Args:
            row: Série do pandas com os dados da linha
            
        Returns:
            String formatada da linha
        """
        # Valida os dados
        if not self.validator.validate(row):
            logger.log_error("Dados inválidos para BPA Individualizado", {"row": row.to_dict()})
            return ""
        
        # Formata os campos
        cnes = self._format_numeric(row['cnes'], 7)
        competencia = self._format_numeric(row['competencia'], 6)
        cns_profissional = self._format_numeric(row['cns_profissional'], 15)
        cbo = self._format_numeric(row['cbo'], 6)
        data_atendimento = self._format_numeric(row['data_atendimento'], 8)
        folha = self._format_numeric(self.current_page, 3)
        sequencial = self._format_numeric(row['sequencial'], 3)
        procedimento = self._format_numeric(row['procedimento'], 10)
        cns_paciente = self._format_numeric(row['cns_paciente'], 15)
        sexo = self._format_field(row['sexo'], 1)
        codigo_municipio = self._format_numeric(row['codigo_municipio'], 7)
        cid = self._format_field(row['cid'], 4)
        idade = self._format_numeric(row['idade'], 3)
        quantidade = self._format_numeric(row['quantidade'], 3)
        carater_atendimento = self._format_field(row['carater_atendimento'], 1)
        numero_autorizacao = self._format_field(row['numero_autorizacao'], 13)
        origem = "EXT"  # Origem fixa
        nome_paciente = self._format_field(row['nome_paciente'], 60)
        data_nascimento = self._format_numeric(row['data_nascimento'], 8)
        raca = self._format_field(row['raca'], 2)
        etnia = self._format_field(row['etnia'], 4)
        nacionalidade = self._format_field(row['nacionalidade'], 3)
        servico = self._format_field(row['servico'], 2)
        classificacao = self._format_field(row['classificacao'], 2)
        equipe_seq = self._format_numeric(row['equipe_seq'], 3)
        equipe_area = self._format_field(row['equipe_area'], 2)
        cnpj = self._format_numeric(row['cnpj'], 14)
        cep = self._format_numeric(row['cep'], 8)
        codigo_logradouro = self._format_numeric(row['codigo_logradouro'], 7)
        endereco = self._format_field(row['endereco'], 60)
        complemento = self._format_field(row['complemento'], 20)
        numero = self._format_field(row['numero'], 6)
        bairro = self._format_field(row['bairro'], 30)
        telefone = self._format_field(row['telefone'], 11)
        email = self._format_field(row['email'], 60)
        ine = self._format_numeric(row['ine'], 7)
        
        # Monta a linha
        line = (
            f"{self.line_type}"
            f"{cnes}"
            f"{competencia}"
            f"{cns_profissional}"
            f"{cbo}"
            f"{data_atendimento}"
            f"{folha}"
            f"{sequencial}"
            f"{procedimento}"
            f"{cns_paciente}"
            f"{sexo}"
            f"{codigo_municipio}"
            f"{cid}"
            f"{idade}"
            f"{quantidade}"
            f"{carater_atendimento}"
            f"{numero_autorizacao}"
            f"{origem}"
            f"{nome_paciente}"
            f"{data_nascimento}"
            f"{raca}"
            f"{etnia}"
            f"{nacionalidade}"
            f"{servico}"
            f"{classificacao}"
            f"{equipe_seq}"
            f"{equipe_area}"
            f"{cnpj}"
            f"{cep}"
            f"{codigo_logradouro}"
            f"{endereco}"
            f"{complemento}"
            f"{numero}"
            f"{bairro}"
            f"{telefone}"
            f"{email}"
            f"{ine}\r\n"
        )
        
        return line 