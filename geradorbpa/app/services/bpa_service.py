import io
import pandas as pd
from app.utils.bpa.generator_factory import BPAGeneratorFactory
from app.utils.fetch_data import DataFetcher
from app.utils.logger import logger
from app.utils.cache import cache
from app.config import settings

class BPAService:
    def __init__(self):
        self.data_fetcher = DataFetcher(schema="public")
    
    def generate_bpa_file(self, year_month: str, tipo_relatorio: str) -> io.BytesIO:
        """
        Gera o arquivo BPA
        
        Args:
            year_month: Mês/Ano de competência
            tipo_relatorio: Tipo do relatório ('consolidado' ou 'individualizado')
            
        Returns:
            Buffer contendo o arquivo gerado
            
        Raises:
            ValueError: Se houver erro na geração do arquivo
        """
        try:
            # Tenta obter dados do cache
            cache_key = f"bpa_data_{year_month}_{tipo_relatorio}"
            data = cache.get(cache_key)
            
            if data is None:
                logger.log_info(f"Buscando dados para competência {year_month}")
                data = self.data_fetcher.fetch_data_by_competencia(year_month)
                
                if not data:
                    logger.log_warning(f"Nenhum dado encontrado para competência {year_month}")
                    raise ValueError(f"Nenhum registro encontrado para competência {year_month}")
                
                # Armazena no cache
                cache.set(cache_key, data)
            
            # Converte para DataFrame
            df = pd.DataFrame(data)
            
            # Cria o gerador apropriado
            generator = BPAGeneratorFactory.create_generator(tipo_relatorio)
            
            # Gera o arquivo
            content = generator.process_data(df)
            
            # Cria o buffer de memória
            memoria = io.BytesIO()
            memoria.write(content.encode("utf-8"))
            memoria.seek(0)
            
            logger.log_info(f"Arquivo BPA {tipo_relatorio} gerado com sucesso")
            return memoria
            
        except Exception as e:
            error_msg = f"Erro ao gerar arquivo BPA: {str(e)}"
            logger.log_error(error_msg)
            raise ValueError(error_msg)