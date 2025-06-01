import pytest
import io
from app.services.bpa_service import BPAService
from app.utils.bpa.generator_factory import BPAGeneratorFactory
from app.utils.cache import cache

class TestBPAService:
    """Testes de integração para o serviço de BPA"""
    
    @pytest.fixture
    def bpa_service(self):
        """Fixture com instância do serviço de BPA"""
        return BPAService()
    
    def test_generate_consolidado_file(self, bpa_service, sample_consolidado_data, monkeypatch):
        """Testa a geração de arquivo BPA Consolidado"""
        # Mock do DataFetcher
        def mock_fetch_data(*args, **kwargs):
            return sample_consolidado_data.to_dict('records')
        
        monkeypatch.setattr(bpa_service.data_fetcher, 'fetch_data_by_competencia', mock_fetch_data)
        
        # Gera o arquivo
        result = bpa_service.generate_bpa_file('202401', 'consolidado')
        
        # Verifica o resultado
        assert isinstance(result, io.BytesIO)
        content = result.getvalue().decode('utf-8')
        assert content
        assert len(content.split('\r\n')) == len(sample_consolidado_data) + 1  # +1 para a linha em branco final
    
    def test_generate_individualizado_file(self, bpa_service, sample_individualizado_data, monkeypatch):
        """Testa a geração de arquivo BPA Individualizado"""
        # Mock do DataFetcher
        def mock_fetch_data(*args, **kwargs):
            return sample_individualizado_data.to_dict('records')
        
        monkeypatch.setattr(bpa_service.data_fetcher, 'fetch_data_by_competencia', mock_fetch_data)
        
        # Gera o arquivo
        result = bpa_service.generate_bpa_file('202401', 'individualizado')
        
        # Verifica o resultado
        assert isinstance(result, io.BytesIO)
        content = result.getvalue().decode('utf-8')
        assert content
        assert len(content.split('\r\n')) == len(sample_individualizado_data) + 1  # +1 para a linha em branco final
    
    def test_generate_file_with_cache(self, bpa_service, sample_consolidado_data, monkeypatch):
        """Testa a geração de arquivo com cache"""
        # Mock do DataFetcher
        def mock_fetch_data(*args, **kwargs):
            return sample_consolidado_data.to_dict('records')
        
        monkeypatch.setattr(bpa_service.data_fetcher, 'fetch_data_by_competencia', mock_fetch_data)
        
        # Primeira geração (sem cache)
        result1 = bpa_service.generate_bpa_file('202401', 'consolidado')
        content1 = result1.getvalue().decode('utf-8')
        
        # Segunda geração (com cache)
        result2 = bpa_service.generate_bpa_file('202401', 'consolidado')
        content2 = result2.getvalue().decode('utf-8')
        
        # Verifica se os resultados são idênticos
        assert content1 == content2
    
    def test_generate_file_with_invalid_type(self, bpa_service):
        """Testa a geração de arquivo com tipo inválido"""
        with pytest.raises(ValueError) as exc_info:
            bpa_service.generate_bpa_file('202401', 'tipo_invalido')
        assert 'Tipo de BPA inválido' in str(exc_info.value)
    
    def test_generate_file_with_no_data(self, bpa_service, monkeypatch):
        """Testa a geração de arquivo sem dados"""
        # Mock do DataFetcher retornando lista vazia
        def mock_fetch_data(*args, **kwargs):
            return []
        
        monkeypatch.setattr(bpa_service.data_fetcher, 'fetch_data_by_competencia', mock_fetch_data)
        
        with pytest.raises(ValueError) as exc_info:
            bpa_service.generate_bpa_file('202401', 'consolidado')
        assert 'Nenhum registro encontrado' in str(exc_info.value)
    
    def test_generate_file_with_error(self, bpa_service, monkeypatch):
        """Testa a geração de arquivo com erro"""
        # Mock do DataFetcher lançando exceção
        def mock_fetch_data(*args, **kwargs):
            raise Exception("Erro simulado")
        
        monkeypatch.setattr(bpa_service.data_fetcher, 'fetch_data_by_competencia', mock_fetch_data)
        
        with pytest.raises(ValueError) as exc_info:
            bpa_service.generate_bpa_file('202401', 'consolidado')
        assert 'Erro ao gerar arquivo BPA' in str(exc_info.value) 