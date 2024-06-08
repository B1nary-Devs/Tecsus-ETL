from src.scripts.Contrato_Agua.index import ProcessamentoDadosDimensaoAgua, TempoDimensaoAgua
from sqlalchemy import create_engine, text
from src.scripts.Contrato_Agua.index import TempoDimensaoAgua
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd


class TestProcessamentoDadosDimensaoAgua(unittest.TestCase):

    @patch('src.scripts.Contrato_Agua.index.ProcessamentoDadosDimensaoAgua.carregar_dados')
    @patch('src.scripts.Contrato_Agua.index.ProcessamentoDadosDimensaoAgua.salvar_dataframe_csv')
    def test_executar_etl_comunicacao(self, mock_salvar_dataframe_csv, mock_carregar_dados):
        # Dados de teste
        sample_data = pd.DataFrame({
            'coluna1': ['valor1', 'valor2'],
            'coluna2': ['valor3', 'valor4']
        })

        # Configurar o mock para retornar os dados de teste em vez de carregar um arquivo
        mock_carregar_dados.return_value = sample_data

        # Instanciar o objeto de processamento de dados
        processador = ProcessamentoDadosDimensaoAgua('dummy_path.csv')

        # Verificar se o método carregar_dados foi chamado durante a inicialização
        mock_carregar_dados.assert_called_once()

        # Executar o método ETL
        resultado = processador.executar_etl()

        # Verificar se o método de salvar não foi chamado durante o ETL
        mock_salvar_dataframe_csv.assert_not_called()

        # Verificar se o resultado é um DataFrame (não nos preocupamos com os dados específicos aqui)
        self.assertIsInstance(resultado, pd.DataFrame)

    @patch('src.scripts.Contrato_Agua.index.ProcessamentoDadosDimensaoAgua.carregar_dados')
    @patch('src.scripts.Contrato_Agua.index.ProcessamentoDadosDimensaoAgua.salvar_dataframe_csv')
    def test_executar_etl_falha_carregar_dados(self, mock_salvar_dataframe_csv, mock_carregar_dados):
        # Configurar o mock para lançar uma exceção ao carregar os dados
        mock_carregar_dados.side_effect = Exception("Erro ao carregar dados")

        # Instanciar o objeto de processamento de dados
        processador = ProcessamentoDadosDimensaoAgua('dummy_path.csv')

        # Verificar se o método carregar_dados foi chamado durante a inicialização
        mock_carregar_dados.assert_called_once()

        # Verificar se o método de salvar não foi chamado durante o ETL
        mock_salvar_dataframe_csv.assert_not_called()

        # Executar o método ETL e verificar se a exceção é lançada
        with self.assertRaises(Exception) as context:
            processador.executar_etl()

        self.assertEqual(str(context.exception), "Erro ao carregar dados")

    @patch('src.scripts.Contrato_Agua.index.ProcessamentoDadosDimensaoAgua.carregar_dados')
    @patch('src.scripts.Contrato_Agua.index.ProcessamentoDadosDimensaoAgua.salvar_dataframe_csv')
    def test_executar_etl_falha_salvar_dados(self, mock_salvar_dataframe_csv, mock_carregar_dados):
        # Dados de teste
        sample_data = pd.DataFrame({
            'coluna1': ['valor1', 'valor2'],
            'coluna2': ['valor3', 'valor4']
        })

        # Configurar o mock para retornar os dados de teste em vez de carregar um arquivo
        mock_carregar_dados.return_value = sample_data

        # Configurar o mock para lançar uma exceção ao salvar os dados
        mock_salvar_dataframe_csv.side_effect = Exception("Erro ao salvar dados")

        # Instanciar o objeto de processamento de dados
        processador = ProcessamentoDadosDimensaoAgua('dummy_path.csv')

        # Verificar se o método carregar_dados foi chamado durante a inicialização
        mock_carregar_dados.assert_called_once()

        # Executar o método ETL
        resultado = processador.executar_etl()

        # Verificar se o método de salvar foi chamado durante o ETL
        mock_salvar_dataframe_csv.assert_called_once()

        # Verificar se o resultado é um DataFrame (não nos preocupamos com os dados específicos aqui)
        self.assertIsInstance(resultado, pd.DataFrame)

        # Verificar se a exceção é lançada ao tentar salvar os dados
        with self.assertRaises(Exception) as context:
            processador.salvar_dataframe_csv(resultado)

        self.assertEqual(str(context.exception), "Erro ao salvar dados")

class TestTempoDimensaoAgua(unittest.TestCase):

    def setUp(self):
        # Configurar o DataFrame de exemplo
        self.sample_data = pd.DataFrame({
            'data_id_vigencia_inicial': [1, 2],
            'vigencia_inicial': ['2020-01-01', '2020-01-02'],
            'dia_vigencia_inicial': [1, 2],
            'mes_vigencia_inicial': [1, 1],
            'ano_vigencia_inicial': [2020, 2020],
            'trimestre_vigencia_inicial': [1, 1],
            'semestre_vigencia_inicial': [1, 1],
            'dia_da_semana_vigencia_inicial': ['Wednesday', 'Thursday'],
            'mes_nome_vigencia_inicial': ['January', 'January'],
            'data_id_vigencia_final': [3, 4],
            'vigencia_final': ['2020-12-31', '2020-12-30'],
            'dia_vigencia_final': [31, 30],
            'mes_vigencia_final': [12, 12],
            'ano_vigencia_final': [2020, 2020],
            'trimestre_vigencia_final': [4, 4],
            'semestre_vigencia_final': [2, 2],
            'dia_da_semana_vigencia_final': ['Thursday', 'Wednesday'],
            'mes_nome_vigencia_final': ['December', 'December']
        })

        # Configurar o objeto de TempoDimensaoAgua
        self.temp_dim = TempoDimensaoAgua(self.sample_data)
        self.temp_dim.engine = create_engine('sqlite:///:memory:')  # Usar banco de dados em memória

        # Criar a tabela no banco de dados em memória
        with self.temp_dim.engine.connect() as connection:
            connection.execute(text("""
            CREATE TABLE dim_tempo (
                data_id INTEGER PRIMARY KEY,
                data_full TEXT,
                dia INTEGER,
                mes INTEGER,
                ano INTEGER,
                trimestre INTEGER,
                semestre INTEGER,
                dia_da_semana TEXT,
                mes_nome TEXT
            )
            """))

    def test_conectar_banco(self):
        with patch('src.scripts.Contrato_Agua.index.create_engine') as mock_create_engine:
            # Configurar o mock para criar engine
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine

            # Instanciar o objeto de TempoDimensaoAgua
            df_dummy = pd.DataFrame()
            temp_dim = TempoDimensaoAgua(df_dummy)

            # Chamar o método para conectar ao banco
            temp_dim.conectar_banco('dummy_connection_string')

            # Verificar se create_engine foi chamado com a string de conexão correta
            mock_create_engine.assert_called_once_with('dummy_connection_string')
            self.assertIsNotNone(temp_dim.engine)

    @patch('src.scripts.Contrato_Agua.index.create_engine')
    @patch('pandas.DataFrame.to_sql')
    def test_inserir_banco(self, mock_to_sql, mock_create_engine):
        # Configurar o mock para criar engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        # Instanciar o objeto de TempoDimensaoAgua com o mock de engine
        temp_dim = TempoDimensaoAgua(self.sample_data)
        temp_dim.conectar_banco('dummy_connection_string')

        # Mockar a conexão
        with patch.object(temp_dim.engine, 'connect', return_value=mock_engine):
            temp_dim.inserir_banco()

        # Verificar se o método to_sql foi chamado com os parâmetros corretos
        mock_to_sql.assert_called_once_with(
            name=temp_dim.tabela,
            con=mock_engine,
            if_exists='append',
            index=False
        )


if __name__ == '__main__':
    unittest.main()

