import unittest
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine, text
import pandas as pd
from src.scripts.Contrato_Agua.index import ProcessamentoDadosDimensaoAgua, TempoDimensaoAgua
from src.scripts.Conta_agua.index import FatoAgua
from sqlalchemy.exc import SQLAlchemyError
import os


class TestProcessamentoDadosDimensaoAgua(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.banco = 'mysql+pymysql://root:12345@localhost/tecsusbd_test'
        cls.engine = create_engine(cls.banco)

        # Setup a simple connection to ensure the database is reachable
        try:
            with cls.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
        except SQLAlchemyError as e:
            raise RuntimeError("Failed to connect to the database during setup.") from e

    def setUp(self):
        # Cria um arquivo CSV de exemplo para teste
        self.csv_file = 'test_data.csv'
        data = {
            'Nome do Contrato': ['Contrato 1', 'Contrato 2'],
            'Fornecedor': ['Fornecedor A', 'Fornecedor B'],
            'Forma de Pagamento': ['Mensal', 'Anual'],
            'Tipo de Acesso a Distribuidora': ['Residencial', 'Comercial'],
            'Vigência Inicial': ['01/01/2022', '01/06/2022'],
            'Vigência Final': ['01/01/2023', '01/06/2023'],
            'Número Cliente': [1, 2],
            'Campo Extra 3': ['12345678000195', '12345678000196'],
            'Campo Extra 4': ['', ''],
            'Tipo de Consumidor': ['Tipo A', 'Tipo B'],
            'Modelo de Faturamento': ['Modelo 1', 'Modelo 2'],
            'Número Contrato': [1, 2],
            'Código de Ligação (RGI)': ['RGI1', 'RGI2'],
            'Endereço de Instalação': ['Endereço 1', 'Endereço 2'],
            'Número Medidor': [1, 2],
            'Hidrômetro': ['H1', 'H2'],
            'Ativado': ['Sim', 'Não'],
            'Campo Extra 1': ['Cliente A', 'Cliente B']
        }
        df = pd.DataFrame(data)
        df.to_csv(self.csv_file, index=False)

    def tearDown(self):
        # Remove o arquivo CSV de exemplo após o teste
        if os.path.exists(self.csv_file):
            os.remove(self.csv_file)

    def test_carregar_dados(self):
        processador = ProcessamentoDadosDimensaoAgua(self.csv_file)
        df_tratado = processador.carregar_dados()

        self.assertFalse(df_tratado.empty, "DataFrame processado está vazio")

    def test_preprocessar_dados(self):
        processador = ProcessamentoDadosDimensaoAgua(self.csv_file)
        df = pd.read_csv(self.csv_file)
        df_tratado = processador.preprocessar_dados(df)

        self.assertIn('nome_do_contrato', df_tratado.columns)
        self.assertIn('fornecedor', df_tratado.columns)
        self.assertIn('vigencia_inicial', df_tratado.columns)
        self.assertIn('vigencia_final', df_tratado.columns)
        self.assertIn('numero_cliente', df_tratado.columns)

    def test_executar_etl(self):
        processador = ProcessamentoDadosDimensaoAgua(self.csv_file)
        df_tratado = processador.executar_etl()

        self.assertFalse(df_tratado.empty, "DataFrame processado está vazio")

    def test_conectar_banco(self):
        try:
            processador = ProcessamentoDadosDimensaoAgua(self.csv_file)
            processador.conectar_banco(self.banco)
            self.assertIsNotNone(processador.engine, "Conexão com o banco de dados falhou")
        except SQLAlchemyError as e:
            self.fail(f"Falha ao conectar ao banco de dados: {e}")



# class TestTempoDimensaoAgua(unittest.TestCase):
#     def setUp(self):
#         # Criar um DataFrame de exemplo
#         data = {
#             'data_id_vigencia_inicial': [1, 2],
#             'vigencia_inicial': ['2021-01-01', '2021-01-02'],
#             'dia_vigencia_inicial': [1, 2],
#             'mes_vigencia_inicial': [1, 1],
#             'ano_vigencia_inicial': [2021, 2021],
#             'trimestre_vigencia_inicial': [1, 1],
#             'semestre_vigencia_inicial': [1, 1],
#             'dia_da_semana_vigencia_inicial': ['Friday', 'Saturday'],
#             'mes_nome_vigencia_inicial': ['January', 'January'],
#             'data_id_vigencia_final': [1, 2],
#             'vigencia_final': ['2022-01-01', '2022-01-02'],
#             'dia_vigencia_final': [1, 2],
#             'mes_vigencia_final': [1, 1],
#             'ano_vigencia_final': [2022, 2022],
#             'trimestre_vigencia_final': [1, 1],
#             'semestre_vigencia_final': [1, 1],
#             'dia_da_semana_vigencia_final': ['Saturday', 'Sunday'],
#             'mes_nome_vigencia_final': ['January', 'January']
#         }
#         df = pd.DataFrame(data)
#         self.tempo_dimensao = TempoDimensaoAgua(df)
#
#     @patch('sqlalchemy.create_engine')
#     def test_conectar_banco(self, mock_create_engine):
#         # Simular a criação do engine
#         mock_engine = MagicMock()
#         mock_create_engine.return_value = mock_engine
#         self.tempo_dimensao.conectar_banco('sqlite:///:memory:')
#         mock_create_engine.assert_called_with('sqlite:///:memory:')
#         self.assertIsNotNone(self.tempo_dimensao.engine)
#
#     @patch('pandas.DataFrame.to_sql')
#     def test_inserir_banco(self, mock_to_sql):
#         # Preparar o ambiente mockado
#         self.tempo_dimensao.engine = create_engine('sqlite:///:memory:')
#         self.tempo_dimensao.inserir_banco()
#         # Verificar se o método to_sql foi chamado uma vez
#         mock_to_sql.assert_called_once()
#
#
# class TestFatoAgua(unittest.TestCase):
#     def setUp(self):
#         data = {
#             'hidrometro': ['123', '456'],
#             'codigo_de_ligacao_rgi': ['789', '012'],
#             'fato_agua_id': [1, 2],
#             'consumo_de_agua_m3': [100, 150],
#             'consumo_de_esgoto_m3': [50, 75],
#             'valor_agua': [200.0, 300.0],
#             'valor_esgoto': [100.0, 150.0],
#             'total_r': [300.0, 450.0],
#             'nivel_de_informacoes_da_fatura': ['Alto', 'Médio'],
#             'data_id_conta_do_mes': [1, 2],
#             'data_id_vencimento': [1, 2],
#             'data_id_emissao': [1, 2],
#             'data_id_leitura_anterior': [1, 2],
#             'data_id_leitura_atual': [1, 2]
#         }
#         df = pd.DataFrame(data)
#         self.fato_agua = FatoAgua(df)
#
#     @patch('sqlalchemy.create_engine')
#     def test_conectar_banco(self, mock_create_engine):
#         mock_engine = MagicMock()
#         mock_create_engine.return_value = mock_engine
#         self.fato_agua.conectar_banco('sqlite:///:memory:')
#         mock_create_engine.assert_called_with('sqlite:///:memory:')
#         self.assertIsNotNone(self.fato_agua.engine)
#
#     @patch('sqlalchemy.engine.base.Connection.execute')
#     @patch('pandas.DataFrame.to_sql')
#     def test_inserir_banco(self, mock_to_sql, mock_execute):
#         mock_engine = create_engine('sqlite:///:memory:')
#         self.fato_agua.engine = mock_engine
#
#         # Simulando o retorno do query
#         mock_execute.return_value.fetchone.return_value = (1, '001', 'C001')
#
#         self.fato_agua.inserir_banco()
#
#         # Verifica se to_sql foi chamado
#         mock_to_sql.assert_called_once()


if __name__ == '__main__':
    unittest.main()

