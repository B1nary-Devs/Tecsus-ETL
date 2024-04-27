import pandas as pd
from sqlalchemy import create_engine


class Conta_energia:
    def __init__(self, caminho_arquivo):
        self.caminho_arquivo = caminho_arquivo
        self.dataframe = None
        self.engine = None

    def leitura_conta_energia(self):
        try:
            # Lendo o arquivo
            self.dataframe = pd.read_csv(self.caminho_arquivo)

            # Renomear colunas 
            self.dataframe.rename(columns={
                'Data Vencimento': 'vencimento',
                'Data Emissao': 'emissao',
                'Leitura Anterior': 'leitura anterior',
                'Leitura Atual': 'leitura atual',
                'Consumo PT': 'consumo de energia na tarifa Ponta Alta',
                'Consumo FP': 'consumo de energia na tarifa Fora de Ponta',
                'Consumo TE': 'consumo de energia total',
                'Valor TUSD': 'valor da Tarifa de Uso do Sistema de Distribuição',
                'Valor FP': 'valor da Tarifa ',
                'Valor ICMS': 'Valor ICMS',
                'Valor PIS': 'valor pis',
                'Valor COFINS': 'valor cofins',
                'Valor INSS': 'valor inss',
                'Valor Total': 'valor total'
            }, inplace=True)

        except Exception as e:
            print(f'Erro ao ler o arquivo {self.caminho_arquivo}: {e}')

    def conectar_banco(self):
        # Ajuste as credenciais de conexão ao seu banco de dados
        try:
            self.engine = create_engine('mysql+pymysql://usuario:1234@localhost/teste')
            print("Conexão com o banco de dados estabelecida com sucesso.")
        except Exception as e:
            print(f"Falha ao conectar ao banco de dados: {e}")

    # Funções para transformar data e valores (adapte-as conforme necessário)
    def transformar_data(self, colunas):
        

    def transformar_valores(self, colunas):
        

    # Função para inserir dados no banco de dados (ajuste a tabela e colunas)
    def inserir_banco(self, tabela):
        

    # Função para gerar relatório (ajuste o nome do arquivo e colunas)
    def gerar_relatorio(self, nome_arquivo, colunas_para_reverter):
        