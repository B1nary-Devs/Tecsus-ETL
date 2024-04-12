from sqlalchemy import create_engine
import pandas as pd


from sqlalchemy import create_engine
import pandas as pd

class Contrato_agua:
    def __init__(self, caminho_arquivo):
        self.caminho_arquivo = caminho_arquivo
        self.dataframe = None
        self.engine = None

    def leitura_conta_agua(self):
        try:
            self.dataframe = pd.read_csv(self.caminho_arquivo)
            self.dataframe.rename(columns={
                'Nome do Contrato': 'nome_do_contrato',
                'Forma de Pagamento': 'forma_de_pagamento',
                'Tipo de Acesso a Distribuidora': 'tipo_de_acesso',
                'Vigência Inicial': 'vigencia_inicial',
                'Vigência Final': 'vigencia_final',
                'Observação': 'observacao',
                'Número Cliente': 'numero_cliente',
                'Campo Extra 3': 'cnpj',
                'Tipo de Consumidor': 'tipo_de_consumidor',
                'Modelo de Faturamento': 'modelo_de_faturamento',
                'Código de Ligação (RGI)': 'codigo_de_ligacao_rgi',
                'Endereço de Instalação': 'endereco_de_instalacao',
                'Número Medidor': 'numero_medidor',
                'Hidrômetro': 'hidrometro'
            }, inplace=True)
            self.formatar_cnpj()
            self.transformar_data(['vigencia_inicial', 'vigencia_final'])
        except Exception as e:
            print(f'Erro ao ler o arquivo {self.caminho_arquivo}: {e}')

    def conectar_banco(self):
        try:
            self.engine = create_engine('mysql+pymysql://root:12345@localhost/teste')
            print("Conexão com o banco de dados estabelecida com sucesso.")
        except Exception as e:
            print(f"Falha ao conectar ao banco de dados: {e}")

    def transformar_data(self, colunas):
        for coluna in colunas:
            self.dataframe[coluna] = pd.to_datetime(
                self.dataframe[coluna],
                dayfirst=True,
                format='%d/%m/%Y',  # Explicitly specifying the format
                errors='coerce'
            )
            self.dataframe[coluna] = self.dataframe[coluna].dt.strftime('%Y-%m-%d')
            self.dataframe[coluna] = self.dataframe[coluna].where(self.dataframe[coluna].notnull(), None)

    def formatar_cnpj(self):
        # Converter a coluna 'cnpj' para string
        self.dataframe['cnpj'] = self.dataframe['cnpj'].astype(str)
        # Remover caracteres não numéricos
        self.dataframe['cnpj'] = self.dataframe['cnpj'].str.replace(r'\D', '', regex=True)

    def inserir_banco(self, tabela):
        if self.engine is None:
            print("Banco de dados não conectado.")
            return

        # Filtering the DataFrame to include only the columns that exist in the database table
        columns_expected = ['nome_do_contrato', 'forma_de_pagamento', 'tipo_de_acesso', 'vigencia_inicial',
                            'vigencia_final', 'observacao', 'numero_cliente', 'cnpj', 'tipo_de_consumidor',
                            'modelo_de_faturamento', 'codigo_de_ligacao_rgi', 'endereco_de_instalacao', 'numero_medidor',
                            'hidrometro']
        df_filtered = self.dataframe[columns_expected]



        try:
            df_filtered.to_sql(name=tabela, con=self.engine, if_exists='append', index=False)
            print(f"Dados inseridos com sucesso na tabela {tabela}.")
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")

    def gerar_relatorio(self, nome_arquivo):
        try:
            self.dataframe.to_csv(nome_arquivo, index=False)
            print(f"Relatório gerado com sucesso: {nome_arquivo}")
        except Exception as e:
            print(f"Erro ao gerar relatório: {e}")

# Example usage:
caminho_arquivo = r'C:\Users\Marcelo\Documents\GitHub\etl\data\raw\con_agua.csv'
contrato_agua = Contrato_agua(caminho_arquivo)
contrato_agua.leitura_conta_agua()
contrato_agua.conectar_banco()
contrato_agua.inserir_banco('contrato_agua')
contrato_agua.gerar_relatorio('Arquivos_inseridos_contrato.csv')
