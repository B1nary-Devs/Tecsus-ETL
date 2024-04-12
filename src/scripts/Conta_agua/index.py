from sqlalchemy import create_engine
import pandas as pd


class Conta_agua:
    def __init__(self, caminho_arquivo):
        self.caminho_arquivo = caminho_arquivo
        self.dataframe = None
        self.engine = None

    def leitura_conta_agua(self):
        try:
            # Lendo o arquivo CSV para o DataFrame
            self.dataframe = pd.read_csv(self.caminho_arquivo)

            self.dataframe.rename(columns={
                'Conta do Mês': 'contadomes',
                'Vencimento': 'vencimento',
                'Emissão': 'emissao',
                'Leitura Anterior': 'leituraanterior',
                'Leitura Atual': 'leituraatual',
                'Consumo de Água m³': 'consumodeaguam3',  # Ajuste no nome para minúsculas e sem espaço
                'Consumo de Esgoto m³': 'consumodeesgotom3',  # Ajuste no nome para minúsculas e sem espaço
                'Valor Água R$': 'valoraguar$',  # Verifique o nome original no CSV
                'Valor Esgoto R$': 'valoresgotor$',  # Correção de digitação, deve ser 'valoresgotor$'
                'Total R$': 'totalr$',  # Correção de digitação
                'Nível de Informações da Fatura': 'niveldeinformacoesdafatura',
                'Código de Ligação (RGI)': 'codigodeligacaorgi',
                'Hidrômetro': 'hidrometro'
            }, inplace=True)

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
                errors='coerce'  # Converte entradas inválidas em NaT
            )
            self.dataframe[coluna] = self.dataframe[coluna].dt.strftime('%Y-%m-%d')
            self.dataframe[coluna] = self.dataframe[coluna].where(self.dataframe[coluna].notnull(), None)

    def reverter_transformacao_data(self, colunas):
        for coluna in colunas:
            # Assumindo que a coluna pode não estar em datetime, tentamos converter
            self.dataframe[coluna] = pd.to_datetime(
                self.dataframe[coluna],
                errors='coerce'  # Converte entradas inválidas em NaT
            )
            # Formatando para o formato brasileiro dd/mm/yyyy
            self.dataframe[coluna] = self.dataframe[coluna].dt.strftime('%d/%m/%Y')
            # Mantendo None onde o valor é NaT (não alterando o comportamento original)
            self.dataframe[coluna] = self.dataframe[coluna].where(self.dataframe[coluna].notnull(), None)

    def transformar_valores(self, colunas):
        for coluna in colunas:
            # Primeiro, remove as vírgulas que são usadas como separadores de milhares
            self.dataframe[coluna] = self.dataframe[coluna].replace({',': ''}, regex=True)
            # Depois, converte a string para float
            self.dataframe[coluna] = self.dataframe[coluna].astype(float)

    def inserir_banco(self, tabela):
        if self.engine is None:
            print("Banco de dados não conectado.")
            return

        colunas_selecionadas = [
            'contadomes', 'vencimento', 'emissao', 'leituraanterior', 'leituraatual',
            'consumodeaguam3', 'consumodeesgotom3', 'valoraguar$', 'valoresgotor$', 'totalr$',
            'niveldeinformacoesdafatura', 'codigodeligacaorgi', 'hidrometro'
        ]

        # Filtrar o DataFrame para conter apenas as colunas selecionadas
        df_selecionado = self.dataframe[colunas_selecionadas]

        try:
            df_selecionado.to_sql(name=tabela, con=self.engine, if_exists='append', index=False)
            print(f"Dados inseridos com sucesso na tabela {tabela}.")

        except Exception as e:
            print(f"Erro ao inserir dados: {e}")

    def gerar_relatorio(self, nome_arquivo, colunas_para_reverter):
        # Primeiro, reverter a transformação das datas
        self.reverter_transformacao_data(colunas_para_reverter)

        # Depois, salvar o DataFrame em um arquivo CSV
        self.dataframe.to_csv(nome_arquivo, index=False)
        print(f"Relatório gerado com sucesso: {nome_arquivo}")


# Exemplo de como você deve instanciar e usar a sua classe
caminho_arquivo = r'C:\Users\Marcelo\Documents\GitHub\etl\data\raw\pro_agua.csv'
conta_agua = Conta_agua(caminho_arquivo)
conta_agua.leitura_conta_agua()
conta_agua.transformar_data(
    ['contadomes', 'vencimento', 'emissao', 'leituraanterior', 'leituraatual']
)
conta_agua.transformar_valores(['valoraguar$', 'valoresgotor$',
                                'totalr$', 'consumodeaguam3', 'consumodeesgotom3'])

conta_agua.conectar_banco()
conta_agua.inserir_banco('conta_agua')
conta_agua.gerar_relatorio('Arquivos_inseridos.csv', ['contadomes', 'vencimento',
                                                      'emissao', 'leituraanterior', 'leituraatual'])
