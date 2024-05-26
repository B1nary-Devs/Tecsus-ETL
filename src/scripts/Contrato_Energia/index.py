from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np



class Cliente_Energia:
    def __init__(self, df, tabela='dim_energia_cliente'):
        self.dataframe = df
        self.engine = None
        self.tabela = tabela

    def conectar_banco(self, connection_string):
        try:
            self.engine = create_engine(connection_string)
            print("Conexão com o banco de dados estabelecida com sucesso.")
        except Exception as e:
            print(f"Falha ao conectar ao banco de dados: {e}")

    def inserir_banco(self):
        if self.engine is None:
            print("Banco de dados não conectado.")
            return

        columns_expected = ['numero_cliente', 'numero_contrato', 'tipo_de_consumidor', 'cnpj']
        missing_cols = [col for col in columns_expected if col not in self.dataframe.columns]
        if missing_cols:
            print(f"Erro: Faltando colunas {missing_cols} no DataFrame.")
            return

        df_filtered = self.dataframe[columns_expected]

        try:
            df_filtered.to_sql(name=self.tabela, con=self.engine, if_exists='append', index=False)
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")


class TempoDimensaoEnergia:
class TempoDimensaoEnergia:
    def __init__(self, df, tabela='dim_energia_tempo'):
        self.dataframe = df
        self.engine = None
        self.tabela = tabela

    def conectar_banco(self, connection_string):
        try:
            self.engine = create_engine(connection_string)
            print("Conexão com o banco de dados estabelecida com sucesso.")
        except Exception as e:
            print(f"Falha ao conectar ao banco de dados: {e}")

    def inserir_banco(self):
        if self.engine is None:
            print("Banco de dados não conectado.")
            return

        # Preparar DataFrame para inserção, adaptando para o esperado pela tabela específica
        columns_expected = ['data_id', 'data_full', 'dia', 'mes', 'ano', 'trimestre', 'semestre', 'dia_da_semana', 'mes_nome']
        dfs_to_insert = []

        for coluna in ['vigencia_inicial_id', 'vigencia_final_id']:  # Ajuste conforme as colunas de datas que você tem
            df_temp = self.dataframe[[f'data_id_{coluna}', f'{coluna}', f'dia_{coluna}', f'mes_{coluna}',
                                      f'ano_{coluna}', f'trimestre_{coluna}', f'semestre_{coluna}',
                                      f'dia_da_semana_{coluna}', f'mes_nome_{coluna}']].dropna()
            df_temp.columns = columns_expected  # Renomear colunas para corresponder às da tabela
            dfs_to_insert.append(df_temp)

        df_final = pd.concat(dfs_to_insert).drop_duplicates().reset_index(drop=True)

        try:
            df_final.to_sql(name=self.tabela, con=self.engine, if_exists='append', index=False)
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")


class Contrato_energia:
    def __init__(self, df, tabela='dim_energia_contrato'):
        self.dataframe = df
        self.engine = None
        self.tabela = tabela

    def conectar_banco(self, connection_string):
        try:
            self.engine = create_engine(connection_string)
            print("Conexão com o banco de dados estabelecida com sucesso.")
        except Exception as e:
            print(f"Falha ao conectar ao banco de dados: {e}")

    def inserir_banco(self):
        if self.engine is None:
            print("Banco de dados não conectado.")
            return

        columns_expected = ['numero_contrato', 'nome_do_contrato', 'fornecedor', 'classe',
                             'data_id_vigencia_inicial_id', 'data_id_vigencia_final_id', 'ativado', 'horario_de_ponta', 'demanda_ponta', 'demanda_fora_ponta',
                            'tensao_contratada'
                            ]
        # Verificar se todas as colunas esperadas estão presentes
        missing_cols = [col for col in columns_expected if col not in self.dataframe.columns]
        if missing_cols:
            print(f"Erro: Faltando colunas {missing_cols} no DataFrame.")
            return

        df_filtered = self.dataframe[columns_expected]

        try:
            df_filtered.to_sql(name=self.tabela, con=self.engine, if_exists='append', index=False)
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")


class Medidor_energia:
    def __init__(self, df, tabela='dim_energia_medidor'):
        self.dataframe = df
        self.engine = None
        self.tabela = tabela

    def conectar_banco(self, connection_string):
        try:
            self.engine = create_engine(connection_string)
            print("Conexão com o banco de dados estabelecida com sucesso.")
        except Exception as e:
            print(f"Falha ao conectar ao banco de dados: {e}")

    def inserir_banco(self):
        if self.engine is None:
            print("Banco de dados não conectado.")
            return

        columns_expected = ['numero_medidor', 'numero_da_instalacao', 'endereco_de_instalacao', 'numero_contrato', 'numero_cliente']
        # Verificar se todas as colunas esperadas estão presentes
        missing_cols = [col for col in columns_expected if col not in self.dataframe.columns]
        if missing_cols:
            print(f"Erro: Faltando colunas {missing_cols} no DataFrame.")
            return

        df_filtered = self.dataframe[columns_expected]

        try:
            df_filtered.to_sql(name=self.tabela, con=self.engine, if_exists='append', index=False)
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")


class ProcessamentoDadosDimensaoEnergia:
class ProcessamentoDadosDimensaoEnergia:
    def __init__(self, caminho_arquivo):
        self.caminho_arquivo = caminho_arquivo
        self.dataframe = self.carregar_dados()
        self.engine = None

    def carregar_dados(self):
        try:
            df = pd.read_csv(self.caminho_arquivo, encoding='utf-8')
            df = pd.read_csv(self.caminho_arquivo, encoding='utf-8')
            df = self.preprocessar_dados(df)
            return df
        except Exception as e:
            print(f"Erro ao carregar dados: {e}")
            return pd.DataFrame()

    def preprocessar_dados(self, df):
        colunas_renomeadas = {
            'Horário de Ponta': 'horario_de_ponta',
            'Planta': 'planta',
            'Classe': 'classe',
            'Nome do Contrato': 'nome_do_contrato',
            'Código de Identificação': 'codigo_de_identificacao',
            'Código Fiscal de Operação': 'codigo_fiscal_de_operacao',
            'Roteiro de Leitura': 'roteiro_de_leitura',
            'Tipo de Consumidor (Energia)': 'tipo_de_consumidor',
            'Número Instalação': 'numero_da_instalacao',
            'Número Medidor': 'numero_medidor',
            'Número Cliente': 'numero_cliente',
            'Modalidade': 'modalidade',
            'Demanda Ponta': 'demanda_ponta',
            'Demanda Fora Ponta': 'demanda_fora_ponta',
            'Tensão Contratada (V)': 'tensao_contratada',
            'Número Contrato': 'numero_contrato',
            'Inscrição Cadastral do Imóvel': 'inscricao_cadastral_do_imovel',
            'Código de Consumidor': 'codigo_de_consumidor',
            'Endereço de Instalação': 'endereco_de_instalacao',
            'Vigência Inicial': 'vigencia_inicial_id',
            'Vigência Final': 'vigencia_final_id',
            'Ativado': 'ativado',
            'Campo Extra 3': 'cnpj1',
            'Campo Extra 4': 'cnpj2',
            'Fornecedor': 'fornecedor'}
        df.rename(columns=colunas_renomeadas, inplace=True)

        df = self.prepara_nome_contrato(df)
        df = self.processar_cnpj(df)
        df = self.processar_numero_instalacao(df)
        # df = self.transformar_valores(df, ['tensao_contratada'])
        df = self.gerar_identificadores(df)
        df = self.transformar_data(df, ['vigencia_inicial_id', 'vigencia_final_id'])
        df = self.preparar_dimensao_tempo(df, ['vigencia_inicial_id', 'vigencia_final_id'])
        return df

    def transformar_valores(self, df, colunas):
        for coluna in colunas:
            # Primeiro, remove as vírgulas que são usadas como separadores de milhares
            df[coluna] = df[coluna].replace({',': ''}, regex=True)
            # Depois, converte a string para float
            df[coluna] = df[coluna].astype(float)

        return df

    def preparar_dimensao_tempo(self, df, colunas_data):
        id_data = 1
        for coluna in colunas_data:
            if coluna in df.columns:
                # Converter para datetime, usando coerce para transformar falhas em NaT
                df_temp = pd.to_datetime(df[coluna], errors='coerce', dayfirst=True)

                # Criar uma máscara que identifica onde as datas são válidas (não NaT)
                mask_valid_dates = df_temp.notna()

                # Inicializar a coluna de ID com None para todo o DataFrame
                df[f'data_id_{coluna}'] = None
                df[f'dia_{coluna}'] = None
                df[f'mes_{coluna}'] = None
                df[f'ano_{coluna}'] = None
                df[f'trimestre_{coluna}'] = None
                df[f'semestre_{coluna}'] = None
                df[f'dia_da_semana_{coluna}'] = None
                df[f'mes_nome_{coluna}'] = None

                # Atribuir IDs apenas nas posições onde há datas válidas
                if mask_valid_dates.any():  # Verifica se há pelo menos uma data válida
                    # Atribuir os IDs de forma incremental apenas onde as datas são válidas
                    df.loc[mask_valid_dates, f'data_id_{coluna}'] = range(id_data, id_data + mask_valid_dates.sum())
                    id_data += mask_valid_dates.sum()  # Incrementar o contador de ID baseado no número de datas válidas
                    # Extrair e atribuir o dia, mês, ano, trimestre, semestre, dia da semana e nome do mês da data válida
                    df.loc[mask_valid_dates, f'dia_{coluna}'] = df_temp.dt.day
                    df.loc[mask_valid_dates, f'mes_{coluna}'] = df_temp.dt.month
                    df.loc[mask_valid_dates, f'ano_{coluna}'] = df_temp.dt.year
                    df.loc[mask_valid_dates, f'trimestre_{coluna}'] = df_temp.dt.quarter
                    df.loc[mask_valid_dates, f'semestre_{coluna}'] = ((df_temp.dt.month - 1) // 6) + 1
                    df.loc[mask_valid_dates, f'dia_da_semana_{coluna}'] = df_temp.dt.day_name()
                    df.loc[mask_valid_dates, f'mes_nome_{coluna}'] = df_temp.dt.month_name()


            else:
                print(f'A coluna {coluna} não está presente no DataFrame.')
        return df

    def prepara_nome_contrato(self, df):

        df['nome_do_contrato'] = df['nome_do_contrato'].str.replace('Energia - ', '', regex=False)
        df['nome_do_contrato'] = df['nome_do_contrato'].str.replace('ENERGIA - ', '', regex=False)
        df['nome_do_contrato'] = df['nome_do_contrato'].str.replace('Energia A - ', '', regex=False)
        df['nome_do_contrato'] = df['nome_do_contrato'].str.replace('Energia B - ', '', regex=False)

        return df

    def gerar_identificadores(self, df):
        # Inicializa os contadores para os IDs
        id_contrato = 1
        id_cliente = 1
        id_medidor = 1

        # Gerar IDs para contratos
        if 'nome_do_contrato' in df.columns:
            df['numero_contrato'] = range(id_contrato, id_contrato + len(df))
            id_contrato += len(df)  # Atualiza o id_contrato para continuar a partir do último usado

        # Gerar IDs para clientes, assumindo que cada cliente é tratado individualmente
        if 'cnpj' in df.columns:
            df['numero_cliente'] = range(id_cliente, id_cliente + len(df))
            id_cliente += len(df)  # Atualiza o id_cliente para continuar a partir do último usado

        # Gerar IDs para medidores, assumindo que cada medidor é único
        if 'numero_da_instalacao' in df.columns:
            df['numero_medidor'] = range(id_medidor, id_medidor + len(df))
            id_medidor += len(df)  # Atualiza o id_medidor para continuar a partir do último usado

        return df

    def processar_cnpj(self, df):
        # Aplica conversão numérica somente se o valor contém 'E+'
        df['cnpj1'] = df['cnpj1'].apply(lambda x: pd.to_numeric(x, errors='coerce') if 'E+' in str(x) else x)
        df['cnpj2'] = df['cnpj2'].apply(lambda x: pd.to_numeric(x, errors='coerce') if 'E+' in str(x) else x)

        # Removendo caracteres especiais
        df['cnpj1'] = df['cnpj1'].astype(str).str.replace(r'[^\d]', '', regex=True)
        df['cnpj2'] = df['cnpj2'].astype(str).str.replace(r'[^\d]', '', regex=True)

        # Verificando o comprimento e ajustando para 14 dígitos se tiver 15 dígitos
        df['cnpj1'] = df['cnpj1'].apply(lambda x: x[:-1] if len(x) == 15 else x)
        df['cnpj2'] = df['cnpj2'].apply(lambda x: x[:-1] if len(x) == 15 else x)

        # Tratando strings vazias como NaN
        df['cnpj1'] = df['cnpj1'].replace(r'^\s*$', pd.NA, regex=True)
        df['cnpj2'] = df['cnpj2'].replace(r'^\s*$', pd.NA, regex=True)

        # Tratando CNPJs compostos inteiramente por zeros como NaN
        df['cnpj1'] = df['cnpj1'].replace(r'^0+$', pd.NA, regex=True)
        df['cnpj2'] = df['cnpj2'].replace(r'^0+$', pd.NA, regex=True)

        # Preenchendo a coluna 'cnpj' com 'cnpj1' se não for nulo, caso contrário com 'cnpj2'
        df['cnpj'] = np.where(pd.notna(df['cnpj1']), df['cnpj1'], df['cnpj2'])

        # Removendo linhas onde a coluna 'cnpj' é NA
        df.dropna(subset=['cnpj'], inplace=True)

        return df

    def processar_numero_instalacao(self, df):
        # Removendo linhas com valores nulos ou strings vazias
        df = df.dropna(subset=['numero_da_instalacao'])
        df = df[(df['numero_da_instalacao'].str.strip() != '')]

        return df

    def transformar_data(self, df, colunas):
        for coluna in colunas:
            # Substitui 'Invalid date' por pd.NaT
            df[coluna] = df[coluna].replace('Invalid date', pd.NaT)

            # Converte strings para datetime, assume o primeiro número como dia se ele for menor ou igual a 12
            df[coluna] = pd.to_datetime(
                df[coluna],
                dayfirst=True,
                errors='coerce'  # Converte entradas inválidas em NaT
            )

            # Agora, formata as datas válidas para o formato 'yyyy-mm-dd'
            # e deixa como None (ou pd.NaT) as entradas que não puderam ser convertidas
            df[coluna] = df[coluna].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)

        return df

    def salvar_dataframe_csv(self, caminho_saida):
        try:
            self.dataframe.to_csv(caminho_saida, index=False)
        except Exception as e:
            print(f"Erro ao salvar o DataFrame: {e}")

    def executar_etl(self):
        # Salvando os dados tratados para revisão
        self.salvar_dataframe_csv('dados_tratados_dimensao_energia.csv')

        return self.dataframe
    


# caminho_arquivo = r'C:\Users\Gilherme Alves\Documents\github\tecsus\etl\Tecsus-ETL\data\raw\con_energia.csv'
# banco = 'mysql+pymysql://root:1234@localhost/sonar' #url de conexao
# processador = ProcessamentoDadosDimensao(caminho_arquivo)
# df_tratado = processador.executar_etl()
#
# tempo = TempoDimensao(df_tratado)
# tempo.conectar_banco(banco)
# tempo.inserir_banco()
#
#
# contrato = Contrato_energia(df_tratado)
# contrato.conectar_banco(banco)
# contrato.inserir_banco()
#
#
# cliente_energia = Cliente_Energia(df_tratado)
# cliente_energia.conectar_banco(banco)
# cliente_energia.inserir_banco()
#
#
# medidor = Medidor_energia(df_tratado)
# medidor.conectar_banco(banco)
# medidor.inserir_banco()
# caminho_arquivo = r'C:\Users\Gilherme Alves\Documents\github\tecsus\etl\Tecsus-ETL\data\raw\con_energia.csv'
# banco = 'mysql+pymysql://root:1234@localhost/sonar' #url de conexao
# processador = ProcessamentoDadosDimensao(caminho_arquivo)
# df_tratado = processador.executar_etl()
#
# tempo = TempoDimensao(df_tratado)
# tempo.conectar_banco(banco)
# tempo.inserir_banco()
#
#
# contrato = Contrato_energia(df_tratado)
# contrato.conectar_banco(banco)
# contrato.inserir_banco()
#
#
# cliente_energia = Cliente_Energia(df_tratado)
# cliente_energia.conectar_banco(banco)
# cliente_energia.inserir_banco()
#
#
# medidor = Medidor_energia(df_tratado)
# medidor.conectar_banco(banco)
# medidor.inserir_banco()
