from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np


class Cliente_Agua:
    def __init__(self, df, tabela='dim_agua_cliente'):
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

        columns_expected = ['numero_cliente', 'numero_contrato', 'nome_cliente', 'cnpj',
                            'tipo_de_consumidor', 'modelo_de_faturamento']
        missing_cols = [col for col in columns_expected if col not in self.dataframe.columns]
        if missing_cols:
            print(f"Erro: Faltando colunas {missing_cols} no DataFrame.")
            return

        df_filtered = self.dataframe[columns_expected]

        try:
            df_filtered.to_sql(name=self.tabela, con=self.engine, if_exists='append', index=False)
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")


class TempoDimensaoAgua:
    def __init__(self, df, tabela='dim_tempo'):
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

        for coluna in ['vigencia_inicial', 'vigencia_final']:  # Ajuste conforme as colunas de datas que você tem
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


class Contrato_agua:
    def __init__(self, df, tabela='dim_agua_contrato'):
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

        columns_expected = ['numero_contrato', 'nome_do_contrato', 'fornecedor', 'forma_de_pagamento',
                            'tipo_de_acesso', 'data_id_vigencia_inicial', 'data_id_vigencia_final', 'ativado']
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


class Medidor_agua:
    def __init__(self, df, tabela='dim_agua_medidor'):
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

        columns_expected = ['numero_medidor', 'hidrometro', 'codigo_de_ligacao_rgi', 'numero_contrato', 'endereco_de_instalacao', 'numero_cliente']
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


class ProcessamentoDadosDimensaoAgua:
    def __init__(self, caminho_arquivo):
        self.caminho_arquivo = caminho_arquivo
        self.dataframe = pd.DataFrame()
        self.engine = None
        try:
            self.dataframe = self.carregar_dados()
        except Exception as e:
            print(f"Erro ao carregar dados na inicialização: {e}")

    def carregar_dados(self):
        df = pd.read_csv(self.caminho_arquivo)
        df = self.preprocessar_dados(df)
        return df

    def preprocessar_dados(self, df):
        colunas_renomeadas = {
            'Nome do Contrato': 'nome_do_contrato',
            'Fornecedor': 'fornecedor',
            'Forma de Pagamento': 'forma_de_pagamento',
            'Tipo de Acesso a Distribuidora': 'tipo_de_acesso',
            'Vigência Inicial': 'vigencia_inicial',
            'Vigência Final': 'vigencia_final',
            'Número Cliente': 'numero_cliente',
            'Campo Extra 3': 'cnpj1',
            'Campo Extra 4': 'cnpj2',
            'Tipo de Consumidor': 'tipo_de_consumidor',
            'Modelo de Faturamento': 'modelo_de_faturamento',
            'Número Contrato': 'numero_contrato',
            'Código de Ligação (RGI)': 'codigo_de_ligacao_rgi',
            'Endereço de Instalação': 'endereco_de_instalacao',
            'Número Medidor': 'numero_medidor',
            'Hidrômetro': 'hidrometro',
            'Ativado': 'ativado',
            'Campo Extra 1': 'nome_cliente'
        }
        df.rename(columns=colunas_renomeadas, inplace=True)
        df = self.prepara_nome_contrato(df)
        df = self.processar_cnpj(df)
        df = self.processar_hidrometro_rgi(df)
        df = self.gerar_identificadores(df)
        df = self.transformar_data(df, ['vigencia_inicial', 'vigencia_final'])
        df = self.preparar_dimensao_tempo(df, ['vigencia_inicial', 'vigencia_final'])
        return df

    def preparar_dimensao_tempo(self, df, colunas_data):
        id_data = 1
        for coluna in colunas_data:
            if (coluna in df.columns):
                df_temp = pd.to_datetime(df[coluna], errors='coerce', dayfirst=True)
                mask_valid_dates = df_temp.notna()
                df[f'data_id_{coluna}'] = None
                df[f'dia_{coluna}'] = None
                df[f'mes_{coluna}'] = None
                df[f'ano_{coluna}'] = None
                df[f'trimestre_{coluna}'] = None
                df[f'semestre_{coluna}'] = None
                df[f'dia_da_semana_{coluna}'] = None
                df[f'mes_nome_{coluna}'] = None
                if mask_valid_dates.any():
                    df.loc[mask_valid_dates, f'data_id_{coluna}'] = range(id_data, id_data + mask_valid_dates.sum())
                    id_data += mask_valid_dates.sum()
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
        df['nome_do_contrato'] = df['nome_do_contrato'].str.replace('Água/Esgoto - ', '', regex=False)
        return df

    def gerar_identificadores(self, df):
        id_contrato = 1
        id_cliente = 1
        id_medidor = 1
        if 'nome_do_contrato' in df.columns:
            df['numero_contrato'] = range(id_contrato, id_contrato + len(df))
            id_contrato += len(df)
        if 'nome_cliente' in df.columns:
            df['numero_cliente'] = range(id_cliente, id_cliente + len(df))
            id_cliente += len(df)
        if 'hidrometro' in df.columns:
            df['numero_medidor'] = range(id_medidor, id_medidor + len(df))
            id_medidor += len(df)
        return df

    def processar_cnpj(self, df):
        df['cnpj1'] = df['cnpj1'].apply(lambda x: pd.to_numeric(x, errors='coerce') if 'E+' in str(x) else x)
        df['cnpj2'] = df['cnpj2'].apply(lambda x: pd.to_numeric(x, errors='coerce') if 'E+' in str(x) else x)
        df['cnpj1'] = df['cnpj1'].astype(str).str.replace(r'[^\d]', '', regex=True)
        df['cnpj2'] = df['cnpj2'].astype(str).str.replace(r'[^\d]', '', regex=True)
        df['cnpj1'] = df['cnpj1'].apply(lambda x: x[:-1] if len(x) == 15 else x)
        df['cnpj2'] = df['cnpj2'].apply(lambda x: x[:-1] if len(x) == 15 else x)
        df['cnpj1'] = df['cnpj1'].replace(r'^\s*$', pd.NA, regex=True)
        df['cnpj2'] = df['cnpj2'].replace(r'^\s*$', pd.NA, regex=True)
        df['cnpj1'] = df['cnpj1'].replace(r'^0+$', pd.NA, regex=True)
        df['cnpj2'] = df['cnpj2'].replace(r'^0+$', pd.NA, regex=True)
        df['cnpj'] = np.where(pd.notna(df['cnpj1']), df['cnpj1'], df['cnpj2'])
        df.dropna(subset=['cnpj'], inplace=True)
        return df

    def processar_hidrometro_rgi(self, df):
        df = df.dropna(subset=['hidrometro', 'codigo_de_ligacao_rgi'])
        df = df[(df['hidrometro'].str.strip() != '') & (df['codigo_de_ligacao_rgi'].str.strip() != '')]
        return df

    def transformar_data(self, df, colunas):
        for coluna in colunas:
            df[coluna] = df[coluna].replace('Invalid date', pd.NaT)
            df[coluna] = pd.to_datetime(df[coluna], dayfirst=True, errors='coerce')
            df[coluna] = df[coluna].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)
        return df

    def salvar_dataframe_csv(self, caminho_saida):
        try:
            self.dataframe.to_csv(caminho_saida, index=False)
        except Exception as e:
            print(f"Erro ao salvar o DataFrame: {e}")

    def executar_etl(self):
        try:
            self.dataframe = self.carregar_dados()
            return self.dataframe
        except Exception as e:
            print(f"Erro ao executar ETL: {e}")
            return pd.DataFrame()

#
# caminho_arquivo = r'C:\Users\Marcelo\Documents\GitHub\Tecsus\Tecsus-ETL\data\raw\con_agua.csv'
# # banco = 'mysql+pymysql://root:12345@localhost/contas' #url de conexao
# banco = 'mysql+pymysql://root:12345@localhost:3306/tecsusbd'
# processador = ProcessamentoDadosDimensaoAgua(caminho_arquivo)
# df_tratado = processador.executar_etl()
#
# tempo = TempoDimensaoAgua(df_tratado)
# tempo.conectar_banco(banco)
# tempo.inserir_banco()
#
#
# contrato = Contrato_agua(df_tratado)
# contrato.conectar_banco(banco)
# contrato.inserir_banco()
#
# cliente_agua = Cliente_Agua(df_tratado)
# cliente_agua.conectar_banco(banco)
# cliente_agua.inserir_banco()
#
#
# medidor = Medidor_agua(df_tratado)
# medidor.conectar_banco(banco)
# medidor.inserir_banco()
