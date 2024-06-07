from sqlalchemy import create_engine, text
import pandas as pd




class TempoFatoEnergia:
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

        columns_expected = ['data_id', 'data_full', 'dia', 'mes', 'ano', 'trimestre', 'semestre', 'dia_da_semana', 'mes_nome']
        dfs_to_insert = []

        for coluna in ['conta_do_mes', 'vencimento', 'emissao', 'leitura_anterior', 'leitura_atual']:  # Ajuste conforme as colunas de datas que você tem
            if all(item in self.dataframe.columns for item in [f'data_id_{coluna}', f'{coluna}', f'dia_{coluna}', f'mes_{coluna}', f'ano_{coluna}', f'trimestre_{coluna}', f'semestre_{coluna}', f'dia_da_semana_{coluna}', f'mes_nome_{coluna}']):
                df_temp = self.dataframe[[f'data_id_{coluna}', f'{coluna}', f'dia_{coluna}', f'mes_{coluna}', f'ano_{coluna}', f'trimestre_{coluna}', f'semestre_{coluna}', f'dia_da_semana_{coluna}', f'mes_nome_{coluna}']].dropna()
                df_temp.columns = columns_expected
                dfs_to_insert.append(df_temp)

        df_final = pd.concat(dfs_to_insert).drop_duplicates().reset_index(drop=True)

        try:
            df_final.to_sql(name=self.tabela, con=self.engine, if_exists='append', index=False)
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")


class FatoEnergia:
    def __init__(self, df, tabela='fato_energia_consumo'):
        self.dataframe = df
        self.engine = None
        self.tabela = tabela

    def conectar_banco(self, connection_string):
        try:
            self.engine = create_engine(connection_string)
            print("Conexão com o banco de dados estabelecida com sucesso.")
        except Exception as e:
            print(f"Falha ao conectar ao banco de dados: {e}")



    def buscar_chaves(self):
        if 'numero_da_instalacao' not in self.dataframe.columns:
            print("Coluna 'numero_da_instalacao' não encontrada no DataFrame. Não é possível buscar chaves.")
            return pd.DataFrame()

        query = text("""
        SELECT numero_medidor, numero_cliente, numero_contrato
        FROM dim_energia_medidor
        WHERE numero_da_instalacao = :numero_da_instalacao
        """)
        resultados = []
        with self.engine.connect() as connection:
            for _, row in self.dataframe.iterrows():
                params = {'numero_da_instalacao': row['numero_da_instalacao']}
                resultado = connection.execute(query, params).fetchone()
                if resultado:
                    resultados.append({
                        'fato_energia_id': row['fato_energia_id'],
                        'numero_medidor': resultado[0],
                        'numero_cliente': resultado[1],
                        'numero_contrato': resultado[2],
                        'consumo_em_ponta': row['consumo_em_ponta'],
                        'consumo_fora_de_ponta_capacidade': row['consumo_fora_de_ponta_capacidade'],
                        'consumo_fora_de_ponta_industrial': row['consumo_fora_de_ponta_industrial'],
                        'demanda_de_ponta_kw': row['demanda_de_ponta_kw'],
                        'demanda_fora_de_ponta_capacidade': row['demanda_fora_de_ponta_capacidade'],
                        'demanda_faturada_custo': row['demanda_faturada_custo'],
                        'demanda_faturada_pt_custo': row['demanda_faturada_pt_custo'],
                        'demanda_faturada_fp_custo': row['demanda_faturada_fp_custo'],
                        'demanda_ultrapassada_kw': row['demanda_ultrapassada_kw'],
                        'demanda_ultrapassada_kw': row['demanda_ultrapassada_kw'],
                        'demanda_ultrapassada_custo': row['demanda_ultrapassada_custo'],
                        'nivel_de_informacoes_da_fatura': row['nivel_de_informacoes_da_fatura'],
                        'data_id_leitura_anterior': row['data_id_leitura_anterior'],
                        'data_id_leitura_atual': row['data_id_leitura_atual'],
                        'data_id_emissao': row['data_id_emissao'],
                        'data_id_conta_do_mes': row['data_id_conta_do_mes'],
                        'data_id_vencimento': row['data_id_vencimento'],
                        'total_da_fatura': row['total_da_fatura']})
        return pd.DataFrame(resultados)

    def inserir_banco(self):
        if self.engine is None:
            print("Banco de dados não conectado.")
            return

        df_preparado = self.buscar_chaves()
        if not df_preparado.empty:
            df_preparado.to_sql(name=self.tabela, con=self.engine, if_exists='append', index=False)
            print("Dados inseridos com sucesso na tabela " + self.tabela + ".")
        else:
            print("Nenhum dado válido encontrado para inserção.")


class ProcessamentoDadosFatoEnergia:
    def __init__(self, caminho_arquivo, connection_string):
        self.connection_string = connection_string
        self.caminho_arquivo = caminho_arquivo
        self.engine = None
        self.dataframe = self.carregar_dados()

    def conectar_banco(self):
        try:
            self.engine = create_engine(self.connection_string)
            print("Conexão com o banco de dados estabelecida com sucesso.")
        except Exception as e:
            print(f"Falha ao conectar ao banco de dados: {e}")

        
    def carregar_dados(self):
        try:
            df = pd.read_csv(self.caminho_arquivo, dtype=str)
            colunas_lista = df.columns.tolist()
            df = self.preprocessar_dados(df)
            return df
        except Exception as e:
            print(f"Erro ao carregar dados: {e}")
            return pd.DataFrame()



    def preprocessar_dados(self, df):
        colunas_renomeadas = {
            'Consumo A PT (TE) Custo': 'consumo_em_ponta',
            'Demanda Ultrapassada Custo': 'demanda_ultrapassada_custo',
            'Demanda Ultrapassada (kW)': 'demanda_ultrapassada_kw',
            'Demanda Faturada FP Custo': 'demanda_faturada_pt_custo',
            'Demanda Faturada PT Custo': 'demanda_faturada_fp_custo',
            'Demanda Faturada Custo': 'demanda_faturada_custo',
            'Demanda FP IND (kW)': 'demanda_fora_de_ponta_industrial',
            'Demanda FP CAP (kW)': 'demanda_fora_de_ponta_capacidade',
            'Demanda PT (kW)': 'demanda_de_ponta_kw',
            'Consumo FP IND VD': 'consumo_fora_de_ponta_industrial',
            'Consumo FP CAP VD': 'consumo_fora_de_ponta_capacidade',
            'Nível de Informações da Fatura': 'nivel_de_informacoes_da_fatura', 
            'Conta do Mês': 'conta_do_mes',
            'Emissão': 'emissao',
            'Leitura Anterior': 'leitura_anterior',
            'Leitura Atual': 'leitura_atual',
            'Data de Vencimento': 'vencimento',
            'Número Instalação': 'numero_da_instalacao',
            'Total': 'total_da_fatura'
        }
        df.rename(columns=colunas_renomeadas, inplace=True)
        df = self.processar_numero_da_instalacao(df)
        df = self.gerar_identificadores(df)
        df = self.transformar_valores(df, [
            'demanda_de_ponta_kw', 'consumo_fora_de_ponta_capacidade', 'consumo_fora_de_ponta_industrial', 'demanda_fora_de_ponta_capacidade',
            'demanda_fora_de_ponta_industrial', 'demanda_faturada_custo', 'demanda_faturada_pt_custo',
            'demanda_faturada_fp_custo', 'demanda_ultrapassada_kw', 'demanda_ultrapassada_custo', 'total_da_fatura'
        ])


        df = self.transformar_data(df, ['leitura_anterior', 'leitura_atual', 
                                               'emissao', 'conta_do_mes', 'vencimento'])
        df = self.preparar_dimensao_tempo(df, ['leitura_anterior', 'leitura_atual', 
                                               'emissao', 'conta_do_mes', 'vencimento'])
        # Adicionado processamento adicional aqui se necessário
        
        return df

    def transformar_valores(self, df, colunas):
        for coluna in colunas:
            # Primeiro, remove as vírgulas que são usadas como separadores de milhares
            df[coluna] = df[coluna].replace({',': ''}, regex=True)
            # Depois, converte a string para float
            df[coluna] = df[coluna].astype(float)

        return df

    
    def processar_numero_da_instalacao(self, df):
        # Removendo linhas com valores nulos ou strings vazias
        df = df.dropna(subset=['numero_da_instalacao'])
        df = df[(df['numero_da_instalacao'].str.strip() != '')]
        return df

    def obter_ultimo_id_data(self):
        try:
            self.conectar_banco()
            if self.engine is None:
                print("Banco de dados não conectado.")
                return 0

            with self.engine.connect() as connection:
                result = connection.execute(text("SELECT MAX(data_id) FROM dim_energia_tempo"))
                ultimo_id = result.fetchone()[0]
                return ultimo_id if ultimo_id is not None else 0
        except Exception as e:
            print(f"Erro ao obter o último ID da tabela dim_energia_tempo: {e}")
            return 0

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

    def preparar_dimensao_tempo(self, df, colunas_data):
        id_data = self.obter_ultimo_id_data() + 1
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

    def gerar_identificadores(self, df):
        # Inicializa os contadores para os IDs
        fato_energia_id = 1

        # Gerar IDs para contratos
        if 'total_da_fatura' in df.columns:
            df['fato_energia_id'] = range(fato_energia_id, fato_energia_id + len(df))
            fato_energia_id += len(df)  # Atualiza o id_contrato para continuar a partir do último usado


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
        # self.salvar_dataframe_csv('dados_tratados_energia_fato.csv')
        return self.dataframe



# caminho_arquivo = r'C:\Users\Marcelo\Documents\GitHub\Tecsus\Tecsus-ETL\data\raw\pro_energia.csv'
# # banco = 'mysql+pymysql://root:12345@localhost/contas' #url de conexao
# banco = 'mysql+pymysql://root:12345@localhost:3306/tecsusbd'
#
# processador = ProcessamentoDadosFatoEnergia(caminho_arquivo, banco)
# df_tratado = processador.executar_etl()
#
#
# tempo = TempoFatoEnergia(df_tratado)
# tempo.conectar_banco(banco)
# tempo.inserir_banco()
#
#
# energia = FatoEnergia(df_tratado)
# energia.conectar_banco(banco)
# energia.inserir_banco()
