import os
import glob
from src.scripts.Contrato_Agua.index import *
from src.scripts.Conta_agua.index import *
from src.scripts.Contrato_Energia.index import *
from src.scripts.Conta_Energia.index import *
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


# Configurações do banco de dados
username = 'binary'
password = 'tecsus123@'
host = 'tecsusbd.mysql.database.azure.com'
database = ''  # Nome do banco de dados, se necessário
ssl_mode = 'require'

# Escapar a senha
escaped_password = quote_plus(password)

# String de conexão
banco_sem = f'mysql+pymysql://{username}:{escaped_password}@{host}/'

banco = 'mysql+pymysql://root:12345@localhost/tecsusbd'



def setup_database():

    engine = create_engine(
            banco_sem,
            connect_args={
                "ssl": {
                    "ssl_mode": "REQUIRED"
                }
            }
        )

    try:

        with engine.connect() as connection:

            connection.execute(text("DROP TABLE IF EXISTS tecsusbd;"))

            connection.execute(text("CREATE DATABASE tecsusbd;"))
            connection.execute(text("USE tecsusbd;"))

            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_tempo (
                data_id int AUTO_INCREMENT primary key,
                data_full date,
                dia int,
                mes int,
                ano int,
                trimestre int,
                semestre int,
                dia_da_semana varchar(10),
                mes_nome varchar(15)
                );
            """))
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_agua_contrato (
                numero_contrato int AUTO_INCREMENT primary key,
                nome_do_contrato varchar(255),
                fornecedor varchar(255),
                forma_de_pagamento varchar(50),
                tipo_de_acesso varchar(50),
                data_id_vigencia_inicial int,
                data_id_vigencia_final int,
                ativado varchar(30),
                foreign key (data_id_vigencia_inicial) references dim_tempo(data_id),
                foreign key (data_id_vigencia_final) references dim_tempo(data_id)
                );
            """))
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_agua_cliente (
                numero_cliente int AUTO_INCREMENT primary key,
                numero_contrato int,
                nome_cliente varchar(255),
                cnpj varchar(14),
                tipo_de_consumidor varchar(50),
                modelo_de_faturamento varchar(255),
                foreign key (numero_contrato) references dim_agua_contrato(numero_contrato)
                );
            """))
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_agua_medidor (
                numero_medidor int AUTO_INCREMENT primary key,
                hidrometro varchar(255),
                codigo_de_ligacao_rgi varchar(50),
                numero_contrato int,
                endereco_de_instalacao varchar(255),
                numero_cliente int,
                foreign key (numero_cliente) references dim_agua_cliente(numero_cliente),
                foreign key (numero_contrato) references dim_agua_contrato(numero_contrato)
                );
            """))
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS fato_agua_consumo (
                fato_agua_id int AUTO_INCREMENT primary key,
                consumo_de_agua_m3 decimal(9,3),
                consumo_de_esgoto_m3 decimal(9,3),
                valor_agua float,
                valor_esgoto float,
                total_r float,
                nivel_de_informacoes_da_fatura varchar(21),
                numero_cliente int,
                numero_medidor int,
                numero_contrato int,
                data_id_conta_do_mes int,
                data_id_vencimento int,
                data_id_emissao int,
                data_id_leitura_anterior int,
                data_id_leitura_atual int,
                foreign key (data_id_conta_do_mes) references dim_tempo(data_id),
                foreign key (data_id_vencimento) references dim_tempo(data_id),
                foreign key (data_id_emissao) references dim_tempo(data_id),
                foreign key (data_id_leitura_anterior) references dim_tempo(data_id),
                foreign key (data_id_leitura_atual) references dim_tempo(data_id),
                foreign key (numero_cliente) references dim_agua_cliente(numero_cliente),
                foreign key (numero_medidor) references dim_agua_medidor(numero_medidor),
                foreign key (numero_contrato) references dim_agua_contrato(numero_contrato)
                );
            """))
            # energia
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_energia_tempo (
                data_id INT AUTO_INCREMENT PRIMARY KEY,
                data_full DATE,
                dia INT,
                mes INT,
                ano INT,
                trimestre INT,
                semestre INT,
                dia_da_semana VARCHAR(10),
                mes_nome VARCHAR(15)
                );
            """))
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_energia_contrato (
                numero_contrato INT PRIMARY KEY,
                nome_do_contrato VARCHAR(255),
                fornecedor VARCHAR(255),
                classe VARCHAR(255),
                horario_de_ponta VARCHAR(20),
                demanda_ponta varchar(10),
                demanda_fora_ponta varchar(10),
                tensao_contratada varchar(20),
                data_id_vigencia_inicial_id INT,
                data_id_vigencia_final_id INT,
                ativado varchar(30),
                FOREIGN KEY (data_id_vigencia_inicial_id) REFERENCES dim_energia_tempo(data_id),
                FOREIGN KEY (data_id_vigencia_final_id) REFERENCES dim_energia_tempo(data_id)
               );
            """))
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_energia_cliente (
                numero_cliente INT AUTO_INCREMENT PRIMARY KEY,
                numero_contrato INT,
                cnpj VARCHAR(20),
                tipo_de_consumidor VARCHAR(50),
                FOREIGN KEY (numero_contrato) REFERENCES dim_energia_contrato(numero_contrato)
                );
            """))
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_energia_medidor (
                numero_medidor INT AUTO_INCREMENT PRIMARY KEY,
                numero_da_instalacao varchar(30),
                numero_contrato INT,
                endereco_de_instalacao VARCHAR(255),
                numero_cliente INT,
                FOREIGN KEY (numero_cliente) REFERENCES dim_energia_cliente(numero_cliente),
                FOREIGN KEY (numero_contrato) REFERENCES dim_energia_contrato(numero_contrato)
                );
            """))
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS fato_energia_consumo (
                fato_energia_id INT AUTO_INCREMENT PRIMARY KEY,
                consumo_em_ponta varchar(20),
                consumo_fora_de_ponta_capacidade DECIMAL(11, 3),
                consumo_fora_de_ponta_industrial DECIMAL(11, 3),
                demanda_de_ponta_kw DECIMAL(11, 3),
                demanda_fora_de_ponta_capacidade DECIMAL(11, 3),
                demanda_fora_de_ponta_industrial DECIMAL(11, 3),
                demanda_faturada_custo DECIMAL(11, 2),
                demanda_faturada_pt_custo DECIMAL(11, 2),
                demanda_faturada_fp_custo DECIMAL(11, 2),
                demanda_ultrapassada_kw DECIMAL(11, 3),
                demanda_ultrapassada_custo DECIMAL(11, 2),
                total_da_fatura DECIMAL(11, 2),
                nivel_de_informacoes_da_fatura varchar(30),
                data_id_leitura_anterior INT,
                data_id_leitura_atual INT,
                data_id_emissao INT,
                data_id_conta_do_mes int,
                data_id_vencimento int,
                numero_cliente INT,
                numero_medidor INT,
                numero_contrato INT,
                FOREIGN KEY (data_id_leitura_anterior) REFERENCES dim_energia_tempo(data_id),
                FOREIGN KEY (data_id_leitura_atual) REFERENCES dim_energia_tempo(data_id),
                FOREIGN KEY (data_id_emissao) REFERENCES dim_energia_tempo(data_id),
                FOREIGN KEY (data_id_conta_do_mes) REFERENCES dim_energia_tempo(data_id),
                FOREIGN KEY (data_id_vencimento) REFERENCES dim_energia_tempo(data_id),
                FOREIGN KEY (numero_cliente) REFERENCES dim_energia_cliente(numero_cliente),
                FOREIGN KEY (numero_medidor) REFERENCES dim_energia_medidor(numero_medidor),
                FOREIGN KEY (numero_contrato) REFERENCES dim_energia_contrato(numero_contrato)
                );
            """))

    except SQLAlchemyError as e:
        print(f"Erro ao configurar o banco de dados: {e}")


def create_view():

    engine = create_engine(banco)

    try:

        with engine.connect() as connection:

            connection.execute(text("""
                CREATE 
                    ALGORITHM = UNDEFINED 
                    DEFINER = 'sql10712676'@'%' 
                    SQL SECURITY DEFINER
                VIEW sql10712676.conta_luz AS
                    SELECT 
                        t.data_full AS Emissao,
                        c.horario_de_ponta AS contrato,
                        c.nome_do_contrato AS cliente,
                        f.consumo_fora_de_ponta_industrial AS consumo_fora_de_ponta_industrial,
                        f.consumo_fora_de_ponta_capacidade AS consumo_fora_de_ponta_capacidade,
                        f.demanda_de_ponta_kw AS demanda_de_ponta_kw,
                        f.demanda_ultrapassada_kw AS demanda_ultrapassada_kw,
                        f.total_da_fatura AS total_da_fatura
                    FROM
                        ((sql10712676.fato_energia_consumo f
                        JOIN sql10712676.dim_energia_contrato c ON ((c.numero_contrato = f.numero_contrato)))
                        JOIN sql10712676.dim_energia_tempo t ON ((f.data_id_emissao = t.data_id)));
                        """))

            print("view energia criada com sucesso.")

            connection.execute(text("""
                CREATE 
                    ALGORITHM = UNDEFINED 
                    DEFINER = 'sql10712676'@'%' 
                    SQL SECURITY DEFINER
                VIEW sql10712676.conta_agua AS
                SELECT 
                    f.consumo_de_agua_m3, 
                    f.consumo_de_esgoto_m3, 
                    f.valor_agua, 
                    f.valor_esgoto, 
                    c.nome_cliente, 
                    t.data_full 
                FROM 
                    sql10712676.fato_agua_consumo AS f
                LEFT JOIN 
                    sql10712676.dim_agua_cliente c
                ON 
                    f.numero_cliente = c.numero_cliente
                LEFT JOIN 
                    sql10712676.dim_tempo t
                ON 
                    f.data_id_emissao = t.data_id;
                """))

            print("view agua criada com sucesso.")



    except SQLAlchemyError as e:
        print(f"Erro ao criar views: {e}")

def processar_dimensoes_agua(csv_file):
    try:

        processador = ProcessamentoDadosDimensaoAgua(csv_file)
        df_tratado = processador.executar_etl()

        tempo = TempoDimensaoAgua(df_tratado)
        tempo.conectar_banco(banco)
        tempo.inserir_banco()



        contrato = Contrato_agua(df_tratado)
        contrato.conectar_banco(banco)
        contrato.inserir_banco()

        cliente_agua = Cliente_Agua(df_tratado)
        cliente_agua.conectar_banco(banco)
        cliente_agua.inserir_banco()



        medidor = Medidor_agua(df_tratado)
        medidor.conectar_banco(banco)
        medidor.inserir_banco()


    except Exception as e:
        print(f'Erro ao processar arquivo de água {csv_file}: {e}')


def processar_fato_agua(csv_file):
    try:

        processador = ProcessamentoDadosFatoAgua(csv_file, banco)
        df_tratado = processador.executar_etl()


        tempo = TempoFatoAgua(df_tratado)
        tempo.conectar_banco(banco)
        tempo.inserir_banco()

        agua = FatoAgua(df_tratado)
        agua.conectar_banco(banco)
        agua.inserir_banco()



    except Exception as e:
        print(f'Erro ao processar arquivo de contrato de água {csv_file}: {e}')

# funcao da ala energia
def processar_dimensoes_energia(csv_file):
    try:
        processador = ProcessamentoDadosDimensaoEnergia(csv_file)
        df_tratado = processador.executar_etl()

        tempo = TempoDimensaoEnergia(df_tratado)
        tempo.conectar_banco(banco)
        tempo.inserir_banco()


        contrato = Contrato_energia(df_tratado)
        contrato.conectar_banco(banco)
        contrato.inserir_banco()


        cliente_energia = Cliente_Energia(df_tratado)
        cliente_energia.conectar_banco(banco)
        cliente_energia.inserir_banco()


        medidor = Medidor_energia(df_tratado)
        medidor.conectar_banco(banco)
        medidor.inserir_banco()


    except Exception as e:
        print(f'Erro ao processar arquivo de energia {csv_file}: {e}')


def processar_fato_energia(csv_file):
    try:
        processador = ProcessamentoDadosFatoEnergia(csv_file, banco)
        df_tratado = processador.executar_etl()


        tempo = TempoFatoEnergia(df_tratado)
        tempo.conectar_banco(banco)
        tempo.inserir_banco()


        energia = FatoEnergia(df_tratado)
        energia.conectar_banco(banco)
        energia.inserir_banco()


    except Exception as e:
        print(f'Erro ao processar arquivo de contrato de energia {csv_file}: {e}')


def main(folder_path):
    csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
    setup_database()
    if not csv_files:
        print('Nenhum arquivo CSV encontrado')
        return

    for csv_file in csv_files:
        nome = os.path.basename(csv_file).lower()
        if 'con_agua' in nome:
            print("con_agua")
            processar_dimensoes_agua(csv_file)
        elif 'con_energia' in nome:
            print("con_energia")
            processar_dimensoes_energia(csv_file)
        elif 'pro_agua' in nome:
            print("pro_agua")
            processar_fato_agua(csv_file)
        elif 'pro_energia' in nome:
            print("pro_energia")
            processar_fato_energia(csv_file)
        else:
            print(f'Arquivo não reconhecido: {csv_file}')

    create_view()



if __name__ == '__main__':
    folder_path = '../data/raw'
    main(folder_path)