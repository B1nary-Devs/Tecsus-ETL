import pandas as pd
import os
import glob
from scripts.Contrato_Agua.index import *
from scripts.Conta_agua.index import *

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

banco = 'mysql+pymysql://b1nary:tecsus@localhost:3306/tecsusDB'
banco_sem = 'mysql+pymysql://b1nary:tecsus@localhost:3306/'
def setup_database():

    engine = create_engine(banco_sem)

    try:

        with engine.connect() as connection:

            connection.execute(text("DROP DATABASE IF EXISTS contas;"))

            connection.execute(text("CREATE DATABASE contas;"))
            connection.execute(text("USE contas;"))


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
            print("Banco de dados e tabelas criados com sucesso.")

    except SQLAlchemyError as e:
        print(f"Erro ao configurar o banco de dados: {e}")

def processar_dimensoes_agua(csv_file):
    try:

        processador = ProcessamentoDadosDimensao(csv_file)
        df_tratado = processador.executar_etl()

        tempo = TempoDimensao(df_tratado)
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

        processador = ProcessamentoDadosFato(csv_file, banco)
        df_tratado = processador.executar_etl()

        tempo = TempoFato(df_tratado)
        tempo.conectar_banco(banco)
        tempo.inserir_banco()

        agua = FatoAgua(df_tratado)
        agua.conectar_banco(banco)
        agua.inserir_banco()

    except Exception as e:
        print(f'Erro ao processar arquivo de contrato de água {csv_file}: {e}')


def main(folder_path):
    csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
    setup_database()
    if not csv_files:
        print('Nenhum arquivo CSV encontrado')
        return

    for csv_file in csv_files:
        nome = os.path.basename(csv_file).lower()
        if 'pro_agua' in nome:
            processar_fato_agua(csv_file)
        elif 'con_agua' in nome:
            processar_dimensoes_agua(csv_file)

        elif 'con_energia' in nome:
            print('Processando contrato de energia para:', csv_file)
        else:
            print(f'Arquivo não reconhecido: {csv_file}')

if __name__ == '__main__':
    folder_path = '../data/raw'
    main(folder_path)
