import pandas as pd
import os
import glob
from scripts.Contrato_Agua.index import Contrato_agua
from scripts.Conta_agua.index import Conta_agua


def processar_conta_agua(csv_file):
    try:
        conta_agua = Conta_agua(csv_file)
        conta_agua.leitura_conta_agua()
        conta_agua.transformar_data(
            ['contadomes', 'vencimento', 'emissao', 'leituraanterior', 'leituraatual']
        )
        conta_agua.transformar_valores([
            'valoraguar$', 'valoresgotor$', 'totalr$', 'consumodeaguam3', 'consumodeesgotom3'
        ])
        conta_agua.conectar_banco()
        conta_agua.inserir_banco('conta_agua')
        conta_agua.gerar_relatorio('Arquivos_inseridos.csv', ['contadomes', 'vencimento', 'emissao', 'leituraanterior', 'leituraatual'])
        print(f'Dados de água processados e inseridos para: {csv_file}')
    except Exception as e:
        print(f'Erro ao processar arquivo de água {csv_file}: {e}')

def processar_contrato_agua(csv_file):
    try:
        contrato_agua = Contrato_agua(csv_file)
        contrato_agua.leitura_conta_agua()
        contrato_agua.conectar_banco()
        contrato_agua.inserir_banco('contrato_agua')
        contrato_agua.gerar_relatorio('Arquivos_inseridos_contrato.csv')
        print(f'Dados de contrato de água processados e inseridos para: {csv_file}')
    except Exception as e:
        print(f'Erro ao processar arquivo de contrato de água {csv_file}: {e}')

def main(folder_path):
    csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
    if not csv_files:
        print('Nenhum arquivo CSV encontrado')
        return

    for csv_file in csv_files:
        nome = os.path.basename(csv_file).lower()
        if 'pro_agua' in nome:
            processar_conta_agua(csv_file)
        elif 'con_agua' in nome:
            processar_contrato_agua(csv_file)
        elif 'pro_energia' in nome:
            print('Processando conta de energia para:', csv_file)
        elif 'con_energia' in nome:
            print('Processando contrato de energia para:', csv_file)
        else:
            print(f'Arquivo não reconhecido: {csv_file}')

if __name__ == '__main__':
    folder_path = '../data/raw'
    main(folder_path)
