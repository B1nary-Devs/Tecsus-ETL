import pandas as pd
import os
import glob
from scripts.Contrato_Agua.index import Contrato_agua
from scripts.Conta_agua.index import Conta_agua
from scripts.Contrato_Energia.index import Contrato_energia  
from scripts.Conta_Energia.index import Conta_energia  




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



# funcao da ala energia

def processar_conta_energia(csv_file):  # processar 
    try:
        conta_energia = Conta_energia(csv_file)  # objeto 
        conta_energia.leitura_conta_energia()  # Leia os dados
        conta_energia.transformar_data(['Data Vencimento', 'Data Emissao', 'Leitura Anterior', 'Leitura Atual'])  # Transforme as colunas
        conta_energia.transformar_valores(['Valor TUSD', 'Valor FP', 'Consumo PT', 'Consumo FP', 'Consumo TE'])  # Transforme os valores
        conta_energia.conectar_banco() 
        conta_energia.inserir_banco('conta_energia')  # Insira os dados na tabela conta_energia
        conta_energia.gerar_relatorio('Arquivos_inseridos_energia.csv', ['Data Vencimento', 'Data Emissao', 'Leitura Anterior', 'Leitura Atual', 'Consumo PT'])  
        print(f'Dados de energia processados e inseridos para: {csv_file}')
    except Exception as e:
        print(f'Erro ao processar arquivo de energia {csv_file}: {e}')


def processar_contrato_energia(csv_file): 
    try:
        contrato_energia = Contrato_energia(csv_file)  
        contrato_energia.leitura_contrato_energia()  
        contrato_energia.conectar_banco()  
        contrato_energia.inserir_banco('contrato_energia')  
        contrato_energia.gerar_relatorio('Arquivos_inseridos_contrato_energia.csv')  
        print(f'Dados de contrato de energia processados e inseridos para: {csv_file}')
    except Exception as e:
        print(f'Erro ao processar arquivo de contrato de energia {csv_file}: {e}')









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
