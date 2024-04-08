import pandas as pd
import os
import glob
import sys 
from pathlib import Path
file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
sys.path.append(str(root))
from scripts.Conta_agua.index import Conta_agua
from scripts.Contrato_Agua.index import Contrato_agua

#caminho para leitura dos arquivos
folder_path = 'data\\raw'

#lista de todos os arquivos csv 
csv_files = glob.glob(os.path.join(folder_path, '*.csv'))

if not csv_files:
    print('Arquivo não encontrado')
else:
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file, low_memory=False, delimiter=',', encoding='UTF-8')
            nome = os.path.basename(csv_file)

            #Separa os arquivos de acordo com o nome e manda ele para as funçoes de tratamento
            if 'pro_agua' in nome.lower():
                contaAgua = Conta_agua(df)
                contaAgua.leitura_conta_agua()
            elif 'con_agua' in nome.lower():
                contratoAgua = Contrato_agua(df)
                contratoAgua.leitura_conta_agua()
            elif 'pro_energia' in nome.lower():
                print('sou conta energia')
            elif 'con_energia' in nome.lower():
                print('sou contrato energia')
            else:
                print('não sou nada')

        except Exception as e:
            print(f'Erro ao ler o arquivo {csv_file}: {e}')