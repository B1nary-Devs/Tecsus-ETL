
class Contrato_agua:

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def leitura_conta_agua(self):
        try:
            #Colocando na variavel df o arquivo recebido para tratamento
            df = self.dataframe
            print(df.info())

        except Exception as e:
            print(f'Erro ao ler o arquivo {self.dataframe}: {e}')