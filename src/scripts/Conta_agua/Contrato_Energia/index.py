import pandas as pd
from sqlalchemy import create_engine


class Contrato_energia:
    def __init__(self, caminho_arquivo):
        self.caminho_arquivo = caminho_arquivo
        self.dataframe = None
        self.engine = None

    def leitura_conta_energia(self):
        try:
            # Lendo o arquivo CSV para o DataFrame
            self.dataframe = pd.read_csv(self.caminho_arquivo, delimiter=',', escapechar='\\')

            # Renomeando colunas (ajuste os nomes de acordo com seu arquivo)
            self.dataframe.rename(columns={
                'Planta': 'planta',
                'Classe': 'classe',
                'Nome do Contrato': 'nome_do_contrato',
                'Fornecedor': 'fornecedor',
                'HorÃ¡rio de Ponta': 'horario_de_ponta',
                'CÃ³digo de IdentificaÃ§Ã£o': 'codigo_de_identificacao',
                'CÃ³digo Fiscal de OperaÃ§Ã£o': 'codigo_fiscal_de_operacao',
                'Roteiro de Leitura': 'roteiro_de_leitura',
                'Tipo de Consumidor (Energia)': 'tipo_de_consumidor_energia',
                'NÃºmero InstalaÃ§Ã£o': 'numero_instalacao',
                'NÃºmero Medidor': 'numero_medidor',
                'NÃºmero Cliente': 'numero_cliente',
                'Modalidade': 'modalidade',
                'Demanda Ponta': 'demanda_ponta',
                'Demanda Fora Ponta': 'demanda_fora_ponta',
                'TensÃ£o Contratada (V)': 'tensao_contratada',
                'Acesso as Contas': 'acesso_as_contas',
                'CÃ³digo de LigaÃ§Ã£o (RGI)': 'codigo_de_ligacao_rgi',
                'HidrÃ´metro': 'hidrometro',
                'NÃºmero Contrato': 'numero_contrato',
                'CodificaÃ§Ã£o da Companhia': 'codificacao_da_companhia',
                'Identificador de UsuÃ¡rio': 'identificador_de_usuario',
                'ID EletrÃ´nico': 'id_eletronico',
                'Setor': 'setor',
                'CÃ³digo da Rua': 'codigo_da_rua',
                'InscriÃ§Ã£o Cadastral do ImÃ³vel': 'inscricao_cadastral_do_imove',
                'CÃ³digo de Consumidor': 'codigo_de_consumidor',
                'Campo Extra 2': 'campo_extra_2',
                'Campo Extra 3': 'campo_extra_3',
                'Campo Extra 4': 'campo_extra_4',
                'Unidade MÃ©trica': 'unidade_metrica',
                'Forma de Pagamento': 'forma_de_pagamento',
                'Site de Acesso': 'site_de_acesso',
                'Tipo de Acesso a Distribuidora': 'tipo_de_acesso_distribuidora',
                'Campo Extra de Acesso 1': 'campo_extra_de_acesso_1',
                'Senha': 'senha',
                'EndereÃ§o de InstalaÃ§Ã£o': 'endereco_de_instalacao',
                'VigÃªncia Inicial': 'vigencia_inicial',
                'VigÃªncia Final': 'vigencia_final',
                'ObservaÃ§Ã£o': 'observacao',
                'End. Divergente': 'end_divergente',
                'Ativado': 'ativado',
                'Loja Nova': 'loja_nova'
            }, inplace=True)

        except Exception as e:
            print(f'Erro ao ler o arquivo {self.caminho_arquivo}: {e}')

    def conectar_banco(self):
        # Ajuste as credenciais de conexão ao seu banco de dados
        try:
            self.engine = create_engine('mysql+pymysql://usuario:senha@localhost/banco_de_dados')
            print("Conexão com o banco de dados estabelecida com sucesso.")

