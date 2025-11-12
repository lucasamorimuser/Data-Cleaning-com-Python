import pandas as pd

# IMPORTAÇÃO DOS DADOS

vendas = pd.read_csv('caminho_arquivo_bruto')
itens_vendidos = pd.read_csv('caminho_arquivo_bruto')
produtos = pd.read_csv('caminho_arquivo_bruto')
transacoes = pd.read_csv('caminho_arquivo_bruto')
clientes = pd.read_json('caminho_arquivo_bruto')

# EXPLORAÇÃO DOS DADOS

lista_arquivos = [vendas, itens_vendidos, produtos, transacoes, clientes]
nomes_arquivos = ['vendas', 'itens_vendidos', 'produtos', 'transacoes','clientes']

for nome, arquivo in zip(nomes_arquivos, lista_arquivos):
    print(f' === {nome} Head ===')
    print(arquivo.head())
    print('\n' + '--' *40 + '\n')
    print(f' === {nome} Describe ===')
    print(arquivo.describe())
    print(f' === {nome} Info ===')
    print(arquivo.info())


# -------------------------------------- FUNÇÕES --------------------------------------

# REMOVE DUPLICADOS

def remove_duplicados(df: pd.DataFrame, colunas_duplicadas: list) -> pd.DataFrame:

    duplicados_antes = df[colunas_duplicadas].duplicated().sum()

    if duplicados_antes > 0:
        df = df.drop_duplicates(subset=colunas_duplicadas, keep='first')
        duplicados_depois = df[colunas_duplicadas].duplicated().sum()
        duplicados_removidos = duplicados_antes - duplicados_depois
        print(f"Foram removidas {duplicados_removidos} linhas com valores duplicados nas colunas duplicadas")
    else:
        print(f'Nenhum valor duplicado foi encontrado nas colunas duplicadas')
    
    return df

# FORMATA DATAS

def formata_datas(df = pd.DataFrame, colunas_data = list):

    for coluna in colunas_data:
        df[coluna] = df[coluna].str.replace(r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})', r'\3-\2-\1', regex=True)
        df[coluna] = pd.to_datetime(df[coluna], errors='coerce')
        datas_formatadas = df[coluna].count()
        datas_NaT = df[coluna].isna().sum()
    print(f'Foram formatadas {datas_formatadas} linhas de data')
    print(f'{datas_NaT} datas não foram corrigidas')

    return df

# TRATAMENTO NEGATIVOS

def remove_negativos(df: pd.DataFrame, colunas_numericas: list = None):

    for coluna in colunas_numericas:
        negativos_antes = (df[coluna] < 0).sum()
        if negativos_antes > 0:
            df[coluna] = df[coluna].abs()
            negativos_depois = (df[coluna] < 0).sum()
            negativos_removidos = negativos_antes - negativos_depois
            print(f'Foram transformados {negativos_removidos} valores negativos nas colunas numéricas')
        else:
            print('Nenhum valor negativo foi encontrado em colunas numéricas')
    return df

# REMOVE NULOS

def remove_nulos(df: pd.DataFrame, colunas_nulos: list) -> pd.DataFrame:

    nulos_antes = df[colunas_nulos].isnull().any(axis = 1).sum()

    if nulos_antes > 0:
        df = df.dropna(subset = colunas_nulos)
        nulos_depois = df[colunas_nulos].isnull().any(axis = 1).sum()
        nulos_removidos = nulos_antes - nulos_depois
        print(f"Foram removidos {nulos_removidos} valores nulos de coluna nulos")
    else:
        print(f'Nenhum valor nulo encontrado nas colunas chave')
    
    return df

# REMOVE VAZIOS

def remove_vazios(df: pd.DataFrame, colunas_vazios: list) -> pd.DataFrame:
    return df[~df[colunas_vazios].isin([None, '']).any(axis=1)]

# -------------------------------------- EXECUÇÃO DAS FUNÇÕES NOS AQUIVOS --------------------------------------

vendas.info()

vendas = remove_duplicados(df = vendas, colunas_duplicadas = ['id_venda'])
vendas = formata_datas(df = vendas, colunas_data = ['data_venda'])
vendas = remove_negativos(df = vendas, colunas_numericas = ['valor_total'])
vendas = remove_nulos(df = vendas, colunas_nulos = vendas.columns)
vendas = remove_vazios(df = vendas, colunas_vazios = ['id_venda','data_venda','valor_total'])

vendas.to_csv('caminho_para_salvar_arquivo_tratado', index=False)

# ---------------------------------------------------------------------------------------------------------------------------

itens_vendidos.info()

itens_vendidos = remove_duplicados(df = itens_vendidos, colunas_duplicadas = ['id_venda'])
itens_vendidos = remove_negativos(df = itens_vendidos, colunas_numericas = ['quantidade'])
itens_vendidos = remove_nulos(df = itens_vendidos, colunas_nulos = itens_vendidos.columns)
itens_vendidos = remove_vazios(df = itens_vendidos, colunas_vazios = itens_vendidos.columns)

itens_vendidos.to_csv('caminho_para_salvar_arquivo_tratado', index=False)

# ---------------------------------------------------------------------------------------------------------------------------

produtos.info()

produtos = remove_duplicados(df = produtos, colunas_duplicadas = ['id_produto', 'nome_produto'])
produtos = remove_negativos(df = produtos, colunas_numericas = ['preco', 'custo'])
produtos = remove_nulos(df = produtos, colunas_nulos = produtos.columns)
produtos = remove_vazios(df = produtos, colunas_vazios = produtos.columns)

produtos.to_csv('caminho_para_salvar_arquivo_tratado', index=False)

# ---------------------------------------------------------------------------------------------------------------------------

transacoes.info()

transacoes = remove_duplicados(df = transacoes, colunas_duplicadas = ['id_venda'])
transacoes = remove_negativos(df = transacoes, colunas_numericas = ['parcelas'])
transacoes = remove_nulos(df = transacoes, colunas_nulos = ['id_venda','forma_de_pagamento'])
transacoes = remove_vazios(df = transacoes, colunas_vazios = ['id_venda','forma_de_pagamento'])

transacoes.to_csv('caminho_para_salvar_arquivo_tratado', index=False)

# ---------------------------------------------------------------------------------------------------------------------------

clientes.info()

clientes = remove_duplicados(df = clientes, colunas_duplicadas = ['id_cliente', 'nome_cliente', 'email'])
clientes = remove_nulos(df = clientes, colunas_nulos = clientes.columns)
clientes = remove_vazios(df = clientes, colunas_vazios = ['cidade','estado'])

clientes.to_csv('caminho_para_salvar_arquivo_tratado', index=False)