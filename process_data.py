"""
Processa os dados da cvm, podendo retornar um 
df mais completo ou um mais simples e formatado
"""
import pandas as pd
import interface_with_cvm as cvm
from typing import List


def process_initial_data() -> pd.DataFrame:
    """Processa os dados da primeira request à cvm

    Returns:
        pd.DataFrame: df com os dados dessa request
    """
    dict_response = cvm.get_initial_data()
    
    df_inicial = pd.DataFrame.from_dict(dict_response)
    
    list_of_dfs: List[pd.DataFrame] = []
    
    for i in range(len(df_inicial)):
        list_of_dfs.append(pd.json_normalize(df_inicial['ofertasComunicadosTO'][i], sep='_'))
 
    df_agregado = pd.concat(list_of_dfs, join="inner", axis=0)
 
    df_agregado = df_agregado.reset_index(drop=True)
    
    return df_agregado


def process_additional_data(list_of_ids: List[str]) -> pd.DataFrame:
    """Processa os dados da segunda request à cvm 
    (além de fazer umma request extra para obter o nome do líder)

    Args:
        list_of_ids (List[str]): lista com os ids obtidos na primeira request

    Returns:
        pd.DataFrame: df com os dados dessa request
    """
    
    list_of_dfs: List[pd.DataFrame] = []
    
    list_of_dicts = cvm.execute_requests_from_list(my_request=cvm.get_additional_data,
                                                   list_of_parameters=list_of_ids)
    
    for dict_response in list_of_dicts:
        list_of_dfs.append(pd.json_normalize(dict_response, sep='_'))   
    
    df_agregado = pd.concat(list_of_dfs)
        
    # pegando o nome dos lideres
    list_of_cnpjs = list(df_agregado['lider_nrPfPj'])
    list_of_dicts2 = cvm.execute_requests_from_list(my_request=cvm.get_nome_lider,
                                                   list_of_parameters=list_of_cnpjs)    
    df_nomes_lideres  = pd.DataFrame.from_dict(list_of_dicts2).drop_duplicates()
    
    df_agregado = df_agregado.merge(df_nomes_lideres, left_on="lider_nrPfPj", right_on="lider_nrPfPj")
    
    df_agregado = df_agregado.reset_index(drop=True)
    
    return df_agregado


def get_complete_dataframe() -> pd.DataFrame:
    """Une os dados obtidos nas duas requests à cvm

    Returns:
        pd.DataFrame: df com todos os dados
    """
    
    df_inicial = process_initial_data()
    
    list_of_ids: List[str] = list(df_inicial['ofertaTO_id'].astype(str))
    
    df_adicional = process_additional_data(list_of_ids=list_of_ids)
    
    df_final = df_adicional.merge(df_inicial, right_on='ofertaTO_id', left_on="id", validate="one_to_one")
    
    return df_final


def get_formatted_dataframe() -> pd.DataFrame:
    """Formata um subset do df completo

    Returns:
        pd.DataFrame: df formatado
    """
    df = get_complete_dataframe()
    
    df = df[["emissor_nomeResponsavel", "numeroEmissao", "quantidadeSerie", "dataInicio", "lider_nome"]]
    
    df["dataInicio"] = pd.to_datetime(df["dataInicio"])
    
    # pega só os 5 dias mais recentes
    df = df[df['dataInicio'].dt.date > (pd.Timestamp('now').floor('D') + pd.Timedelta(-5, unit='D'))]
    
    df["dataInicio"] = df["dataInicio"].apply(lambda x: x.strftime("%Y-%m-%d"))
    
    df["quantidadeSerie"] = df["quantidadeSerie"].fillna("").apply(lambda x: str(int(x)) if type(x) is not str else x)
    
    df.columns = ["Emissor", "Numero da Emissao", "Numero da Serie", "Data de Inicio", "Lider"]
    
    return df    
