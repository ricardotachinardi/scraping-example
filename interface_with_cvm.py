"""
Realiza a comunicação com a cvm
"""

from datetime import datetime
from typing import List, Union
import json
import requests
import concurrent.futures
from retry import retry

headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
    }

@retry(Exception, total_tries=5)
def get_initial_data(ano: int = datetime.today().year) -> dict:
    """Obtém os dados iniciais

    Args:
        ano (int, optional): Ano dos dados a serem requisitados. Defaults to datetime.today().year.

    Returns:
        dict: dados iniciais correspondentes a uma única emissão
    """

    url = "http://web.cvm.gov.br/app/esforcosrestritos/consultarOfertaController/consultarValorMobiliario"
 
    payload = {
    "ano": str(ano),
    "comunicado": "1",
    "situacao": "1",
    "situacaoAndamento": True,
    "situacaoEncerrada": False,
    "tipoComunicado": "INICIAL",
    "valorMobiliarioId": "15",
    }
 
    response = requests.post(url=url, 
                        json=payload, 
                        headers=headers)
    
    return json.loads(response.content)


@retry(Exception, total_tries=5)
def get_additional_data(id_oferta: str) -> dict:
    """Obtém alguns dados não presentes na request inicial

    Args:
        id_oferta (str): id obtido na request inicial data

    Returns:
        dict: dados adicionais correspondentes a uma única emissão
    """

    url = "http://web.cvm.gov.br/app/esforcosrestritos/enviarFormularioParcial/getOfertaPorId/" + str(id_oferta)
    
    response = requests.get(url=url,
                             headers=headers)
    
    return json.loads(response.content)


@retry(Exception, total_tries=5)
def get_nome_lider(cnpj_lider: Union[int,str]) -> dict:
    """Obtém o nome do lider a partir do cnpj

    Args:
        cnpj_lider (Union[int,str]): cnpj

    Returns:
        dict: {"lider_nome": nome, "lider_nrPfPj": cnpj_lider}
    """
    
    payload = {"cnpj": cnpj_lider}
    
    url = "http://web.cvm.gov.br/app/esforcosrestritos/responsavel/getNomeResponsavelPorCnpj"
    
    response = requests.post(url=url,
                            json=payload,
                            headers=headers)
    
    nome = json.loads(response.content).strip()
    return {"lider_nome": nome, "lider_nrPfPj": cnpj_lider}

# versão paralelizada (bem mais rápido, mas poderia ficar melhor com
# outro approach, como alguma lib especializada em requests em paralelo)
def execute_requests_from_list(my_request: Union[get_additional_data,get_nome_lider], 
                               list_of_parameters: list) -> List[dict]:
    """Executa múltiplas vezes uma das requests, em paralelo

    Args:
        my_request (Union[get_additional_data,get_nome_lider]): request a ser executada
        list_of_parameters (list): lista cujos valores serão usados nas requests (1 por request) 

    Returns:
        List[dict]: lista com o resultado das requests
    """

    list_of_dicts: List[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            
            tasks = [executor.submit(my_request, parameter) for parameter in list_of_parameters]
            for _ in concurrent.futures.as_completed(tasks):
                list_of_dicts.append(_.result())
                
    return list_of_dicts

