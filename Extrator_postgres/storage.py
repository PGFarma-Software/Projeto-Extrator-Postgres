import asyncio
import os

from azure_storage import realizar_upload_azure
from s3_storage import realizar_upload_s3
import logging
import concurrent.futures

def enviar_resultados(temp_dir, portal, destino_tipo, destino_config, workers,nome_consulta):
    """
    Envia os resultados para os destinos configurados.

    Args:
        temp_dir (str): Caminho do diretório temporário contendo os arquivos Parquet.
        nome_consulta (str): Nome da consulta.
        portal (str): Caminho base no destino.
        destino_tipo (str): Tipo de destino ("azure", "s3" ou "ambos").
        destino_config (dict): Configurações específicas do(s) destino(s).
        workers (int): Número de threads para paralelismo.

    Returns:
        bool: `True` se o envio foi bem-sucedido para todos os destinos, `False` caso contrário.
    """
    caminho_destino = f"{portal}/{nome_consulta}"
    logging.info(f"Iniciando envio da consulta '{nome_consulta}' para '{destino_tipo}'.")

    # Criar tarefas para upload em paralelo
    futures = []
    success_azure, success_s3 = None, None

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        if destino_tipo in ["azure", "ambos"]:
            futures.append(executor.submit(enviar_para_azure, workers, temp_dir, caminho_destino, destino_config.get("azure", {}), nome_consulta))
        if destino_tipo in ["s3", "ambos"]:
            futures.append(executor.submit(enviar_para_s3, workers, temp_dir, caminho_destino, destino_config.get("s3", {}), nome_consulta))

        # Processar os resultados
        for future in concurrent.futures.as_completed(futures):
            try:
                resultado_envio = future.result()  # ✅ Agora captura corretamente o retorno
                if destino_tipo in ["azure", "ambos"]:
                    success_azure = resultado_envio
                if destino_tipo in ["s3", "ambos"]:
                    success_s3 = resultado_envio
            except Exception as e:
                logging.error(f"Erro ao enviar '{nome_consulta}' para '{destino_tipo}': {e}")
                if destino_tipo in ["azure", "ambos"]:
                    success_azure = False
                if destino_tipo in ["s3", "ambos"]:
                    success_s3 = False

    # Avaliação final do sucesso do envio
    if destino_tipo == "azure":
        sucesso = success_azure
    elif destino_tipo == "s3":
        sucesso = success_s3
    else:  # Ambos
        sucesso = (success_azure is True) and (success_s3 is True)  # Somente retorna sucesso se ambos forem True

    if sucesso:
        logging.info(f"Todos os arquivos de '{caminho_destino}' foram enviados com sucesso.")
    else:
        logging.error(f"Falha no envio de '{nome_consulta}' para '{destino_tipo}'.")

    return sucesso


def enviar_para_azure(workers, temp_dir, caminho_destino, azure_config, nome_consulta):
    """Executa o envio para o Azure."""
    try:
        # 🔹 Executa a função assíncrona e captura o retorno corretamente
        resultado_upload = realizar_upload_azure(temp_dir, caminho_destino, azure_config, workers=workers, nome_consulta=nome_consulta)

        # 🔹 Verifica se houve erro no envio
        if resultado_upload["erros"]:
            logging.error(f"Falha no upload de alguns arquivos para Azure: {resultado_upload['erros']}")
            return False  # Indica falha no envio
        else:
            logging.info(f"Todos os arquivos de '{caminho_destino}' enviados para Azure com sucesso.")
            return True  # Indica sucesso no envio

    except Exception as e:
        logging.error(f"Erro ao enviar '{caminho_destino}' para Azure: {e}")
        return False  # Indica falha no envio


def enviar_para_s3(workers, temp_dir, caminho_destino, s3_config, nome_consulta):
    """Executa o envio para o S3 e retorna o status."""
    try:
        # ✅ Captura corretamente o retorno de `realizar_upload_s3`
        resultado_upload = realizar_upload_s3(temp_dir, caminho_destino, s3_config, workers=workers, nome_consulta=nome_consulta)

        # ✅ Se houver erro no envio, retorna False
        if resultado_upload["erros"]:
            logging.error(f"Falha no upload de alguns arquivos para S3: {resultado_upload['erros']}")
            return False  # Indica falha no envio
        else:
            logging.info(f"Todos os arquivos de '{caminho_destino}' enviados para S3 com sucesso.")
            return True  # Indica sucesso no envio

    except Exception as e:
        logging.error(f"Erro ao enviar '{caminho_destino}' para S3: {e}")
        return False  # Indica falha no envio
