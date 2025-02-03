from azure_storage import realizar_upload_azure
from s3_storage import realizar_upload_s3
import logging
import concurrent.futures

def enviar_resultados(temp_dir, nome_consulta, portal, destino_tipo, destino_config,workers):
    """
    Envia os resultados para os destinos configurados.

    Args:
        temp_dir (str): Caminho do diretório temporário contendo os arquivos Parquet.
        nome_consulta (str): Nome da consulta.
        portal (str): Caminho base no destino.
        destino_tipo (str): Tipo de destino ("azure", "s3" ou "ambos").
        destino_config (dict): Configurações específicas do(s) destino(s).
    """
    caminho_destino = f"{portal}/{nome_consulta}"
    logging.info(f"Iniciando envio da consulta '{nome_consulta}' para '{destino_tipo}'.")

    # Criar tarefas para upload em paralelo
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        if destino_tipo in ["azure", "ambos"]:
            futures.append(executor.submit(enviar_para_azure, workers,temp_dir, caminho_destino, destino_config.get("azure", {})))
        if destino_tipo in ["s3", "ambos"]:
            futures.append(executor.submit(enviar_para_s3, workers,temp_dir, caminho_destino, destino_config.get("s3", {})))

        # Aguardar todas as tarefas terminarem
        for future in concurrent.futures.as_completed(futures):
            future.result()

    logging.info(f"Envio de '{nome_consulta}' para '{destino_tipo}' concluído com sucesso.")
    return True


def validar_config_azure(azure_config):
    """Valida e inicializa a configuração do Azure se necessário."""
    if "blob_service_client" not in azure_config:
        if "account_name" in azure_config and "account_key" in azure_config:
            from azure.storage.blob import BlobServiceClient
            azure_config["blob_service_client"] = BlobServiceClient(
                account_url=f"https://{azure_config['account_name']}.blob.core.windows.net",
                credential=azure_config["account_key"]
            )
            logging.info("Conexão com o Azure inicializada com sucesso.")
        else:
            raise ValueError("Configuração do Azure está incompleta. Certifique-se de fornecer 'account_name' e 'account_key'.")
    return azure_config


def validar_config_s3(s3_config):
    """Valida e inicializa a configuração do S3 se necessário."""
    if "s3_client" not in s3_config:
        import boto3
        s3_config["s3_client"] = boto3.client(
            "s3",
            aws_access_key_id=s3_config.get("access_key"),
            aws_secret_access_key=s3_config.get("secret_key"),
            region_name=s3_config.get("region"),
        )
        logging.info("Conexão com o S3 inicializada com sucesso.")
    return s3_config


def enviar_para_azure(workers,temp_dir, caminho_destino, azure_config):
    """Executa o envio para o Azure."""
    try:
        logging.info(f"Iniciando envio para Azure: {caminho_destino}")
        azure_config = validar_config_azure(azure_config)
        realizar_upload_azure(temp_dir, caminho_destino, azure_config, workers=workers)
        logging.info("Envio para Azure concluído.")
    except Exception as e:
        logging.error(f"Erro ao enviar '{caminho_destino}' para Azure: {e}")


def enviar_para_s3(workers,temp_dir, caminho_destino, s3_config):
    """Executa o envio para o S3."""
    try:
        logging.info(f"Iniciando envio para S3: {caminho_destino}")
        s3_config = validar_config_s3(s3_config)
        realizar_upload_s3(temp_dir, caminho_destino, s3_config,workers=workers)
        logging.info("Envio para S3 concluído.")
    except Exception as e:
        logging.error(f"Erro ao enviar '{caminho_destino}' para S3: {e}")
