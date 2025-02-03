import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import STORAGE_CONFIG


def limpar_prefixo_no_azure(blob_service_client, container_name, prefixo, workers=5):
    """
    Remove todos os blobs em um prefixo específico no Azure Blob Storage de forma paralela.

    Args:
        blob_service_client: Instância do cliente de serviço do Azure Blob.
        container_name (str): Nome do container no Azure.
        prefixo (str): Prefixo a ser limpo.
        workers (int): Número de threads paralelas para exclusão.
    """
    try:
        prefixo = f"{prefixo.rstrip('/')}/idEmpresa={STORAGE_CONFIG['idemp']}/"
        logging.info(
            f"Iniciando limpeza do prefixo '{prefixo}' no container '{container_name}' com {workers} threads...")

        container_client = blob_service_client.get_container_client(container_name)
        blobs = list(container_client.list_blobs(name_starts_with=prefixo))

        if not blobs:
            logging.info(f"Nenhum blob encontrado para exclusão em '{prefixo}'.")
            return

        total_excluidos, total_erros = 0, 0
        erros = []

        def excluir_blob(blob_name):
            """Função auxiliar para exclusão de um único blob."""
            try:
                container_client.delete_blob(blob_name)
                return blob_name, None  # Sucesso
            except Exception as e:
                if "DirectoryIsNotEmpty" not in str(e):
                    return blob_name, str(e)  # Erro

        # Criar um `ThreadPoolExecutor` para paralelizar a exclusão dos blobs
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_blob = {executor.submit(excluir_blob, blob.name): blob.name for blob in blobs}

            for future in as_completed(future_to_blob):
                blob_name, erro = future.result()
                if erro:
                    erros.append(blob_name)
                    total_erros += 1
                    logging.error(f"Erro ao excluir blob '{blob_name}': {erro}")
                else:
                    total_excluidos += 1

        # Log final consolidado
        logging.info(f"Limpeza concluída. Blobs removidos: {total_excluidos}, Erros: {total_erros}")

        if erros:
            logging.warning(f"Blobs que falharam na exclusão: {erros}")

    except Exception as e:
        logging.error(f"Erro ao limpar prefixo no Azure Blob Storage: {e}")
        raise


def realizar_upload_azure(temp_dir: str, caminho_destino: str, azure_config: dict, workers: int = 5) -> dict:
    """
    Realiza o upload de arquivos do diretório temporário para o Azure Blob Storage de forma paralela.

    Args:
        temp_dir (str): Diretório temporário contendo os arquivos.
        caminho_destino (str): Caminho no Azure Blob Storage.
        azure_config (dict): Configurações do Azure (blob_service_client, container_name).
        workers (int): Número máximo de threads para paralelismo.

    Returns:
        dict: Dicionário com listas de arquivos enviados e arquivos com erro.
    """
    # Reduzir logs desnecessários de HTTP
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    def upload_arquivo_azure(blob_service_client, container_name, local_path, destino_path):
        """Realiza o upload de um único arquivo para o Azure."""
        try:
            with open(local_path, "rb") as data:
                blob_client = blob_service_client.get_blob_client(container_name, destino_path)
                blob_client.upload_blob(data, overwrite=True)
            return destino_path, None  # Sucesso
        except Exception as e:
            return destino_path, str(e)  # Erro

    try:
        logging.info(f"Iniciando upload para Azure: '{caminho_destino}' com {workers} threads...")

        blob_service_client = azure_config["blob_service_client"]
        container_name = azure_config["container_name"]

        # Obter lista de arquivos para upload
        arquivos = [os.path.join(root, file) for root, _, files in os.walk(temp_dir) for file in files]
        if not arquivos:
            logging.info(f"Nenhum arquivo encontrado para upload em '{temp_dir}'.")
            return {"enviados": [], "erros": []}

        # Limpeza prévia no Azure
        logging.info(f"Limpando prefixo '{caminho_destino}' no Azure antes do upload.")
        limpar_prefixo_no_azure(blob_service_client, container_name, caminho_destino, workers)

        enviados, erros = [], []

        # Criar um novo loop de eventos para evitar erros do asyncio dentro da ThreadPoolExecutor
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(upload_arquivo_azure, blob_service_client, container_name, arquivo,
                                destino_path): arquivo
                for arquivo in arquivos
                for destino_path in [f"{caminho_destino}/{os.path.relpath(arquivo, temp_dir).replace(os.sep, '/')}"]
            }

            for future in as_completed(futures):
                destino_path, erro = future.result()
                if erro:
                    erros.append(destino_path)
                    logging.error(f"Erro no upload de '{destino_path}': {erro}")
                else:
                    enviados.append(destino_path)

        # Log final consolidado
        logging.info(f"Upload para o Azure finalizado. Total enviados: {len(enviados)}, Total com erro: {len(erros)}")

        if erros:
            logging.warning(f"Arquivos que falharam no upload: {erros}")

        return {"enviados": enviados, "erros": erros}

    except Exception as e:
        logging.error(f"Erro no upload para o Azure: {e}")
        return {"enviados": [], "erros": []}
