import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import STORAGE_CONFIG


def limpar_prefixo_no_azure(blob_service_client, container_name, caminho_destino, particoes, workers=5, nome_consulta=""):
    """
    Remove apenas as partições que precisam ser recarregadas no Azure Blob Storage.

    Args:
        blob_service_client: Cliente do Azure Blob Storage.
        container_name (str): Nome do container no Azure.
        caminho_destino (str): Caminho base no Azure onde os dados estão armazenados.
        particoes (list): Lista de partições que precisam ser limpas antes do recarregamento.
        nome_consulta (str): Nome da consulta (usado para diferenciar a regra de limpeza).
        workers (int): Número de threads paralelas para exclusão.
    """
    try:
        if not particoes:
            logging.info(f"[{nome_consulta}] Nenhuma partição relevante encontrada para exclusão no Azure.")
            return

        # 🔹 Definir estratégia de limpeza:
        if nome_consulta in ["Compras", "Vendas"]:
            particoes_validas = [p for p in particoes if "AnoMesDia=" in p]
        else:
            particoes_validas = [p for p in particoes if "idEmpresa=" in p and "AnoMesDia=" not in p]

        if not particoes_validas:
            logging.info(f"[{nome_consulta}] Nenhuma partição precisa ser excluída no Azure.")
            return

        # 🔹 Exibir apenas a primeira e a última partição
        total_particoes = len(particoes_validas)
        if total_particoes == 1:
            particoes_log = f"{particoes_validas[0]}"
        else:
            particoes_log = f"{particoes_validas[0]} ... {particoes_validas[-1]}"

        logging.info(f"[{nome_consulta}] Iniciando limpeza no Azure. Total de partições: {total_particoes}")
        logging.info(f"[{nome_consulta}] Partições afetadas: {particoes_log}")

        blobs_para_excluir = []

        # 🔹 Identificar os blobs pertencentes às partições que serão atualizadas
        container_client = blob_service_client.get_container_client(container_name)
        for particao in particoes_validas:
            prefixo_completo = f"{caminho_destino}/{particao}".rstrip("/") + "/"
            blobs = list(container_client.list_blobs(name_starts_with=prefixo_completo))
            blobs_para_excluir.extend([blob.name for blob in blobs if not blob.name.endswith("/")])

        if not blobs_para_excluir:
            logging.info(f"[{nome_consulta}] Nenhum arquivo encontrado para exclusão nas partições selecionadas.")
            return

        logging.info(f"[{nome_consulta}] Removendo {len(blobs_para_excluir)} arquivos no Azure...")

        def excluir_blob(blob_name):
            """Exclui um único blob no Azure."""
            try:
                container_client.delete_blob(blob_name)
                return blob_name, None  # Sucesso
            except Exception as e:
                return blob_name, str(e)  # Erro

        erros = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_blob = {executor.submit(excluir_blob, blob): blob for blob in blobs_para_excluir}

            for future in as_completed(future_to_blob):
                blob_name, erro = future.result()
                if erro:
                    erros.append(blob_name)
                    logging.error(f"[{nome_consulta}] Erro ao excluir blob '{blob_name}': {erro}")

        total_excluidos = len(blobs_para_excluir) - len(erros)
        total_erros = len(erros)

        logging.info(f"[{nome_consulta}] Limpeza no Azure finalizada. Arquivos removidos: {total_excluidos}, Erros: {total_erros}")

        if total_erros > 0:
            logging.warning(f"[{nome_consulta}] Blobs que falharam na exclusão no Azure.")

    except Exception as e:
        logging.error(f"[{nome_consulta}] Erro ao limpar partições no Azure: {e}")
        raise


def realizar_upload_azure(temp_dir, caminho_destino, azure_config, workers=10, nome_consulta=""):
    """
    Realiza o upload de arquivos para o Azure Blob Storage de forma paralela.

    Args:
        temp_dir (str): Diretório temporário contendo os arquivos a serem enviados.
        caminho_destino (str): Caminho no Azure Blob Storage.
        azure_config (dict): Configurações do Azure.
        workers (int): Número máximo de threads para paralelismo.
        nome_consulta (str): Nome da consulta (para logs mais claros).
    """
    # 🔹 Reduzir logs desnecessários de HTTP
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # 🔹 Identificar partições a serem limpas antes do upload
    particoes = [
        os.path.relpath(root, temp_dir).replace(os.sep, "/")
        for root, _, _ in os.walk(temp_dir) if "idEmpresa=" in root
    ]

    # 🔹 Executa limpeza seletiva antes do upload
    limpar_prefixo_no_azure(
        azure_config["blob_service_client"], azure_config["container_name"], caminho_destino, particoes, workers, nome_consulta
    )

    # 🔹 Obter lista de arquivos a serem enviados
    arquivos = [os.path.join(root, file) for root, _, files in os.walk(temp_dir) for file in files]

    if not arquivos:
        logging.info(f"[{nome_consulta}] Nenhum arquivo encontrado para upload em '{temp_dir}'.")
        return {"enviados": [], "erros": []}

    enviados, erros = [], []

    def upload_arquivo_azure(blob_service_client, container_name, local_path, destino_path):
        """Realiza o upload de um único arquivo para o Azure Blob Storage."""
        try:
            with open(local_path, "rb") as data:
                blob_client = blob_service_client.get_blob_client(container_name, destino_path)
                blob_client.upload_blob(data, overwrite=True)
            return destino_path, None  # Sucesso
        except Exception as e:
            return destino_path, str(e)  # Erro

    # 🔹 Iniciar upload assíncrono usando threads
    logging.info(f"[{nome_consulta}] Iniciando upload para Azure ({len(arquivos)} arquivos) usando {workers} threads...")

    blob_service_client = azure_config["blob_service_client"]
    container_name = azure_config["container_name"]

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                upload_arquivo_azure, blob_service_client, container_name, arquivo,
                f"{caminho_destino}/{os.path.relpath(arquivo, temp_dir).replace(os.sep, '/')}"
            ): arquivo for arquivo in arquivos
        }

        for future in as_completed(futures):
            destino_path, erro = future.result()
            if erro:
                erros.append(destino_path)
                logging.error(f"[{nome_consulta}] Erro no upload de '{destino_path}': {erro}")
            else:
                enviados.append(destino_path)

    # 🔹 Log final consolidado
    total_enviados = len(enviados)
    total_erros = len(erros)

    logging.info(f"[{nome_consulta}] Upload para Azure concluído. Total enviados: {total_enviados}, Total com erro: {total_erros}")

    if total_erros > 0:
        logging.warning(f"[{nome_consulta}] Arquivos que falharam no upload: {erros}")

    return {"enviados": enviados, "erros": erros}
