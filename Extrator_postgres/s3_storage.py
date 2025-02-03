import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

from config import STORAGE_CONFIG


def limpar_prefixo_no_s3(s3_client, bucket_name, prefixo, max_workers=5):
    """
    Remove todos os objetos em um prefixo específico no S3 de forma paralela.

    Args:
        s3_client: Cliente do Amazon S3.
        bucket_name (str): Nome do bucket no S3.
        prefixo (str): Prefixo a ser limpo.
        max_workers (int): Número de threads paralelas para exclusão.
    """
    try:
        if not bucket_name:
            raise ValueError("O parâmetro 'bucket_name' não pode ser vazio.")
        if not prefixo:
            raise ValueError("O parâmetro 'prefixo' não pode ser vazio.")

        prefixo = f"{prefixo.rstrip('/')}/idEmpresa={STORAGE_CONFIG['idemp']}/"
        logging.info(f"Iniciando limpeza do prefixo '{prefixo}' no bucket '{bucket_name}' com {max_workers} threads...")

        objetos_excluidos, erros = 0, []
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefixo)

        if not response.get("Contents"):
            logging.info(f"Nenhum objeto encontrado para exclusão em '{prefixo}'.")
            return

        def excluir_objeto(obj):
            """Exclui um único objeto do bucket."""
            try:
                s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
                return obj["Key"], None  # Sucesso
            except Exception as e:
                return obj["Key"], str(e)  # Erro

        while response.get("Contents"):
            objetos = response["Contents"]

            # Executa a exclusão em paralelo
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_objeto = {executor.submit(excluir_objeto, obj): obj for obj in objetos}

                for future in as_completed(future_to_objeto):
                    obj_key, erro = future.result()
                    if erro:
                        erros.append(obj_key)
                        logging.error(f"Erro ao excluir objeto '{obj_key}': {erro}")
                    else:
                        objetos_excluidos += 1

            # Paginação para grandes volumes
            if response.get("IsTruncated"):
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name, Prefix=prefixo, ContinuationToken=response["NextContinuationToken"]
                )
            else:
                break

        logging.info(f"Limpeza concluída. Objetos removidos: {objetos_excluidos}, Erros: {len(erros)}")

        if erros:
            logging.warning(f"Objetos que falharam na exclusão: {erros}")

    except Exception as e:
        logging.error(f"Erro ao limpar prefixo no S3: {e}")
        raise


def realizar_upload_s3(temp_dir: str, caminho_destino: str, s3_config: Dict, workers: int = 5) -> Dict[str, List[str]]:
    """
    Realiza o upload de arquivos para o S3 de forma paralela, corrigindo problemas com asyncio.

    Args:
        temp_dir (str): Diretório temporário contendo os arquivos.
        caminho_destino (str): Caminho no S3.
        s3_config (dict): Configurações do S3 (s3_client, bucket).
        workers (int): Número máximo de threads para paralelismo.

    Returns:
        Dict[str, List[str]]: Dicionário com listas de arquivos enviados e arquivos com erro.
    """

    def upload_arquivo_s3(s3_client, bucket, local_path, destino_path):
        """Realiza o upload de um único arquivo para o S3."""
        try:
            with open(local_path, "rb") as data:
                s3_client.upload_fileobj(data, bucket, destino_path)
            return destino_path, None  # Sucesso
        except Exception as e:
            return destino_path, str(e)  # Erro

    try:
        logging.info(f"Iniciando upload para o S3: '{caminho_destino}' com {workers} threads...")

        s3_client = s3_config["s3_client"]
        bucket = s3_config["bucket"]

        # Obter lista de arquivos para upload
        arquivos = [os.path.join(root, file) for root, _, files in os.walk(temp_dir) for file in files]
        if not arquivos:
            logging.info(f"Nenhum arquivo encontrado para upload em '{temp_dir}'.")
            return {"enviados": [], "erros": []}

        # Limpeza prévia no S3
        logging.info(f"Limpando prefixo '{caminho_destino}' no S3 antes do upload.")
        limpar_prefixo_no_s3(s3_client, bucket, caminho_destino, workers)

        enviados, erros = [], []

        # Criar um novo loop de eventos para evitar erros do asyncio dentro da ThreadPoolExecutor
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(upload_arquivo_s3, s3_client, bucket, arquivo, destino_path): arquivo
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
        logging.info(f"Upload para o S3 finalizado. Total enviados: {len(enviados)}, Total com erro: {len(erros)}")

        if erros:
            logging.warning(f"Arquivos que falharam no upload: {erros}")

        return {"enviados": enviados, "erros": erros}

    except Exception as e:
        logging.error(f"Erro no upload para o S3: {e}")
        return {"enviados": [], "erros": []}
