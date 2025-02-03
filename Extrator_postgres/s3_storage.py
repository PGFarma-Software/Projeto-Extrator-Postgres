import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

from config import STORAGE_CONFIG


def limpar_prefixo_no_s3(s3_client, bucket_name, caminho_destino, particoes, workers=5, nome_consulta: str = ""):
    """
    Remove apenas as partições que precisam ser recarregadas no S3.

    Args:
        s3_client: Cliente do Amazon S3.
        bucket_name (str): Nome do bucket no S3.
        caminho_destino (str): Caminho no S3 onde os dados estão armazenados.
        particoes (list): Lista de partições que precisam ser limpas antes do recarregamento.
        nome_consulta (str): Nome da consulta (usado para diferenciar a regra de limpeza).
        workers (int): Número de threads paralelas para exclusão.
    """
    try:
        if not particoes:
            logging.info(f"[{nome_consulta}] Nenhuma partição relevante encontrada para exclusão no S3.")
            return

        # 🔹 Definir estratégia de limpeza:
        if nome_consulta in ["Compras", "Vendas"]:
            particoes_validas = [p for p in particoes if "AnoMesDia=" in p]
        else:
            particoes_validas = [p for p in particoes if "idEmpresa=" in p and "AnoMesDia=" not in p]

        if not particoes_validas:
            logging.info(f"[{nome_consulta}] Nenhuma partição precisa ser excluída no S3.")
            return

        # 🔹 Exibir apenas a primeira e a última partição
        total_particoes = len(particoes_validas)
        if total_particoes == 1:
            particoes_log = f"{particoes_validas[0]}"
        else:
            particoes_log = f"{particoes_validas[0]} ... {particoes_validas[-1]}"

        logging.info(f"[{nome_consulta}] Iniciando limpeza no S3. Total de partições: {total_particoes}")
        logging.info(f"[{nome_consulta}] Partições afetadas: {particoes_log}")

        objetos_para_excluir = []

        # 🔹 Identificar os arquivos pertencentes às partições que serão atualizadas
        for particao in particoes_validas:
            prefixo_completo = f"{caminho_destino}/{particao}".rstrip("/") + "/"
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefixo_completo)

            if "Contents" in response:
                arquivos_para_excluir = [obj["Key"] for obj in response["Contents"] if not obj["Key"].endswith("/")]
                objetos_para_excluir.extend(arquivos_para_excluir)

        if not objetos_para_excluir:
            logging.info(f"[{nome_consulta}] Nenhum arquivo encontrado para exclusão nas partições selecionadas.")
            return

        logging.info(f"[{nome_consulta}] Removendo {len(objetos_para_excluir)} arquivos no S3...")

        def excluir_objeto(obj_key):
            """Exclui um único objeto no S3."""
            try:
                s3_client.delete_object(Bucket=bucket_name, Key=obj_key)
                return obj_key, None  # Sucesso
            except Exception as e:
                return obj_key, str(e)  # Erro

        erros = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_obj = {executor.submit(excluir_objeto, obj): obj for obj in objetos_para_excluir}

            for future in as_completed(future_to_obj):
                obj_key, erro = future.result()
                if erro:
                    erros.append(obj_key)
                    logging.error(f"[{nome_consulta}] Erro ao excluir objeto '{obj_key}': {erro}")

        total_excluidos = len(objetos_para_excluir) - len(erros)
        total_erros = len(erros)

        logging.info(f"[{nome_consulta}] Limpeza no S3 finalizada. Arquivos removidos: {total_excluidos}, Erros: {total_erros}")

        if total_erros > 0:
            logging.warning(f"[{nome_consulta}] Objetos que falharam na exclusão no S3.")

    except Exception as e:
        logging.error(f"[{nome_consulta}] Erro ao limpar partições no S3: {e}")
        raise


def realizar_upload_s3(temp_dir: str, caminho_destino: str, s3_config: Dict, workers: int = 5, nome_consulta: str = "") -> Dict[str, List[str]]:
    """
    Realiza o upload de arquivos para o S3 de forma paralela.

    Args:
        temp_dir (str): Diretório temporário contendo os arquivos.
        caminho_destino (str): Caminho no S3.
        s3_config (dict): Configurações do S3 (s3_client, bucket).
        workers (int): Número máximo de threads para paralelismo.
        nome_consulta (str): Nome da consulta (usado para logs mais claros).

    Returns:
        Dict[str, List[str]]: Dicionário com listas de arquivos enviados e arquivos com erro.
    """
    # 🔹 Identificar partições que serão limpas antes do upload
    particoes = [
        os.path.relpath(root, temp_dir).replace(os.sep, "/")
        for root, _, _ in os.walk(temp_dir) if "idEmpresa=" in root
    ]

    # 🔹 Definição da função de upload individual
    def upload_arquivo_s3(s3_client, bucket, local_path, destino_path):
        """Realiza o upload de um único arquivo para o S3."""
        try:
            with open(local_path, "rb") as data:
                s3_client.upload_fileobj(data, bucket, destino_path)
            return destino_path, None  # Sucesso
        except Exception as e:
            return destino_path, str(e)  # Erro

    try:
        logging.info(f"[{nome_consulta}] Iniciando upload para o S3 ({len(particoes)} partições) usando {workers} threads...")

        s3_client = s3_config["s3_client"]
        bucket = s3_config["bucket"]

        # 🔹 Obter lista de arquivos a serem enviados
        arquivos = [os.path.join(root, file) for root, _, files in os.walk(temp_dir) for file in files]

        if not arquivos:
            logging.info(f"[{nome_consulta}] Nenhum arquivo encontrado para upload em '{temp_dir}'.")
            return {"enviados": [], "erros": []}

        # 🔹 Limpeza seletiva no S3 apenas das partições relevantes
        limpar_prefixo_no_s3(s3_client, bucket, caminho_destino, particoes, workers, nome_consulta)

        enviados, erros = [], []

        # 🔹 Execução do upload em paralelo usando ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    upload_arquivo_s3, s3_client, bucket, arquivo,
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

        logging.info(f"[{nome_consulta}] Upload para S3 concluído. Total enviados: {total_enviados}, Total com erro: {total_erros}")

        if total_erros > 0:
            logging.warning(f"[{nome_consulta}] Arquivos que falharam no upload: {erros}")

        return {"enviados": enviados, "erros": erros}

    except Exception as e:
        logging.error(f"[{nome_consulta}] Erro no upload para o S3: {e}")
        return {"enviados": [], "erros": []}
