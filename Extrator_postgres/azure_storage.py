import os
import sys
import asyncio
import logging
import aiofiles
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Set, Tuple

# Cliente síncrono para limpeza
from azure.storage.blob import BlobServiceClient as BlobServiceClientSync
# Cliente assíncrono para upload
from azure.storage.blob.aio import BlobServiceClient as BlobServiceClientAsync

# --------------------------------------------------
# CONFIGURAÇÃO DE LOG
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)

# --------------------------------------------------
# VALIDAÇÃO DA CONFIGURAÇÃO AZURE
# --------------------------------------------------
def validar_config_azure(azure_config):
    """Valida e inicializa a configuração do Azure se necessário."""
    if "blob_service_client" not in azure_config:
        from azure.storage.blob import BlobServiceClient
        if "account_name" in azure_config and "account_key" in azure_config:
            azure_config["blob_service_client"] = BlobServiceClient(
                account_url=f"https://{azure_config['account_name']}.blob.core.windows.net",
                credential=azure_config["account_key"]
            )
            logging.info("Conexão com o Azure inicializada com sucesso (cliente síncrono).")
        else:
            raise ValueError("Configuração do Azure está incompleta. Forneça 'account_name' e 'account_key'.")
    return azure_config

# --------------------------------------------------
# FUNÇÕES DE NORMALIZAÇÃO E FORMATAÇÃO
# --------------------------------------------------
def normalizar_particao(particao: str) -> str:
    """Remove barras finais da string para normalização."""
    return particao.rstrip("/")

def formatar_particoes_log(particoes: Set[str], nivel: str) -> str:
    """Formata as partições para log (primeira, última e total) usando strings normalizadas."""
    if not particoes:
        return ""
    norm = {normalizar_particao(p) for p in particoes}
    particoes_ordenadas = sorted(norm)
    total = len(particoes_ordenadas)
    if total == 1:
        return f"{nivel}: {particoes_ordenadas[0]} (1 partição)"
    return f"{nivel}: {particoes_ordenadas[0]} ... {particoes_ordenadas[-1]} ({total} partições)"

# --------------------------------------------------
# FUNÇÕES DE LISTAGEM E EXTRAÇÃO DE PARTIÇÕES (SÍNCRONAS)
# --------------------------------------------------
def obter_blobs_azure_sync(blob_service_client: BlobServiceClientSync, container_name: str, prefix: str) -> List:
    """Lista todos os blobs no container que começam com o prefixo informado."""
    container_client = blob_service_client.get_container_client(container_name)
    return list(container_client.list_blobs(name_starts_with=prefix))

def extrair_particoes_dos_blobs(blobs: List) -> Set[str]:
    """
    Extrai as partições a partir dos nomes dos blobs, removendo o último segmento (nome do arquivo)
    e normalizando a string.
    """
    return {normalizar_particao("/".join(blob.name.split("/")[:-1])) for blob in blobs}

def filtrar_particoes_existentes(particoes_existentes: Set[str], particoes_recarregadas: Set[str]) -> Set[str]:
    """
    Retém somente as partições existentes que estejam contidas no conjunto de partições recarregadas.
    As comparações são feitas com strings normalizadas.
    """
    recarregadas_norm = {normalizar_particao(r) for r in particoes_recarregadas}
    def pertence_recarregadas(p: str) -> bool:
        p_norm = normalizar_particao(p)
        for rec in recarregadas_norm:
            if p_norm == rec or p_norm.startswith(rec + "/"):
                return True
        return False
    return {p for p in particoes_existentes if pertence_recarregadas(p)}

# --------------------------------------------------
# FUNÇÃO DE DEFINIÇÃO DE PARTIÇÕES PARA EXCLUSÃO
# --------------------------------------------------
def definir_particoes_para_exclusao(particoes_existentes: Set[str], particoes_recarregadas: Set[str]) -> Dict[str, Set[str]]:
    """
    Define as partições a serem excluídas de acordo com o tipo de consulta:
      - Tipo A (Somente idEmpresa): Se as partições recarregadas não contiverem indicadores de data,
        a exclusão será feita a nível de idEmpresa.
      - Tipo B (idEmpresa + Data): Se houver informações de data, avalia os níveis Dia, Mes e Ano,
        excluindo somente os dados que estão sendo recarregados.
    Todas as comparações são feitas com strings normalizadas.
    """
    if not particoes_existentes:
        return {}

    particoes_recarregadas_norm = {normalizar_particao(r) for r in particoes_recarregadas}
    tem_data = any(token in r for r in particoes_recarregadas_norm for token in ("Ano=", "Mes="))
    if not tem_data:
        exclusao = {"idEmpresa": set()}
        id_empresas = {normalizar_particao(r).split("/")[0] for r in particoes_recarregadas_norm if "idEmpresa=" in r}
        for id_empresa in id_empresas:
            if any(normalizar_particao(p).startswith(id_empresa) for p in particoes_existentes):
                exclusao["idEmpresa"].add(id_empresa)
        return exclusao
    else:
        exclusao = {"Mes": set(), "Ano": set(), "idEmpresa": set()}
        exclusao["Mes"] = {normalizar_particao(r) for r in particoes_recarregadas_norm if all(token in r for token in ("Ano=", "Mes="))}
        anos_map: Dict[str, Set[str]] = {}
        for mes in exclusao["Mes"]:
            ano = mes.split("/")[0]
            anos_map.setdefault(ano, set()).add(mes)
        for ano, meses_recarregados in anos_map.items():
            meses_existentes = {normalizar_particao(p) for p in particoes_existentes if normalizar_particao(p).startswith(ano)}
            if meses_existentes and meses_existentes == meses_recarregados:
                exclusao["Ano"].add(ano)
        particoes_excluidas = exclusao["Mes"] | exclusao["Ano"]
        id_empresas = {normalizar_particao(r).split("/")[0] for r in particoes_recarregadas_norm if "idEmpresa=" in r}
        for id_empresa in id_empresas:
            particoes_empresa = {normalizar_particao(p) for p in particoes_existentes if normalizar_particao(p).startswith(id_empresa)}
            if particoes_empresa and particoes_empresa.issubset(particoes_excluidas):
                exclusao["idEmpresa"].add(id_empresa)
            else:
                logging.info(f"{id_empresa} NÃO será excluída pois possui partições válidas não recarregadas.")
        return exclusao
# --------------------------------------------------
# FUNÇÃO DE DELEÇÃO EM BATCH (SÍNCRONA) PARA AZURE
# --------------------------------------------------
def chunk_list(lst: List, chunk_size: int):
    """Divide uma lista em chunks de tamanho chunk_size."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def executar_exclusao_blobs_batch(blob_service_client: BlobServiceClientSync,
                                  container_name: str,
                                  blob_names: List[str],
                                  max_workers: int = 10,
                                  dry_run: bool = False,
                                  nome_consulta: str = "") -> None:
    """
    Realiza a deleção em batch dos blobs usando o método delete_blobs do ContainerClient.
    Divide a lista em chunks (até 256 blobs por lote) e executa em paralelo.
    """
    if dry_run:
        logging.info(f"[{nome_consulta}] Dry run ativado: {len(blob_names)} blobs seriam deletados.")
        return

    # Obtém o container_client
    container_client = blob_service_client.get_container_client(container_name)
    errors = []

    def delete_batch(chunk: List[str]):
        try:
            # Usa o método delete_blobs diretamente no container_client
            container_client.delete_blobs(*chunk, raise_on_any_failure=True)
        except Exception as e:
            return e, chunk
        return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(delete_batch, chunk): chunk for chunk in chunk_list(blob_names, 256)}
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                err, chunk = result
                errors.append((err, chunk))
                logging.error(f"[{nome_consulta}] Erro ao deletar lote: {err}. Blobs: {chunk}")

    if errors:
        logging.error(f"[{nome_consulta}] Erros durante a deleção em batch: {errors}")
        raise Exception("Falha na deleção em batch de blobs.")
    else:
        logging.info(f"[{nome_consulta}] Deleção em batch concluída com sucesso para {len(blob_names)} blobs.")

def limpar_prefixo_no_azure(blob_service_client: BlobServiceClientSync, container_name: str, caminho_destino: str,
                             particoes_recarregadas: List[str], workers: int = 10, dry_run: bool = False,
                             nome_consulta: str = "") -> None:
    if not particoes_recarregadas:
        logging.info(f"[{nome_consulta}] Nenhuma partição para exclusão no Azure.")
        return

    blobs = obter_blobs_azure_sync(blob_service_client, container_name, caminho_destino)
    particoes_existentes = extrair_particoes_dos_blobs(blobs)
    # Filtra apenas aquelas que contenham "idEmpresa="
    particoes_existentes = {normalizar_particao(p) for p in particoes_existentes if "idEmpresa=" in p}
    # Remove o prefixo remoto (caminho_destino) se presente, para que o resultado seja apenas "idEmpresa=XYZ/..."
    prefixo = normalizar_particao(caminho_destino)
    particoes_existentes = {
        normalizar_particao(p[len(prefixo)+1:]) if p.startswith(prefixo + "/") else normalizar_particao(p)
        for p in particoes_existentes
    }
    # Agora compara com as partições recarregadas (que devem conter apenas o final do caminho)
    particoes_existentes = filtrar_particoes_existentes(particoes_existentes, {normalizar_particao(r) for r in particoes_recarregadas})
    if not particoes_existentes:
        logging.info(f"[{nome_consulta}] Nenhuma partição existente (pertencente à recarga) encontrada para o prefixo '{caminho_destino}'.")
        return

    exclusao = definir_particoes_para_exclusao(particoes_existentes, {normalizar_particao(r) for r in particoes_recarregadas})
    if not exclusao:
        logging.info(f"[{nome_consulta}] Não há partições marcadas para exclusão.")
        return

    for nivel, parts in exclusao.items():
        log_msg = formatar_particoes_log(parts, nivel)
        if log_msg:
            logging.info(f"[{nome_consulta}] Exclusão no nível {nivel}: {log_msg}")

    blob_names = []
    container_client = blob_service_client.get_container_client(container_name)
    for nivel, parts in exclusao.items():
        for particao in parts:
            prefixo_completo = f"{caminho_destino}/{particao}".rstrip("/") + "/"
            blobs_part = list(container_client.list_blobs(name_starts_with=prefixo_completo))
            blob_names.extend([blob.name for blob in blobs_part if not blob.name.endswith("/")])

    if not blob_names:
        logging.info(f"[{nome_consulta}] Nenhum blob encontrado para exclusão no Azure.")
        return

    logging.info(f"[{nome_consulta}] {len(blob_names)} blobs serão deletados (processo crítico).")
    executar_exclusao_blobs_batch(blob_service_client, container_name, blob_names,
                                  max_workers=workers, dry_run=dry_run, nome_consulta=nome_consulta)

# --------------------------------------------------
# FUNÇÕES DE UPLOAD ASSÍNCRONO – AZURE
# --------------------------------------------------
async def upload_file_async(semaphore: asyncio.Semaphore,
                            blob_service_client: BlobServiceClientAsync,
                            container_name: str,
                            local_path: str,
                            destino_blob: str) -> str:
    """
    Realiza o upload assíncrono de um único arquivo para o Azure Blob Storage,
    utilizando um semáforo para limitar a concorrência.
    """
    async with semaphore:
        try:
            async with aiofiles.open(local_path, "rb") as f:
                data = await f.read()
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=destino_blob)
            await blob_client.upload_blob(data, overwrite=True)
         #   logging.info(f"Upload realizado: {destino_blob}")
            return destino_blob
        except Exception as e:
            logging.error(f"Erro ao fazer upload de '{destino_blob}': {e}")
            raise

async def realizar_upload_azure_async(temp_dir: str,
                                      caminho_destino: str,
                                      azure_config: dict,
                                      max_concurrency: int = 1000,
                                      nome_consulta: str = "") -> dict:
    """
    Orquestra o upload assíncrono para o Azure Blob Storage:
      1. Lista recursivamente todos os arquivos em temp_dir.
      2. Para cada arquivo, determina o destino baseado em caminho_destino.
      3. Executa uploads concorrentes controlados por semáforo.
    """
    # Cria o cliente assíncrono utilizando os mesmos dados de conexão
    from azure.storage.blob.aio import BlobServiceClient as BlobServiceClientAsync
    blob_service_client = BlobServiceClientAsync(
        account_url=f"https://{azure_config['account_name']}.blob.core.windows.net",
        credential=azure_config["account_key"]
    )
    container_name = azure_config["container_name"]

    arquivos = []
    for root, _, files in os.walk(temp_dir):
        for file in files:
            arquivos.append(os.path.join(root, file))
    if not arquivos:
        logging.info(f"[{nome_consulta}] Nenhum arquivo encontrado para upload em '{temp_dir}'.")
        await blob_service_client.close()
        return {"enviados": [], "erros": []}

    logging.info(f"[{nome_consulta}] Iniciando upload de {len(arquivos)} arquivos para o Azure com {max_concurrency} uploads concorrentes...")
    semaphore = asyncio.Semaphore(max_concurrency)
    tasks = []
    for file_path in arquivos:
        relative_path = os.path.relpath(file_path, temp_dir).replace(os.sep, "/")
        destino_blob = f"{caminho_destino}/{relative_path}"
        tasks.append(upload_file_async(semaphore, blob_service_client, container_name, file_path, destino_blob))

    enviados = []
    erros = []
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            erros.append(str(result))
        else:
            enviados.append(result)
    logging.info(f"[{nome_consulta}] Upload concluído. Enviados: {len(enviados)}, Erros: {len(erros)}")
    await blob_service_client.close()
    return {"enviados": enviados, "erros": erros}

# --------------------------------------------------
# FUNÇÃO FINAL – INTEGRA LIMPEZA (SÍNCRONA) E UPLOAD (ASSÍNCRONO) PARA AZURE
# --------------------------------------------------
def realizar_upload_azure(temp_dir: str, caminho_destino: str, azure_config: dict,
                           workers: int = 10, max_concurrency: int = 1000, dry_run: bool = False,
                           nome_consulta: str = "") -> dict:
    """
    Executa o fluxo completo para o Azure:
      1. Valida a configuração e inicializa o cliente síncrono se necessário.
      2. Executa a limpeza das partições recarregadas (deleção em batch).
      3. Realiza o upload assíncrono dos arquivos.
    """
    # 🔹 Reduzir logs desnecessários de HTTP
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    azure_config = validar_config_azure(azure_config)
    blob_service_client_sync = azure_config["blob_service_client"]
    container_name = azure_config["container_name"]

    # Para alinhar as partições locais com o prefixo remoto,
    # considere que os arquivos estão em <temp_dir>/<caminho_destino>/...
    base_local = os.path.join(temp_dir, caminho_destino)
    if os.path.isdir(base_local):
        # Extrai partições relativas a partir de base_local
        particoes = [os.path.relpath(root, base_local).replace(os.sep, "/")
                     for root, _, _ in os.walk(base_local) if "idEmpresa=" in root]
        # Se a raiz (base_local) contém "idEmpresa=", garanta que seja incluída
        if "idEmpresa=" in os.path.basename(base_local):
            particoes.append(os.path.basename(base_local))
    else:
        # Caso os arquivos estejam organizados diretamente em temp_dir
        particoes = [os.path.relpath(root, temp_dir).replace(os.sep, "/")
                     for root, _, _ in os.walk(temp_dir) if "idEmpresa=" in root]

    # Executa a limpeza das partições recarregadas
    limpar_prefixo_no_azure(blob_service_client_sync, container_name, caminho_destino,
                             particoes, workers, dry_run, nome_consulta)

    # Em seguida, realiza o upload assíncrono
    return asyncio.run(realizar_upload_azure_async(temp_dir, caminho_destino, azure_config, max_concurrency, nome_consulta))
