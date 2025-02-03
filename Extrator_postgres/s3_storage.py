import os
import sys
import asyncio
import logging
import aiofiles
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Set, Tuple

# Cliente síncrono para limpeza (boto3)
import boto3
# Cliente assíncrono para upload (aioboto3)
import aioboto3

# --------------------------------------------------
# CONFIGURAÇÃO DE LOG
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)

# --------------------------------------------------
# VALIDAÇÃO DA CONFIGURAÇÃO S3
# --------------------------------------------------
def validar_config_s3(s3_config):
    """Valida e inicializa a configuração do S3 se necessário."""
    if "s3_client" not in s3_config:
        s3_config["s3_client"] = boto3.client(
            "s3",
            aws_access_key_id=s3_config.get("access_key"),
            aws_secret_access_key=s3_config.get("secret_key"),
            region_name=s3_config.get("region"),
        )
        logging.info("Conexão com o S3 inicializada com sucesso.")
    return s3_config

# --------------------------------------------------
# FUNÇÕES AUXILIARES PARA DELEÇÃO EM BATCH (SÍNCRONA)
# --------------------------------------------------
def chunk_list(lst: List, chunk_size: int):
    """Divide uma lista em pedaços (chunks) de tamanho chunk_size."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def executar_exclusao_objetos_batch(s3_client, bucket: str, object_keys: List[str],
                                    max_workers: int = 10, dry_run: bool = False,
                                    nome_consulta: str = "") -> None:
    """
    Realiza a deleção em batch dos objetos utilizando o método delete_objects do S3.
    Deleta até 1000 objetos por requisição.

    Parâmetros:
      - s3_client: Cliente síncrono do boto3.
      - bucket: Nome do bucket.
      - object_keys: Lista de chaves dos objetos a serem deletados.
      - max_workers: Número máximo de threads para deleção concorrente.
      - dry_run: Se True, apenas loga os objetos que seriam deletados.
      - nome_consulta: Identificador para os logs.
    """
    if dry_run:
        logging.info(f"[{nome_consulta}] Dry run ativado: {len(object_keys)} objetos seriam deletados.")
        return

    errors = []
    def delete_batch(chunk: List[str]):
        try:
            response = s3_client.delete_objects(
                Bucket=bucket,
                Delete={
                    'Objects': [{'Key': key} for key in chunk],
                    'Quiet': True
                }
            )
            if 'Errors' in response and response['Errors']:
                raise Exception(f"Erros no batch: {response['Errors']}")
        except Exception as e:
            return e, chunk
        return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(delete_batch, chunk): chunk for chunk in chunk_list(object_keys, 1000)}
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                err, chunk = result
                errors.append((err, chunk))
                logging.error(f"[{nome_consulta}] Erro ao deletar lote: {err}. Objetos: {chunk}")

    if errors:
        logging.error(f"[{nome_consulta}] Erros durante a deleção em batch: {errors}")
        raise Exception("Falha na deleção em batch de objetos.")
    else:
        logging.info(f"[{nome_consulta}] Deleção em batch concluída com sucesso para {len(object_keys)} objetos.")

# --------------------------------------------------
# FUNÇÕES DE LISTAGEM, EXTRAÇÃO E FILTRAGEM DE PARTIÇÕES (S3)
# --------------------------------------------------
def normalizar_particao(particao: str) -> str:
    """Remove barras finais da string da partição para normalização."""
    return particao.rstrip("/")

def formatar_particoes_log(particoes: Set[str], nivel: str) -> str:
    """Formata as partições para log: exibe a primeira, a última e o total."""
    if not particoes:
        return ""
    norm = {normalizar_particao(p) for p in particoes}
    particoes_ordenadas = sorted(norm)
    total = len(particoes_ordenadas)
    if total == 1:
        return f"{nivel}: {particoes_ordenadas[0]} (1 partição)"
    return f"{nivel}: {particoes_ordenadas[0]} ... {particoes_ordenadas[-1]} ({total} partições)"

def listar_objetos_s3(s3_client, bucket: str, prefix: str) -> List[dict]:
    """Lista todos os objetos existentes no bucket que começam com o prefixo informado."""
    objetos = []
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    while response.get("Contents"):
        objetos.extend(response["Contents"])
        if response.get("IsTruncated"):
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                ContinuationToken=response["NextContinuationToken"]
            )
        else:
            break
    return objetos

def extrair_particoes_dos_objetos(objetos: List[dict]) -> Set[str]:
    """
    Extrai as partições a partir das chaves dos objetos.
    Considera como partição a string resultante de remover o último segmento (nome do arquivo)
    e normaliza a string (remove trailing slash).
    """
    return {normalizar_particao("/".join(obj["Key"].split("/")[:-1])) for obj in objetos}

def filtrar_particoes_existentes(particoes_existentes: Set[str], particoes_recarregadas: Set[str]) -> Set[str]:
    """
    Mantém somente as partições existentes que pertençam ao conjunto de partições recarregadas.
    A verificação é feita após normalizar ambas as strings.
    """
    recarregadas_norm = {normalizar_particao(r) for r in particoes_recarregadas}
    def pertence_recarregadas(p: str) -> bool:
        p_norm = normalizar_particao(p)
        for rec in recarregadas_norm:
            if p_norm == rec or p_norm.startswith(rec + "/"):
                return True
        return False
    return {p for p in particoes_existentes if pertence_recarregadas(p)}

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
    tem_data = any(token in r for r in particoes_recarregadas_norm for token in ("Ano=", "Mes=", "Dia="))
    if not tem_data:
        exclusao = {"idEmpresa": set()}
        id_empresas = {normalizar_particao(r).split("/")[0] for r in particoes_recarregadas_norm if "idEmpresa=" in r}
        for id_empresa in id_empresas:
            if any(normalizar_particao(p).startswith(id_empresa) for p in particoes_existentes):
                exclusao["idEmpresa"].add(id_empresa)
        return exclusao
    else:
        exclusao = {"Dia": set(), "Mes": set(), "Ano": set(), "idEmpresa": set()}
        exclusao["Dia"] = {normalizar_particao(r) for r in particoes_recarregadas_norm if all(token in r for token in ("Ano=", "Mes=", "Dia="))}
        meses_map: Dict[str, Set[str]] = {}
        for r in exclusao["Dia"]:
            mes = r.rsplit("/", 1)[0]
            meses_map.setdefault(mes, set()).add(r)
        for mes, dias_recarregados in meses_map.items():
            dias_existentes = {normalizar_particao(p) for p in particoes_existentes if normalizar_particao(p).startswith(mes)}
            if dias_existentes and dias_existentes == dias_recarregados:
                exclusao["Mes"].add(mes)
        anos_map: Dict[str, Set[str]] = {}
        for mes in exclusao["Mes"]:
            ano = mes.split("/")[0]
            anos_map.setdefault(ano, set()).add(mes)
        for ano, meses_recarregados in anos_map.items():
            meses_existentes = {normalizar_particao(p) for p in particoes_existentes if normalizar_particao(p).startswith(ano)}
            if meses_existentes and meses_existentes == meses_recarregados:
                exclusao["Ano"].add(ano)
        particoes_excluidas = exclusao["Dia"] | exclusao["Mes"] | exclusao["Ano"]
        id_empresas = {normalizar_particao(r).split("/")[0] for r in particoes_recarregadas_norm if "idEmpresa=" in r}
        for id_empresa in id_empresas:
            particoes_empresa = {normalizar_particao(p) for p in particoes_existentes if normalizar_particao(p).startswith(id_empresa)}
            if particoes_empresa and particoes_empresa.issubset(particoes_excluidas):
                exclusao["idEmpresa"].add(id_empresa)
            else:
                logging.info(f"{id_empresa} NÃO será excluída pois possui partições válidas não recarregadas.")
        return exclusao

def limpar_prefixo_no_s3(s3_client, bucket: str, caminho_destino: str,
                         particoes_recarregadas: List[str], workers: int = 10,
                         dry_run: bool = False, nome_consulta: str = "") -> None:
    """
    Orquestra a limpeza no S3:
      1. Lista os objetos com o prefixo.
      2. Extrai e filtra as partições existentes (mantendo apenas as da recarga).
      3. Define as partições a serem excluídas.
      4. Obtém a lista de objetos (chaves) a serem deletados.
      5. Executa a deleção em batch de forma concorrente (com modo dry_run opcional).
    """
    if not particoes_recarregadas:
        logging.info(f"[{nome_consulta}] Nenhuma partição para exclusão no S3.")
        return

    objetos = listar_objetos_s3(s3_client, bucket, caminho_destino)
    particoes_existentes = extrair_particoes_dos_objetos(objetos)
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

    exclusao = definir_particoes_para_exclusao(particoes_existentes, set(particoes_recarregadas))
    if not exclusao:
        logging.info(f"[{nome_consulta}] Não há partições marcadas para exclusão.")
        return

    for nivel, parts in exclusao.items():
        log_msg = formatar_particoes_log(parts, nivel)
        if log_msg:
            logging.info(f"[{nome_consulta}] Exclusão no nível {nivel}: {log_msg}")

    object_keys = []
    for nivel, parts in exclusao.items():
        for particao in parts:
            prefixo_completo = f"{caminho_destino}/{particao}".rstrip("/") + "/"
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefixo_completo)
            if "Contents" in response:
                object_keys.extend([obj["Key"] for obj in response["Contents"] if not obj["Key"].endswith("/")])

    if not object_keys:
        logging.info(f"[{nome_consulta}] Nenhum objeto encontrado para exclusão no S3.")
        return

    logging.info(f"[{nome_consulta}] {len(object_keys)} objetos serão deletados (processo crítico).")
    executar_exclusao_objetos_batch(s3_client, bucket, object_keys, max_workers=workers, dry_run=dry_run, nome_consulta=nome_consulta)

# --------------------------------------------------
# FUNÇÕES DE UPLOAD ASSÍNCRONO – S3 (USANDO aioboto3)
# --------------------------------------------------
async def upload_file_s3_async(semaphore: asyncio.Semaphore,
                               s3_client,
                               bucket: str,
                               local_path: str,
                               destino_path: str) -> str:
    """
    Realiza o upload assíncrono de um único arquivo para o S3,
    utilizando um semáforo para limitar o número de uploads concorrentes.
    """
    async with semaphore:
        try:
            async with aiofiles.open(local_path, "rb") as f:
                data = await f.read()
            await s3_client.put_object(Bucket=bucket, Key=destino_path, Body=data)
         #   logging.info(f"Upload realizado: {destino_path}")
            return destino_path
        except Exception as e:
            logging.error(f"Erro ao fazer upload de '{destino_path}': {e}")
            raise

async def realizar_upload_s3_async(temp_dir: str,
                                   caminho_destino: str,
                                   s3_config: Dict,
                                   max_concurrency: int = 1000,
                                   nome_consulta: str = "") -> dict:
    """
    Orquestra o upload assíncrono para o S3:
      1. Lista recursivamente todos os arquivos em temp_dir.
      2. Para cada arquivo, determina o destino (baseado em caminho_destino).
      3. Executa uploads concorrentes controlados por semáforo.
    """
    bucket = s3_config["bucket"]
    session = aioboto3.Session(
        aws_access_key_id=s3_config["access_key"],
        aws_secret_access_key=s3_config["secret_key"],
        region_name=s3_config.get("region")
    )
    async with session.client('s3') as s3_client:
        arquivos = []
        for root, _, files in os.walk(temp_dir):
            for file in files:
                arquivos.append(os.path.join(root, file))
        if not arquivos:
            logging.info(f"[{nome_consulta}] Nenhum arquivo encontrado para upload em '{temp_dir}'.")
            return {"enviados": [], "erros": []}
        logging.info(f"[{nome_consulta}] Iniciando upload de {len(arquivos)} arquivos para o S3 com {max_concurrency} uploads concorrentes...")
        semaphore = asyncio.Semaphore(max_concurrency)
        tasks = []
        for file_path in arquivos:
            relative_path = os.path.relpath(file_path, temp_dir).replace(os.sep, "/")
            destino_path = f"{caminho_destino}/{relative_path}"
            tasks.append(upload_file_s3_async(semaphore, s3_client, bucket, file_path, destino_path))
        enviados = []
        erros = []
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                erros.append(str(result))
            else:
                enviados.append(result)
        logging.info(f"[{nome_consulta}] Upload concluído. Enviados: {len(enviados)}, Erros: {len(erros)}")
        return {"enviados": enviados, "erros": erros}

# --------------------------------------------------
# FUNÇÃO FINAL – INTEGRA LIMPEZA (SÍNCRONA) E UPLOAD (ASSÍNCRONO) PARA S3
# --------------------------------------------------
def realizar_upload_s3(temp_dir: str, caminho_destino: str, s3_config: Dict,
                       workers: int = 10, max_concurrency: int = 1000, dry_run: bool = False,
                       nome_consulta: str = "") -> dict:
    """
    Executa o fluxo completo para o S3:
      1. Valida a configuração e inicializa o cliente síncrono se necessário.
      2. Executa a limpeza das partições recarregadas (deleção em batch).
      3. Realiza o upload assíncrono dos arquivos.
    """
    s3_config = validar_config_s3(s3_config)
    s3_client_sync = s3_config["s3_client"]
    bucket = s3_config["bucket"]

    # Identifica as partições locais a partir dos diretórios que contenham "idEmpresa="
    particoes = [
        os.path.relpath(root, temp_dir).replace(os.sep, "/")
        for root, _, _ in os.walk(temp_dir) if "idEmpresa=" in root
    ]
    # Executa a limpeza das partições recarregadas (síncrona)
    limpar_prefixo_no_s3(s3_client_sync, bucket, caminho_destino, particoes, workers, dry_run, nome_consulta)

    # Em seguida, realiza o upload assíncrono
    return asyncio.run(realizar_upload_s3_async(temp_dir, caminho_destino, s3_config, max_concurrency, nome_consulta))
