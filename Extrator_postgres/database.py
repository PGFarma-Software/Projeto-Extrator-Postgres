import logging
import os
import sys
from datetime import datetime
import time
import concurrent.futures
import gc
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Dict
import pyarrow.parquet as pq
import polars as pl
import pandas as pd
import pytz
from psycopg2 import OperationalError
from sqlalchemy.engine import Connection
from config import DATABASE_CONFIG, GENERAL_CONFIG, STORAGE_CONFIG
from typing import List, Dict
import concurrent.futures
from sqlalchemy import create_engine, text

from dicionario_dados import ajustar_tipos_dados, obter_dicionario_tipos
from storage import enviar_resultados



def conectar_ao_banco(host: str, port: int, database: str, user: str, password: str) -> Connection:
    """
    Estabelece conexão com o banco de dados PostgreSQL.

    Args:
        host (str): Endereço do servidor do banco de dados.
        port (int): Porta do servidor do banco de dados.
        database (str): Caminho ou nome do banco de dados.
        user (str): Nome de usuário para autenticação.
        password (str): Senha para autenticação.

    Returns:
        sqlalchemy.engine.base.Connection: Objeto de conexão ao banco de dados.
    """
    try:
        logging.info("Conectando ao banco de dados PostgreSQL...")
        # Cria a URL de conexão com o banco de dados PostgreSQL
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        # Cria o engine de conexão usando a URL
        engine = create_engine(url)
        # Estabelece a conexão com o banco de dados
        conexao = engine.connect()
        logging.info("Conexão com o banco de dados PostgreSQL estabelecida com sucesso.")
        return conexao
    except Exception as e:
        logging.error(f"Erro ao conectar ao banco de dados PostgreSQL: {e}")
        raise


def fechar_conexao(conexao: Connection):
    """
    Fecha a conexão com o banco de dados.

    Args:
        conexao (Connection): Conexão ativa com o banco de dados.
    """
    try:
        conexao.close()
        logging.info("Conexão com o banco de dados fechada.")
    except Exception as e:
        logging.error(f"Erro ao fechar a conexão: {e}")


def executar_consultas(
    conexoes_config: dict,
    consultas: List[Dict[str, str]],
    pasta_temp: str,
    paralela: bool = False,
    workers: int = 4,
) -> Dict[str, str]:
    """
    Executa consultas no banco de dados de forma paralela ou sequencial utilizando conexão persistente.

    Args:
        conexoes_config (dict): Configuração de conexão com o banco de dados.
        consultas (List[Dict[str, str]]): Lista de consultas a serem executadas.
        pasta_temp (str): Caminho final onde os resultados serão salvos.
        paralela (bool): Define se as consultas serão executadas paralelamente ou sequencialmente.
        workers (int): Número de threads para execução paralela.

    Returns:
        Dict[str, str]: Dicionário com os caminhos das pastas com os arquivos particionados.
    """
    resultados = {}
    os.makedirs(pasta_temp, exist_ok=True)

    # 🔹 Criar conexão única para todas as consultas
    conexao = None
    try:
        conexao = conectar_ao_banco(**conexoes_config)
        logging.info("Conexão única com o banco estabelecida.")

        def processa_consulta(consulta):
            nome_consulta = consulta.get("name", "").replace(" ", "")
            query = consulta.get("query")

            try:
                pasta_consulta = executar_consulta(conexao, nome_consulta, query, pasta_temp)
                if pasta_consulta:
                    return nome_consulta, pasta_consulta
                else:
                    return nome_consulta, None
            except Exception as e:
                logging.error(f"Erro ao processar consulta '{nome_consulta}': {e}")
                return nome_consulta, None

        # 🔹 Execução paralela ou sequencial
        if paralela:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                futuros = {executor.submit(processa_consulta, consulta): consulta for consulta in consultas}
                for futuro in concurrent.futures.as_completed(futuros):
                    nome_consulta, pasta_consulta = futuro.result()
                    if pasta_consulta:
                        resultados[nome_consulta] = pasta_consulta
        else:
            for consulta in consultas:
                nome_consulta, pasta_consulta = processa_consulta(consulta)
                if pasta_consulta:
                    resultados[nome_consulta] = pasta_consulta

    except Exception as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
    finally:
        if conexao:
            fechar_conexao(conexao)
            logging.info("Conexão com o banco fechada.")

    return resultados  # 🔹 Retorna as pastas finais prontas para envio



def executar_consulta(conexao, nome: str, query: str, pasta_temp: str) -> str:
    """
    Executa uma consulta SQL e retorna os dados como um DataFrame.

    Args:
        conexao: Conexão ativa com o banco de dados.
        nome (str): Nome da consulta.
        query (str): Consulta SQL a ser executada.
        pasta_temp (str): Pasta onde os arquivos serão salvos.

    Returns:
        str: Caminho da pasta final com os arquivos Parquet particionados.
    """
    retries = 5  # Tentativas para lidar com falhas de conexão

    for tentativa in range(retries):
        try:
            logging.info(f"Executando consulta: {nome}...")

            # 🔹 Lê os dados da consulta
            df_pandas = pd.read_sql(query, con=conexao)

            if df_pandas.empty:
                logging.warning(f"Consulta '{nome}' retornou um DataFrame vazio.")
                return ""

            total_registros = len(df_pandas)
            logging.info(f"Consulta '{nome}' finalizada. Total de registros: {total_registros}")

            # 🔹 Chama `processar_dados()` para aplicar tratamentos e salvar os Parquet
            return processar_dados(df_pandas, nome, pasta_temp)

        except OperationalError as e:
            logging.warning(f"Erro de conexão ao executar consulta '{nome}', tentativa {tentativa+1}/{retries}: {e}")
            time.sleep(5)

        except Exception as e:
            logging.error(f"Erro ao executar a consulta '{nome}': {e}")
            return ""

    logging.error(f"Consulta '{nome}' falhou após {retries} tentativas.")
    return ""



def processar_dados(df_pandas: pd.DataFrame, nome: str, pasta_temp: str) -> str:
    """
    Aplica tratamentos e salva os arquivos em Parquet particionado.

    Args:
        df_pandas (pd.DataFrame): DataFrame contendo os dados brutos.
        nome (str): Nome da consulta.
        pasta_temp (str): Pasta final onde os arquivos serão salvos.

    Returns:
        str: Caminho da pasta final com os arquivos Parquet particionados.
    """
    try:
        # 🔹 Garante que a pasta final existe
        os.makedirs(pasta_temp, exist_ok=True)

        # 🔹 Define a pasta onde os arquivos serão salvos
        pasta_consulta = os.path.join(pasta_temp, nome)

        logging.info(f"Processando dados da consulta '{nome}'...")

        # 🔹 Ajuste do tipo TIME para string formatada HH:MM:SS (corrige erro do MySQL)
        if "HoraVenda" in df_pandas.columns:
            if pd.api.types.is_timedelta64_dtype(df_pandas["HoraVenda"]):
                df_pandas["HoraVenda"] = df_pandas["HoraVenda"].apply(lambda x: str(x).split()[-1] if not pd.isna(x) else "00:00:00")
            elif df_pandas["HoraVenda"].dtype == "object":
                df_pandas["HoraVenda"] = df_pandas["HoraVenda"].astype(str).str.extract(r"(\d{2}:\d{2}:\d{2})")[0].fillna("00:00:00")

        # 🔹 Converte para Polars e adiciona colunas extras
        df_polars = pl.from_pandas(df_pandas).with_columns([
            pl.lit(datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%d/%m/%Y %H:%M:%S")).alias("DataHoraAtualizacao"),
            pl.lit(STORAGE_CONFIG["idemp"]).alias("idEmpresa"),
            pl.lit(STORAGE_CONFIG["idemp"]).alias("idEmp")
        ])

        # 🔹 Ajusta tipos de dados
        df_polars = ajustar_tipos_dados(df_polars, nome)

        if 'idEmpresa' not in df_polars.schema:
            raise ValueError("A coluna 'idEmpresa' é obrigatória para particionamento.")

        logging.info(f"Salvando '{nome}' em formato particionado...")

        # 🔹 Salvar particionado por 'idEmpresa'
        pq.write_to_dataset(
            df_polars.to_arrow(),
            root_path=pasta_consulta,
            partition_cols=['idEmpresa'],
            compression="snappy",
            use_dictionary=True,  # Otimiza colunas repetitivas
            row_group_size=500_000  # Ajustável conforme necessidade
            )

        logging.info(f"Salvamento concluído para '{nome}'. Arquivos disponíveis em: {pasta_consulta}")

        return pasta_consulta  # 🔹 Retorna o caminho da pasta final já pronta

    except Exception as e:
        logging.error(f"Erro ao processar dados da consulta '{nome}': {e}")
        return ""
