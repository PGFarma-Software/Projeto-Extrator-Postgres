import json
import logging
import os
import sys

import pandas as pd
import polars as pl


# Dicionário de tipos para consultas específicas
def obter_caminho_dicionario():
    """
    Retorna o caminho do arquivo de dicionários, compatível com execução local ou empacotada.

    Returns:
        str: Caminho absoluto para o arquivo de dicionários.
    """
    # Quando executado com PyInstaller
    if getattr(sys, 'frozen', False):
        # Diretório base onde o executável está rodando
        base_path = sys._MEIPASS  # Atributo definido pelo PyInstaller
    else:
        # Diretório local do código fonte
        base_path = os.path.dirname(os.path.abspath(__file__))

    return os.path.join(base_path, "dicionarios_tipos.json")


def obter_dicionario_tipos(nome_consulta):
    """
    Retorna o dicionário de tipos para uma consulta específica.

    Args:
        nome_consulta (str): Nome da consulta.

    Returns:
        dict: Dicionário de tipos para a consulta, ou None se não encontrado.
    """
    global _dicionarios_cache
    if _dicionarios_cache is None:
        caminho_dicionario = obter_caminho_dicionario()
        if not os.path.exists(caminho_dicionario):
            logging.error(f"Arquivo de dicionários não encontrado: {caminho_dicionario}")
            return None

        try:
            with open(caminho_dicionario, "r", encoding="utf-8") as f:
                _dicionarios_cache = json.load(f)
                logging.info("Dicionário de tipos carregado com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao carregar o arquivo de dicionários: {e}")
            return None

    resultado = _dicionarios_cache.get(nome_consulta)
    if resultado is None:
        logging.warning(f"Nenhum dicionário encontrado para a consulta: {nome_consulta}")
    else:
        logging.info(f"Dicionário carregado para a consulta: {nome_consulta}")
    return resultado


# Cache global para carregar o dicionário apenas uma vez
_dicionarios_cache = None


def ajustar_tipos_dados(dataframe: pl.DataFrame, nome_consulta: str) -> pl.DataFrame:
    """
    Ajusta os tipos de dados do DataFrame com base no dicionário de tipos definido para a consulta.

    Args:
        dataframe (pl.DataFrame): DataFrame a ser ajustado.
        nome_consulta (str): Nome da consulta, para identificar o dicionário de tipos correspondente.

    Returns:
        pl.DataFrame: DataFrame ajustado.
    """
    dicionario = obter_dicionario_tipos(nome_consulta)


    if not dicionario:
        logging.warning(f"[Consulta: {nome_consulta}] Dicionário de tipos não encontrado. Dados serão retornados sem ajuste.")
        return dataframe

    tipo_polars = {
        "string": pl.Utf8,
        "int64": pl.Int64,
        "float64": pl.Float64,
        "date": pl.Date,
        "timestamp": pl.Datetime("ms"),
        "datetime": pl.Datetime
    }
    # 🔹 Conversão automática de float64 para int64 (evita 35 → 35.0 no Parquet)
    for coluna in dataframe.schema:
        if dataframe.schema[coluna] == pl.Float64:
            try:
                # 🔹 Converte para Pandas e garante que valores são float antes de chamar is_integer()
                if dataframe[coluna].drop_nulls().to_pandas().apply(lambda x: float(x).is_integer() if not pd.isna(x) else False).all():
                    dataframe = dataframe.with_columns(pl.col(coluna).cast(pl.Int64))
                    logging.info(f"Coluna '{coluna}' convertida automaticamente de float64 para int64.")
            except Exception as e:
                logging.error(f"Erro ao verificar conversão automática de '{coluna}': {e}")




    for coluna, tipo in dicionario.items():
        if coluna not in dataframe.schema:
            logging.warning(f"Coluna '{coluna}' ausente. Adicionando com valor padrão.")
            valor_padrao = {
                "string": "",
                "int64": 0,
                "float64": 0.0,
                "date": "1970-01-01",
                "timestamp": "1970-01-01T00:00:00.000"
            }.get(tipo, "")
            dataframe = dataframe.with_columns(
                pl.lit(valor_padrao).cast(tipo_polars.get(tipo, pl.Utf8)).alias(coluna)
            )
        else:
            try:
                dataframe = dataframe.with_columns(
                    pl.col(coluna).cast(tipo_polars[tipo])
                )
            except Exception as e:
                logging.error(f"Erro ao ajustar coluna '{coluna}' para tipo '{tipo}': {e}")

    logging.info(f"[Consulta: {nome_consulta}] Tipos ajustados com sucesso.")
    return dataframe
