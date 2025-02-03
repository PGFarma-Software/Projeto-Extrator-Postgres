import logging
import os
import shutil
import sys
import threading
from datetime import datetime

import polars as pl
import pyarrow.parquet as pq
import pytz

from config import (
    MONGO_CONFIG, STORAGE_CONFIG, configurar_destino_parametros,
    configurar_conexao_banco, configurar_parametro_workers,
    configurar_parametro_qtd_linha
)
from database import executar_consultas
from dicionario_dados import ajustar_tipos_dados
from logging_config import LoggingConfigurator
from mongo import MongoDBConnector
from storage import enviar_resultados

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

def enviar_tabela_atualizacao(portal, destino_tipo, destino_config, consultas_status, workers, inicio_processo):
    """
    Cria e envia a tabela de atualização para os destinos configurados.
    Somente será enviada se todas as consultas forem bem-sucedidas.

    Args:
        portal (str): Caminho base no destino.
        destino_tipo (str): Tipo de destino ("azure", "s3" ou "ambos").
        destino_config (dict): Configurações dos destinos.
        consultas_status (dict): Dicionário com o status de cada consulta (`True` se enviada com sucesso).
        workers (int): Número de threads para envio.
        inicio_processo (str): Timestamp do início do processo.
    """
    try:
        # 🔹 Verificar se TODAS as consultas foram bem-sucedidas
        if not consultas_status or not all(consultas_status.values()) or len(consultas_status) != 8:
            logging.info(
                "A tabela de atualização NÃO será enviada, pois nem todas as consultas foram processadas com sucesso.\n"
                "ou a opção de consulta unica foi selecionada."
            )
            return

        logging.info("Criando tabela de atualização...")

        df_atualizacao = pl.DataFrame({
            "DataHoraAtualizacao": [inicio_processo],
            "idEmp": [STORAGE_CONFIG["idemp"]],
            "idEmpresa": [STORAGE_CONFIG["idemp"]]
        })

        # 🔹 Ajustar tipos de dados antes de salvar
        df_atualizacao = ajustar_tipos_dados(df_atualizacao, "Atualizacao")

        # 🔹 Criar diretório temporário para salvar a tabela de atualização
        temp_dir = os.path.join("temp", "Atualizacao")
        os.makedirs(temp_dir, exist_ok=True)

        # 🔹 Salvar a tabela de atualização no formato Parquet
        pq.write_to_dataset(
            df_atualizacao.to_arrow(),
            root_path=temp_dir,
            partition_cols=['idEmpresa']
        )

        logging.info("Tabela de atualização criada. Iniciando envio...")

        # 🔹 Enviar a tabela de atualização para o destino
        sucesso_envio = enviar_resultados(temp_dir, portal, destino_tipo, destino_config, workers=workers,nome_consulta="Atualizacao")

        # 🔹 Verificar se o envio foi bem-sucedido
        if sucesso_envio:
            logging.info("Tabela de atualização enviada com sucesso!")
        else:
            logging.error("Falha no envio da tabela de atualização!")

    except Exception as e:
        logging.error(f"Erro ao enviar tabela de atualização: {e}")


def main():
    """
    Função principal que orquestra o fluxo de execução do sistema.
    """
    try:
        configurador = LoggingConfigurator()
        configurador.configurar_logging()
        timezone = pytz.timezone("America/Sao_Paulo")
        inicio_processo = datetime.now(timezone).strftime("%d/%m/%Y %H:%M:%S")
        logging.info(f"Sistema iniciado às {inicio_processo}")
        if os.path.exists("temp"):
            shutil.rmtree("temp")
            logging.info("Diretório temporário removido.")

        # Conectar ao MongoDB
        cliente_mongo_empresa = MongoDBConnector(MONGO_CONFIG["uri"], MONGO_CONFIG["database"],
                                                 MONGO_CONFIG["collection_empresa"])
        cliente_mongo_nuvem = MongoDBConnector(MONGO_CONFIG["uri"], MONGO_CONFIG["database"],
                                               MONGO_CONFIG["collection_nuvem"])

        parametros_mongo_empresa = cliente_mongo_empresa.obter_parametros_empresa(STORAGE_CONFIG["idemp"])
        parametros_mongo_nuvem = cliente_mongo_nuvem.obter_parametros_nuvem()

        if not parametros_mongo_empresa or not parametros_mongo_nuvem:
            logging.error("Parâmetros do MongoDB não encontrados. Execução abortada.")
            return

        # Configurar destino e banco
        destino_tipo, portal, destino_config = configurar_destino_parametros(parametros_mongo_empresa,
                                                                             parametros_mongo_nuvem)
        conexoes_config = configurar_conexao_banco(parametros_mongo_empresa)

        qtd_linhas = configurar_parametro_qtd_linha(parametros_mongo_empresa)
        workers = configurar_parametro_workers(parametros_mongo_empresa)

        # 🔹 Capturar corretamente container e bucket baseado na estrutura do destino_config
        container = destino_config.get("azure", {}).get("container_name", "Desconhecido")
        bucket = destino_config.get("s3", {}).get("bucket", "Desconhecido")

        # 🔹 Log detalhado do destino
        if destino_tipo == "azure":
            logging.info(f"Os dados serão enviados para **Azure Blob Storage** no container '{container}'.")
        elif destino_tipo == "s3":
            logging.info(f"Os dados serão enviados para **Amazon S3** no bucket '{bucket}'.")
        elif destino_tipo == "ambos":
            logging.info(
                f"Os dados serão enviados para **Azure Blob Storage** no container '{container}' e para **Amazon S3** no bucket '{bucket}'.")

        logging.info(f"Executando com {workers} threads.")

        consultas = parametros_mongo_empresa.get("parametrizacaoBi", {}).get("consultas", [])
        if not consultas:
            logging.error("Nenhuma consulta configurada.")
            return

        consulta_desejada = ""  # Defina um nome para filtrar uma consulta específica
        if consulta_desejada:
            consultas = [c for c in consultas if c.get("name") == consulta_desejada]
            if not consultas:
                logging.warning(f"Consulta '{consulta_desejada}' não encontrada. Executando todas as consultas.")
                consultas = parametros_mongo_empresa.get("parametrizacaoBi", {}).get("consultas", [])

        if not consultas:
            logging.error("Nenhuma consulta válida encontrada.")
            return

        pasta_temp = "temp"
        os.makedirs(pasta_temp, exist_ok=True)

        # Executar consultas e obter as pastas com os dados processados
        pastas_resultados, particoes_utilizadas = executar_consultas(
            conexoes_config, consultas, pasta_temp, paralela=True, workers=workers
        )

        # Remover espaços dos nomes das consultas para evitar problemas na manipulação
        pastas_resultados = {
            nome.replace(" ", ""): caminho
            for nome, caminho in pastas_resultados.items()
            if caminho
        }

        if not pastas_resultados:
            logging.error("Nenhuma consulta gerou resultados válidos. Nenhum dado será enviado.")
            return

        # Exibir as consultas e partições que serão enviadas
        logging.info("Resumo das consultas concluídas:")
        for nome_consulta, pasta_consulta in pastas_resultados.items():
            logging.info(f" - {nome_consulta}: {pasta_consulta}")

        # Dicionário para armazenar o status do envio das consultas
        consultas_status = {}

        for nome_consulta, pasta_consulta in pastas_resultados.items():
            try:
                logging.info(f"Iniciando envio da consulta '{nome_consulta}'...")

                sucesso = enviar_resultados(
                    pasta_consulta, portal, destino_tipo, destino_config, workers,nome_consulta
                )

                consultas_status[nome_consulta] = sucesso

                if sucesso:
                    logging.info(f"Consulta '{nome_consulta}' enviada com sucesso.")
                else:
                    logging.error(f"Falha no envio da consulta '{nome_consulta}'.")

            except Exception as e:
                consultas_status[nome_consulta] = False
                logging.error(f"Erro inesperado ao enviar consulta '{nome_consulta}': {e}")

        # Exibir um resumo final do envio
        sucessos = sum(1 for v in consultas_status.values() if v)  # Conta apenas os True
        falhas = len(consultas_status) - sucessos

        logging.info(f"Resumo do envio: {sucessos} consultas enviadas com sucesso, {falhas} falhas.")

        if falhas > 0:
            logging.error("Nem todas as consultas foram enviadas corretamente. A tabela de atualização não será gerada.")
            return

        logging.info("Todas as consultas foram enviadas com sucesso. Prosseguindo com a tabela de atualização...")

        # Enviar tabela de atualização apenas se todas as consultas foram bem-sucedidas
        enviar_tabela_atualizacao(portal, destino_tipo, destino_config, consultas_status, workers, inicio_processo)


    except Exception as e:
        logging.error(f"Erro na execução principal: {e}")
        raise

    finally:
        for thread in threading.enumerate():
            if thread is not threading.main_thread():
                logging.info(f"Aguardando thread {thread.name} encerrar...")
                thread.join(timeout=5)

    if os.path.exists("temp"):
        shutil.rmtree("temp")
        logging.info("Diretório temporário removido.")

    encerramento_processo = datetime.now(timezone).strftime("%d/%m/%Y %H:%M:%S")
    logging.info(f"Sistema encerrado às {encerramento_processo}")


if __name__ == "__main__":
    main()
