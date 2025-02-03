import logging
import os
import sys

from dotenv import load_dotenv


# Função para determinar o caminho do arquivo config.env
def obter_caminho_recurso(caminho_relativo):
    """
    Retorna o caminho absoluto para o arquivo, considerando o contexto de execução.
    - Em execução com PyInstaller, utiliza o diretório temporário.
    - Em execução local, utiliza o diretório atual.
    """
    if getattr(sys, 'frozen', False):  # PyInstaller empacotado
        caminho_base = sys._MEIPASS  # Diretório temporário do PyInstaller
    else:
        caminho_base = os.path.abspath(".")
    return os.path.join(caminho_base, caminho_relativo)


# Carrega as variáveis de ambiente do arquivo config.env
load_dotenv(obter_caminho_recurso("config/config.env"))


# Classe base para configurações com validação
class BaseConfig:
    def _validar(self, variaveis, nomes):
        """Valida se as variáveis essenciais estão configuradas."""
        faltando = [nomes[i] for i, var in enumerate(variaveis) if not var]
        if faltando:
            raise EnvironmentError(f"Variáveis faltando: {', '.join(faltando)}")


class GeneralConfig(BaseConfig):
    def __init__(self):
        """Carrega e valida configurações gerais do sistema."""
        self.num_workers = int(os.getenv("NUM_WORKERS", 4))  # Padrão: 4 workers
        self.qtd_linhas = int(os.getenv("QTD_LINHAS", 10000))  # Padrão: 10000 linhas
        self._validar(
            [self.num_workers, self.qtd_linhas],
            ["NUM_WORKERS", "QTD_LINHAS"]
        )

    def to_dict(self):
        return {
            "num_workers": self.num_workers,
            "qtd_linhas": self.qtd_linhas,
        }


# Classe para configuração do banco de dados
class DatabaseConfig(BaseConfig):
    def __init__(self):
        """Carrega e valida as configurações do banco de dados."""
        self.host = os.getenv("DATABASE_HOST")
        self.port = int(os.getenv("DATABASE_PORT", 3306))  # Padrão: 3306
        self.database = os.getenv("DATABASE_DATABASE")
        self.user = os.getenv("DATABASE_USER")
        self.password = os.getenv("DATABASE_PASSWORD")
        self._validar(
            [self.host, self.port, self.database, self.user, self.password],
            ["DATABASE_HOST", "DATABASE_PORT", "DATABASE_DATABASE", "DATABASE_USER", "DATABASE_PASSWORD"]
        )

    def to_dict(self):
        """Converte a configuração em um dicionário."""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
        }


# Classe para configuração do MongoDB
class MongoConfig(BaseConfig):
    def __init__(self):
        """Carrega e valida as configurações do MongoDB."""
        self.uri = os.getenv("MONGO_URI")
        self.database = os.getenv("MONGO_DATABASE")
        self.collection_empresa = os.getenv("EMPRESA_COLLECTION")
        self.collection_nuvem = os.getenv("NUVEM_COLLECTION")
        self._validar(
            [self.uri, self.database, self.collection_empresa,self.collection_nuvem],
            ["MONGO_URI", "MONGO_DATABASE", "EMPRESA_COLLECTION","NUVEM_COLLECTION"]
        )

    def to_dict(self):
        """Converte a configuração em um dicionário."""
        return {
            "uri": self.uri,
            "database": self.database,
            "collection_empresa": self.collection_empresa,
            "collection_nuvem": self.collection_nuvem,

        }


# Classe para configuração de armazenamento
class StorageConfig(BaseConfig):
    def __init__(self):
        self.destino_tipo = os.getenv("DESTINO_TIPO", "s3").lower().strip()
        self.portal = os.getenv("PORTAL")
        self.idemp = os.getenv("IDEMP")

        # Ajustar valores de destino
        if self.destino_tipo in ["aws", "amazon"]:
            self.destino_tipo = "s3"
        elif self.destino_tipo not in ["s3", "azure", "ambos"]:
            self.destino_tipo = "ambos"  # Valor padrão

        logging.info(f"Valor configurado para DESTINO_TIPO: {self.destino_tipo}")
        self._validar(
            [self.destino_tipo, self.portal, self.idemp],
            ["DESTINO_TIPO", "PORTAL", "IDEMP"]
        )

    def to_dict(self):
        return {
            "destino_tipo": self.destino_tipo,
            "portal": self.portal,
            "idemp": self.idemp,
        }


def configurar_destino_parametros(parametros_mongo_empresas, parametros_mongo_nuvem_cursor):
    """
    Configura os parâmetros de destino (Azure, S3 ou ambos), priorizando os valores do MongoDB.

    Args:
        parametros_mongo_empresas (dict): Parâmetros obtidos do MongoDB (empresa).
        parametros_mongo_nuvem_cursor (Cursor or list): Cursor ou lista de documentos com os parâmetros de nuvem.

    Returns:
        dict: Destino(s) configurado(s), portal, e configurações específicas do(s) destino(s).
    """
    # Verificar se parametros_mongo_nuvem_cursor é uma lista ou um cursor
    if not isinstance(parametros_mongo_nuvem_cursor, list):
        parametros_mongo_nuvem = list(parametros_mongo_nuvem_cursor)
    else:
        parametros_mongo_nuvem = parametros_mongo_nuvem_cursor

    if not parametros_mongo_nuvem:
        raise ValueError("Nenhum dado encontrado em parametros_mongo_nuvem.")

    # Recuperar configurações principais da empresa
    destino_tipo = parametros_mongo_empresas.get("TipoDestino", "ambos").strip().lower()
    portal = parametros_mongo_empresas.get("portal", "pgfarma")

    # Inicializar as configurações dos destinos
    azure_config = None
    s3_config = None

   # Processar os documentos da nuvem para configurar Azure e S3
    for doc in parametros_mongo_nuvem:
        destino = doc.get("Destino", {})  # Captura o campo 'Destino'

        if "azure" in destino:
            azure_config = {
                "account_name": destino["azure"].get("NomeConta"),
                "account_key": destino["azure"].get("ChaveConta"),
                "container_name": destino["azure"].get("NomeContainer")
            }

        if "s3" in destino:
            s3_config = {
                "access_key": destino["s3"].get("ChaveAcesso"),
                "secret_key": destino["s3"].get("ChaveSecreta"),
                "bucket": destino["s3"].get("Bucket"),
                "region": destino["s3"].get("Regiao")
            }
    # Configuração final com base no tipo de destino
    if destino_tipo == "azure":
        return "azure", portal, {
            "azure": {
                "account_name": destino["azure"].get("NomeConta"),
                "account_key": destino["azure"].get("ChaveConta"),
                "container_name": destino["azure"].get("NomeContainer")
            }
        }
    elif destino_tipo == "s3":
        return "s3", portal, {
            "s3": {
                "access_key": destino["s3"].get("ChaveAcesso"),
                "secret_key": destino["s3"].get("ChaveSecreta"),
                "bucket": destino["s3"].get("Bucket").replace("s3://", "").split("/")[0],
                "region": destino["s3"].get("Regiao")            }
        }
    else:
        return "ambos", portal, {
            "azure": {
                "account_name": destino["azure"].get("NomeConta"),
                "account_key": destino["azure"].get("ChaveConta"),
                "container_name": destino["azure"].get("NomeContainer")
            },
            "s3": {
                "access_key": destino["s3"].get("ChaveAcesso"),
                "secret_key": destino["s3"].get("ChaveSecreta"),
                "bucket": destino["s3"].get("Bucket").replace("s3://", "").split("/")[0],
                "region": destino["s3"].get("Regiao")
            }
        }

def configurar_conexao_banco(parametros_mongo: dict) -> dict:
    """
    Configura os parâmetros de conexão com o banco de dados, priorizando os valores do MongoDB.

    Args:
        parametros_mongo (dict): Parâmetros obtidos do MongoDB.

    Returns:
        dict: Configuração completa de conexão com o banco de dados.
    """
    logging.info("Configurando conexão com o banco de dados a partir dos parâmetros.")
    return {
        "host": parametros_mongo.get("ConexaoBanco", {}).get("host", DATABASE_CONFIG["host"]),
        "port": int(parametros_mongo.get("ConexaoBanco", {}).get("porta", DATABASE_CONFIG["port"])),
        "database": parametros_mongo.get("ConexaoBanco", {}).get("nomeOuCaminhoBanco", DATABASE_CONFIG["database"]),
        "user": parametros_mongo.get("ConexaoBanco", {}).get("usuario", DATABASE_CONFIG["user"]),
        "password": parametros_mongo.get("ConexaoBanco", {}).get("senha", DATABASE_CONFIG["password"]),
    }

def configurar_parametro_qtd_linha(parametros_mongo: dict) -> int:
    """
    Configura os parâmetros de configuração de chunk para as consultas, priorizando os valores do MongoDB.

    Args:
        parametros_mongo (dict): Parâmetros obtidos do MongoDB.

    Returns:
        int: valor do parametro
    """
    logging.info("Configurando quantidade de linhas por consulta a partir dos parâmetros.")
    return parametros_mongo.get("QuantidadeLinhas", 10000)

def configurar_parametro_workers(parametros_mongo: dict) -> int:
    """
    Configura os parâmetros de configuração de threads, priorizando os valores do MongoDB.

    Args:
        parametros_mongo (dict): Parâmetros obtidos do MongoDB.

    Returns:
        int: valor do parametro
    """
    logging.info("Configurando numero de workers a partir dos parâmetros.")
    return parametros_mongo.get("Workers", 4)

def obter_diretorio_temporario():
    """
    Obtém o diretório temporário no mesmo diretório do executável ou do projeto.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    temp_dir = os.path.join(base_dir, "temporario")
    os.makedirs(temp_dir, exist_ok=True)
    return temp_dir



# Inicialização e validação das configurações
try:
    # Instanciar e validar configurações
    database_config = DatabaseConfig()
    mongo_config = MongoConfig()
    storage_config = StorageConfig()
    general_config = GeneralConfig()

    # Exportar configurações como dicionários
    DATABASE_CONFIG = database_config.to_dict()
    MONGO_CONFIG = mongo_config.to_dict()
    STORAGE_CONFIG = storage_config.to_dict()
    GENERAL_CONFIG = general_config.to_dict()

    logging.info("Configurações carregadas com sucesso.")
except EnvironmentError as e:
    # Log de erro em caso de falha na configuração
    logging.basicConfig(filename="registro.log", level=logging.ERROR, encoding="utf-8")
    logging.error(str(e))
    sys.exit(1)
