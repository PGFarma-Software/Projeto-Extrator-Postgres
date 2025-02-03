import logging
from bson import ObjectId
from pymongo import MongoClient


class MongoDBConnector:
    """
    Classe para gerenciar conexões e interações com o MongoDB.
    """

    def __init__(self, uri, database, collection):
        """
        Inicializa o conector do MongoDB com os parâmetros fornecidos.

        Args:
            uri (str): URI de conexão com o MongoDB.
            database (str): Nome do banco de dados a ser utilizado.
            collection (str): Nome da coleção a ser acessada.
        """
        self.uri = uri
        self.database_name = database
        self.collection_name = collection

    def conectar(self):
        """
        Retorna a conexão com o MongoDB dentro de um context manager.
        """
        return MongoClient(self.uri)

    def obter_parametros_empresa(self, idEmp):
        """
        Obtém os parâmetros de configuração para o cliente especificado.

        Args:
            idEmp (str): HASH do cliente para busca no MongoDB.

        Returns:
            dict: Documento correspondente aos parâmetros do cliente, ou None se não encontrado.
        """
        try:
            with self.conectar() as client:
                logging.info("Conectando ao MongoDB...")
                database = client[self.database_name]
                collection = database[self.collection_name]

                parametros = collection.find_one({"_id": ObjectId(idEmp)})
                if not parametros:
                    logging.warning(f"Cliente '{idEmp}' não encontrado no MongoDB.")
                    return None

                # Validação de chaves essenciais
                campos_essenciais = ["ConexaoBanco", "parametrizacaoBi"]
                for campo in campos_essenciais:
                    if campo not in parametros:
                        logging.error(f"Parâmetro '{campo}' ausente para o cliente '{idEmp}' no MongoDB.")
                        return None

                nome_cliente = parametros.get("name", "Desconhecido")
                logging.info(f"Parâmetros obtidos com sucesso do MongoDB para o cliente '{nome_cliente}'.")
                return parametros

        except Exception as e:
            logging.error(f"Erro ao buscar parâmetros no MongoDB: {e}")
            raise

    def obter_parametros_nuvem(self):
        """
        Obtém todos os documentos na collection para os parâmetros de configuração.

        Returns:
            list: Lista de documentos correspondentes aos parâmetros, ou lista vazia se nenhum for encontrado.
        """
        try:
            with self.conectar() as client:
                logging.info("Conectando ao MongoDB...")
                database = client[self.database_name]
                collection = database[self.collection_name]

                parametros_cursor = collection.find()
                parametros = list(parametros_cursor)

                if not parametros:
                    logging.warning(f"Parâmetros de destino não encontrados no MongoDB.")
                    return []

                logging.info(f"Parâmetros obtidos com sucesso do MongoDB: {len(parametros)} documentos encontrados.")
                return parametros

        except Exception as e:
            logging.error(f"Erro ao buscar parâmetros no MongoDB: {e}")
            raise
