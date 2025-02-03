import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from rich.logging import RichHandler
from rich.console import Console
from rich.panel import Panel
from rich.traceback import install as rich_traceback

# Instalação do rastreamento detalhado de erros com Rich
rich_traceback(show_locals=True)


class LoggingConfigurator:
    """
    Classe responsável por configurar o sistema de logging.
    Fornece logs detalhados no console e arquivos rotativos para histórico.
    """

    def __init__(self, base_log_dir="logs", log_level=logging.INFO):
        """
        Inicializa a configuração de logging.

        Args:
            base_log_dir (str): Diretório onde os logs serão armazenados.
            log_level (int): Nível do log (default: logging.INFO).
        """
        self.console = Console()
        self.log_level = log_level
        self.log_format = "%(message)s"
        self.file_log_format = "%(asctime)s - %(levelname)-8s - [%(filename)s:%(lineno)d] %(message)s"
        self.is_frozen = getattr(sys, 'frozen', False)  # Detecta se está empacotado com PyInstaller

        # Determina o diretório de logs correto
        self.base_log_dir = base_log_dir if not self.is_frozen else os.path.dirname(sys.executable)
        os.makedirs(self.base_log_dir, exist_ok=True)

    def _get_log_filepath(self, filename):
        """Retorna o caminho completo do arquivo de log."""
        return os.path.join(self.base_log_dir, filename)

    def _configurar_arquivo_log(self, logger):
        """
        Configura um arquivo de log rotativo para armazenar logs persistentes.
        """
        log_file_path = self._get_log_filepath("registro.log")
        file_handler = RotatingFileHandler(
            log_file_path,
            maxBytes=10 * 1024 * 1024,  # 10 MB antes de girar o log
            backupCount=5  # Mantém até 5 logs antigos
        )
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(logging.Formatter(self.file_log_format))
        logger.addHandler(file_handler)

    def _configurar_console_log(self, logger):
        """
        Configura a saída de log para o console usando Rich.
        """
        console_handler = RichHandler(
            console=self.console,
            rich_tracebacks=True,  # Exibir rastreamentos bonitos
            show_time=True,        # Mostrar timestamps no console
            show_level=True,       # Mostrar níveis de log
            show_path=False        # Não mostrar caminho do arquivo no console
        )
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(logging.Formatter(self.log_format))
        logger.addHandler(console_handler)

    def configurar_logging(self):
        """
        Configura o sistema de logging geral (console + arquivo).
        """
        logger = logging.getLogger()
        logger.setLevel(self.log_level)

        if logger.hasHandlers():
            logger.handlers.clear()  # Limpa manipuladores antigos

        # Configuração de log para arquivo e console
        self._configurar_arquivo_log(logger)
        self._configurar_console_log(logger)

        # Determina o modo de execução
        modo_execucao = "empacotado (PyInstaller)" if self.is_frozen else "local"
        self.console.print(Panel(f"[bold cyan]Modo de execução: {modo_execucao}[/bold cyan]"))

        # Mensagem inicial estilizada
        self.console.print(Panel("[bold green] Sistema de logging configurado com sucesso![/bold green]"))
        logging.info(f" Sistema iniciado no modo: {modo_execucao}")


# Exemplo de uso
if __name__ == "__main__":
    configurador = LoggingConfigurator()
    configurador.configurar_logging()

    # Exemplo de logs
    logging.info(" Sistema inicializado com sucesso.")
    logging.warning("⚠ Aviso: Verifique os parâmetros configurados.")
    logging.error(" Erro crítico detectado!")

    # Captura de erro com traceback detalhado
    try:
        1 / 0
    except ZeroDivisionError:
        logging.exception("🔥 Erro de divisão por zero capturado.")
