@echo off
:: ================================
:: Script de Empacotamento com PyInstaller para SQL Server
:: ================================

:: Configuração Inicial
set PYTHON_EXECUTABLE=python.exe
set PYTHON_VERSION=3.13
set PYTHON_313_PATH=.venv\Scripts\python.exe
set VENV_DIR=.venv
set BUILD_DIR=build
set DIST_DIR=dist
set SPEC_FILE=pgfarma_Postgres.spec
set MAIN_SCRIPT=main.py
set PROJECT_NAME=pgfarma_Postgres

:: Verificar se o Python 3.13 está disponível
echo Verificando se o Python %PYTHON_VERSION% está instalado...
%PYTHON_313_PATH% --version >nul 2>&1
IF ERRORLEVEL 1 (
    echo ERRO: Python %PYTHON_VERSION% não encontrado no caminho especificado: %PYTHON_313_PATH%.
    echo Por favor, instale o Python %PYTHON_VERSION% ou ajuste o caminho no script.
    pause
    exit /B 1
)

:: Verificar se o ambiente virtual existe
if not exist %VENV_DIR% (
    echo Criando ambiente virtual com Python %PYTHON_VERSION%...
    "%PYTHON_313_PATH%" -m venv %VENV_DIR%
) else (
    echo Ambiente virtual encontrado. Usando o existente...
)

:: Ativar ambiente virtual
echo Ativando ambiente virtual...
call %VENV_DIR%\Scripts\activate

:: Garantir que todas as dependências estão instaladas
if not exist requirements.txt (
    echo ERRO: Arquivo requirements.txt não encontrado.
    deactivate
    pause
    exit /B 1
)
echo Instalando dependências do projeto...
pip install --upgrade pip
pip install pyinstaller
.venv\Scripts\python.exe -m pip install -r requirements.txt

:: Limpar diretórios de build antigos
if exist %BUILD_DIR% (
    echo Removendo pasta de build antiga...
    rmdir /s /q %BUILD_DIR%
)
if exist %DIST_DIR% (
    echo Removendo pasta dist antiga...
    rmdir /s /q %DIST_DIR%
)
if exist %SPEC_FILE% (
    echo Removendo arquivo spec antigo...
    del /q %SPEC_FILE%
)

:: Gerar o executável com PyInstaller
echo Gerando o executável com PyInstaller...
%VENV_DIR%\Scripts\python.exe -m PyInstaller ^
    --clean ^
    --onefile ^
    --noconsole ^
    --add-data "config;config" ^
     --add-data "dicionarios_tipos.json;." ^
    --hidden-import psycopg2-binary ^
    --name "%PROJECT_NAME%" ^
    %MAIN_SCRIPT%

IF ERRORLEVEL 1 (
    echo ERRO: Falha ao gerar o executável.
    deactivate
    pause
    exit /B 1
)

:: Desativar ambiente virtual
echo Desativando o ambiente virtual...
deactivate

echo Processo concluído com sucesso!
echo O executável está localizado no diretório "%DIST_DIR%".
pause
exit /B 0
