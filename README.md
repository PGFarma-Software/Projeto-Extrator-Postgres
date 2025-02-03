# Projeto Extrator PostgreSQL

## Descrição
Este projeto extrai dados para integrar clientes do **PGFarma** e seus whitelabels que utilizam bancos de dados **PostgreSQL**.

## Requisitos
Para utilizar este projeto corretamente, é necessário atender aos seguintes requisitos, pois garantem a compatibilidade do ambiente de execução e evitam erros durante o processamento dos dados.

### Ambiente Python
- **Python 3.13** (recomendado o uso de um ambiente virtual dedicado).
- Instalação das dependências necessárias via `pip install -r requirements.txt`.

### Banco de Dados
- **Não é necessário ter o PostgreSQL instalado na máquina.**

## Configuração e Uso
1. Clone este repositório:
   ```sh
   git clone https://github.com/PGFarma-Software/Projeto-Extrator-PostgreSQL.git
   ```
2. Acesse o diretório do projeto:
   ```sh
   cd Projeto-Extrator-PostgreSQL
   ```
3. Crie e ative um ambiente virtual:
   ```sh
   python3.13 -m venv venv
   source venv/bin/activate  # Linux/macOS
   venv\Scripts\activate  # Windows
   ```
4. Instale as dependências do projeto:
   ```sh
   pip install -r requirements.txt
   ```
5. Certifique-se de que as configurações de conexão com o banco PostgreSQL estão corretas.
6. Execute o extrator conforme a documentação do projeto.

## Uso Portátil
Para rodar o projeto sem a necessidade de instalação do Python na máquina de destino, utilize a opção de execução portátil, especialmente útil em ambientes restritos ou onde não há acesso à instalação de dependências.
1. Execute o script `criar_executavel.bat`, incluso no projeto.
2. O executável empacotado pelo **PyInstaller** será gerado.
3. Utilize o arquivo `.exe` gerado para rodar o extrator em qualquer máquina sem necessidade de instalar o Python.

## Contribuição
Contribuições são bem-vindas! Caso queira sugerir melhorias ou reportar problemas, abra uma [issue](https://github.com/PGFarma-Software/Projeto-Extrator-Postgres/issues) ou envie um pull request.

## Licença
Este projeto está licenciado sob a [MIT License](LICENSE).

