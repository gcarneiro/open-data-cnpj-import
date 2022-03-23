# Description
Ferramenta para ler os dados de CNPJs liberados pela Receita Federal e armazenar esses dados em um banco Mysql/Mariadb

Requisito: Python 3.6+

Você precisará instalar o pip caso não tenha
```
sudo apt-get install pip
```

Você precisará instalar as dependência do python
```
sudo pip install -r requirements.txt
```


# Usage

## Download

Este processo vai demorar horrores, temos que baixar todos os arquivos da receita federal e descompacta-los. O servidor dos caras é muito lento.
```
python download.py <host> <port> <user> <password> <database>
```

## Import
Você pode importar os dados para um servidor MySQL já existente, ou você pode iniciar um novo utilizando o docker:
```
docker run --name mysql-cnpj -p 3306:3306 -h localhost -e MYSQL_ROOT_PASSWORD=my-secret-pw -e MYSQL_DATABASE=cnpj -d mysql
```
Importe o schema para o seu banco rodando o script create-tables.sql dentro de schema/mysql

Depois de iniciar o servidor MySQL, rode o seguinte comando, alterando para os dados do seu servidor:
```
python mysql_import.py <host> <port> <user> <password> <database>
```
Exemplo:
```
python mysql_import.py localhost 3306 root my-secret-pw cnpj
```

# Schema
The following table describes the defined schema:

Table name | Description
---------- | -------------
arquivos_processado | Tabela de controle utilizada pelo script de download
empresa | Basic company data.
estabelecimento | Detailed company data, each row represents a subsidiary company or the parent company itself.
socio | Basic info about the company partners.
optante_simples | References companies which use Simples Nacional tax regime.
cnae | List of all company activities, references `cnae_principal` and `cnae_secundaria` in `estabelecimento`.
motivo_situacao_cadastral | List of detailed company status info, references `motivo_situacao_cadastral` in `estabelecimento`.
municipio | List of all brazilian cities, references `endereco_codigo_municipio` in `estabelecimento`.
natureza_juridica | List of all company types, references `codigo_natureza_juridica` in `empresa`.
pais | List of countries.
qualificacao_socio | List of partner types, references `codigo_qualificacao` in `socio`.

# Query examples
Get all data from a specific company:
```
SELECT * FROM
    estabelecimento e
        JOIN
    empresa em ON em.id = e.id_empresa
WHERE
    e.cnpj = '00000000000191'
```
