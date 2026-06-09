# Metabase - Projeto de Visualização de Dados

## Sobre o Projeto

Este repositório contém os arquivos necessários para executar localmente a instância do Metabase utilizada no projeto, incluindo:

* Banco de dados utilizado nas análises (`data`)
* Banco interno do Metabase com dashboards, perguntas, coleções e usuários (`metabase-data`)

O arquivo compactado `metabase.rar` encontra-se na raiz deste repositório.

---

# Requisitos

## Windows

* Docker Desktop instalado e em execução

## Linux

* Docker Engine instalado e em execução

---

# Estrutura Esperada

Após extrair o arquivo `metabase.rar`, a estrutura deve permanecer exatamente como abaixo:

```text
metabase/
├── data/
└── metabase-data/
    └── metabase.db/
        ├── metabase.db.mv.db
        └── metabase.db.trace.db
```

---

# Execução no Windows

## 1. Extrair os arquivos

Extraia o conteúdo do arquivo `metabase.rar`.

## 2. Abrir o terminal

Abra o Prompt de Comando (CMD) dentro da pasta `metabase`.

A pasta atual deve conter:

```text
data
metabase-data
```

## 3. Iniciar o Metabase

Execute:

```cmd
docker run -d ^
--name metabase ^
-p 3003:3000 ^
-e MB_DB_FILE=/metabase-data/metabase.db ^
-v "%cd%\metabase-data:/metabase-data" ^
-v "%cd%\data:/work-data" ^
metabase/metabase:latest
```

## 4. Acessar a aplicação

Abra o navegador e acesse:

```text
http://localhost:3003
```

---

# Execução no Linux

## 1. Extrair os arquivos

Extraia o conteúdo do arquivo `metabase.rar`.

## 2. Abrir o terminal

Entre na pasta `metabase`.

A pasta atual deve conter:

```text
data
metabase-data
```

## 3. Iniciar o Metabase

Execute:

```bash
docker run -d \
--name metabase \
-p 3003:3000 \
-e MB_DB_FILE=/metabase-data/metabase.db \
-v "$(pwd)/metabase-data:/metabase-data" \
-v "$(pwd)/data:/work-data" \
metabase/metabase:latest
```

## 4. Acessar a aplicação

Abra o navegador e acesse:

```text
http://localhost:3003
```

---

# Credenciais de Acesso

Utilize as seguintes credenciais:

```text
E-mail: fatec@gmail.com
Senha: 192837Aa
```

---

# Comandos Úteis

## Parar o Metabase

### Windows e Linux

```bash
docker stop metabase
```

## Iniciar novamente

### Windows e Linux

```bash
docker start metabase
```

## Remover o container

### Windows e Linux

```bash
docker rm -f metabase
```

---

# Observações

* Os dashboards, perguntas, coleções e filtros já estão configurados.
* O banco de dados utilizado nas análises encontra-se na pasta `data`.
* Não altere a estrutura das pastas após a extração do arquivo.
* Caso já exista um container chamado `metabase`, remova-o antes da execução:

```bash
docker rm -f metabase
```

* O projeto foi configurado para utilizar caminhos relativos, permitindo sua execução em diferentes máquinas sem necessidade de alterar diretórios ou caminhos absolutos.
