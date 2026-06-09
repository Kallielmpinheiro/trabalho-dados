# Metabase Dashboard Project

## Requisitos

Antes de iniciar, certifique-se de que os seguintes softwares estão instalados:

* Docker Desktop (Windows)
* Docker Engine e Docker Compose (Linux)

## Estrutura do Projeto

Após extrair o arquivo compactado, a estrutura de diretórios deve permanecer exatamente como abaixo:

```text
.
├── data/
│   └── gutenberg.db
└── metabase-data/
    └── metabase.db/
        ├── metabase.db.mv.db
        └── metabase.db.trace.db
```

## Executando no Windows

### 1. Extrair os arquivos

Extraia o arquivo compactado para qualquer diretório de sua preferência.

### 2. Abrir o terminal

Abra o Prompt de Comando (CMD) ou PowerShell dentro da pasta que contém os diretórios:

```text
data
metabase-data
```

### 3. Iniciar o Metabase

Execute o comando abaixo:

```cmd
docker run -d ^
--name metabase ^
-p 3003:3000 ^
-e MB_DB_FILE=/metabase-data/metabase.db ^
-v "%cd%\metabase-data:/metabase-data" ^
-v "%cd%\data:/work-data" ^
metabase/metabase:latest
```

### 4. Acessar a aplicação

Abra o navegador e acesse:

```text
http://localhost:3003
```

### 5. Encerrar a aplicação

```cmd
docker stop metabase
```

### 6. Reiniciar a aplicação

```cmd
docker start metabase
```

### 7. Remover a aplicação

```cmd
docker rm -f metabase
```

---

## Executando no Linux

### 1. Extrair os arquivos

Extraia o arquivo compactado para qualquer diretório.

### 2. Abrir o terminal

Entre na pasta que contém os diretórios:

```text
data
metabase-data
```

Exemplo:

```bash
cd caminho/do/projeto
```

### 3. Iniciar o Metabase

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

### 4. Acessar a aplicação

Abra o navegador e acesse:

```text
http://localhost:3003
```

### 5. Encerrar a aplicação

```bash
docker stop metabase
```

### 6. Reiniciar a aplicação

```bash
docker start metabase
```

### 7. Remover a aplicação

```bash
docker rm -f metabase
```

---

## Observações

* Os dashboards, perguntas, coleções e usuários já estão configurados no banco interno do Metabase.
* O banco de dados analisado encontra-se na pasta `data`.
* Não altere a estrutura das pastas após a extração do arquivo compactado.
* Caso já exista um container chamado `metabase`, remova-o antes de executar os comandos apresentados:

### Windows

```cmd
docker rm -f metabase
```

### Linux

```bash
docker rm -f metabase
```

## Credenciais de Acesso

Utilize as credenciais fornecidas separadamente para acessar o sistema.
