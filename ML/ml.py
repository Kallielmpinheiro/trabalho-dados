"""
Mineração de Dados no Projeto Gutenberg - Fase 2
Técnica de Aprendizado de Máquina aplicada ao acervo coletado.

Objetivo: prever se um livro pertence ao idioma inglês (classificação binária
supervisionada) a partir de seus atributos numéricos, categóricos e relacionais,
utilizando Regressão Logística.

Para preservar a integridade dos dados originais, toda a leitura é feita sobre
uma CÓPIA temporária do banco SQLite; o arquivo original nunca é aberto pela
mineração.
"""

import argparse
import os
import shutil
import sqlite3
import tempfile
import warnings

import matplotlib

matplotlib.use("Agg")  # backend sem interface gráfica: salva os gráficos em arquivo
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    ConfusionMatrixDisplay,
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    roc_auc_score,
    roc_curve,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

warnings.filterwarnings("ignore")
RANDOM_STATE = 42
DIR_GRAFICOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "graficos")



def criar_copia_bd(caminho_db: str) -> str:
    """
    Cria uma cópia temporária do banco SQLite e devolve o caminho dela.

    A mineração trabalha somente sobre essa cópia, garantindo que o banco
    original gerado na fase 1 nunca seja modificado nem corrompido.
    """
    if not os.path.exists(caminho_db):
        raise FileNotFoundError(
            f"Banco não encontrado em '{caminho_db}'. "
            "Informe o caminho correto com --db."
        )

    fd, caminho_copia = tempfile.mkstemp(prefix="gutenberg_copia_", suffix=".db")
    os.close(fd)
    shutil.copy2(caminho_db, caminho_copia)
    print(f"Cópia temporária do banco criada em: {caminho_copia}")
    return caminho_copia



def carregar_dados(caminho_db: str) -> pd.DataFrame:
    """
    Lê o banco SQLite relacional e monta um DataFrame consolidado no nível de
    livro, agregando as relações de autores e assuntos.
    """
    conn = sqlite3.connect(caminho_db)

    books = pd.read_sql_query(
        """
        SELECT id,
               title,
               quantity_downloads,
               reading_level,
               release_date,
               language,
               category,
               summary
        FROM books
        """,
        conn,
    )

    n_authors = pd.read_sql_query(
        """
        SELECT book_id AS id, COUNT(author_id) AS n_authors
        FROM book_authors
        GROUP BY book_id
        """,
        conn,
    )

    n_subjects = pd.read_sql_query(
        """
        SELECT book_id AS id, COUNT(subject_id) AS n_subjects
        FROM book_subjects
        GROUP BY book_id
        """,
        conn,
    )

    conn.close()

    df = books.merge(n_authors, on="id", how="left")
    df = df.merge(n_subjects, on="id", how="left")
    df["n_authors"] = df["n_authors"].fillna(0).astype(int)
    df["n_subjects"] = df["n_subjects"].fillna(0).astype(int)

    return df



def preparar_features(df: pd.DataFrame):
    """
    Cria a variável alvo (is_english) e deriva as features usadas no modelo.
    Retorna (X, y, features_num, features_cat) prontos para o pipeline.
    """
    df = df.copy()

    # Variável alvo: 1 = inglês, 0 = outro idioma.
    # O banco armazena o idioma por extenso ("English", "German", ...),
    # por isso a comparação é feita com "english".
    df["is_english"] = (
        df["language"].fillna("").str.lower().str.strip() == "english"
    ).astype(int)

    df["release_year"] = pd.to_datetime(df["release_date"], errors="coerce").dt.year

    downloads = (
        df["quantity_downloads"]
        .astype(str)
        .str.replace(r"[^\d]", "", regex=True)
        .replace("", np.nan)
        .astype(float)
    )


    df["log_downloads"] = np.log1p(downloads.fillna(0))
    df["title_length"] = df["title"].fillna("").str.len()
    df["summary_length"] = df["summary"].fillna("").str.len()
    df = df.dropna(subset=["reading_level", "release_year"])

    features_num = [
        "reading_level",
        "log_downloads",
        "release_year",
        "title_length",
        "summary_length",
        "n_authors",
        "n_subjects",
    ]
    features_cat = ["category"]

    X = df[features_num + features_cat]
    y = df["is_english"]

    return X, y, features_num, features_cat


def construir_preprocessador(features_num, features_cat) -> ColumnTransformer:
    """
    Padroniza as variáveis numéricas e aplica one-hot nas categóricas.
    """
    return ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), features_num),
            (
                "cat",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                features_cat,
            ),
        ]
    )


def construir_modelo() -> LogisticRegression:
    """Devolve o classificador de Regressão Logística configurado."""
    return LogisticRegression(
        max_iter=1000,
        class_weight="balanced",  # compensa o desbalanceamento (~95% inglês)
        random_state=RANDOM_STATE,
    )


def treinar_avaliar(X, y, features_num, features_cat):
    """
    Treina e avalia a Regressão Logística, imprime o relatório e devolve:
        - pipeline treinado
        - X_test, y_test (para gerar os gráficos)
    """
    preprocessador = construir_preprocessador(features_num, features_cat)

    # Divisão estratificada para preservar a proporção de classes
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, stratify=y, random_state=RANDOM_STATE
    )

    modelo = Pipeline(steps=[("prep", preprocessador), ("clf", construir_modelo())])
    modelo.fit(X_train, y_train)

    y_pred = modelo.predict(X_test)
    y_proba = modelo.predict_proba(X_test)[:, 1]

    acuracia = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    auc = roc_auc_score(y_test, y_proba)

    print("\n" + "=" * 60)
    print("Regressão Logística (previsão: idioma inglês)")
    print("=" * 60)
    print(f"Acurácia : {acuracia:.4f}")
    print(f"F1-Score : {f1:.4f}")
    print(f"ROC-AUC  : {auc:.4f}")
    print("\nMatriz de confusão:")
    print(confusion_matrix(y_test, y_pred))
    print("\nRelatório de classificação:")
    print(
        classification_report(
            y_test, y_pred, target_names=["Outro idioma", "Inglês"], zero_division=0
        )
    )

    return modelo, X_test, y_test


def interpretar_coeficientes(modelo):
    """
    Influência de cada atributo na probabilidade de o livro ser inglês,
    segundo a Regressão Logística. Coeficiente positivo -> aumenta a chance.
    """
    nomes = modelo.named_steps["prep"].get_feature_names_out()
    coefs = modelo.named_steps["clf"].coef_[0]

    importancia = (
        pd.DataFrame({"atributo": nomes, "coeficiente": coefs})
        .assign(abs_coef=lambda d: d["coeficiente"].abs())
        .sort_values("abs_coef", ascending=False)
        .drop(columns="abs_coef")
    )

    print("\nInfluência dos atributos (coeficientes da Regressão Logística):")
    print(importancia.to_string(index=False))



def _salvar(fig, dir_saida: str, nome_arquivo: str) -> str:
    """Salva a figura como PNG, fecha-a e devolve o caminho."""
    os.makedirs(dir_saida, exist_ok=True)
    caminho = os.path.join(dir_saida, nome_arquivo)
    fig.tight_layout()
    fig.savefig(caminho, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  gráfico salvo: {caminho}")
    return caminho


def plot_distribuicao_alvo(y, dir_saida: str):
    """Gráfico de barras com a distribuição da variável alvo (idioma)."""
    contagem = y.value_counts().sort_index()
    rotulos = ["Outro idioma", "Inglês"]
    fig, ax = plt.subplots(figsize=(6, 4))
    barras = ax.bar(rotulos, contagem.values, color=["#d97706", "#2563eb"])
    ax.bar_label(barras, padding=3)
    ax.set_title("Distribuição da variável alvo (idioma)")
    ax.set_ylabel("Quantidade de livros")
    _salvar(fig, dir_saida, "01_distribuicao_alvo.png")


def plot_curva_roc(modelo, X_test, y_test, dir_saida: str):
    """Curva ROC da Regressão Logística."""
    y_proba = modelo.predict_proba(X_test)[:, 1]
    fpr, tpr, _ = roc_curve(y_test, y_proba)
    auc = roc_auc_score(y_test, y_proba)

    fig, ax = plt.subplots(figsize=(7, 6))
    ax.plot(fpr, tpr, color="#2563eb", label=f"Regressão Logística (AUC={auc:.2f})")
    ax.plot([0, 1], [0, 1], "k--", alpha=0.5, label="Aleatório")
    ax.set_title("Curva ROC")
    ax.set_xlabel("Taxa de falsos positivos")
    ax.set_ylabel("Taxa de verdadeiros positivos")
    ax.legend(loc="lower right", fontsize=8)
    _salvar(fig, dir_saida, "02_curva_roc.png")


def plot_matriz_confusao(modelo, X_test, y_test, dir_saida: str):
    """Matriz de confusão da Regressão Logística."""
    fig, ax = plt.subplots(figsize=(5, 4.5))
    ConfusionMatrixDisplay.from_estimator(
        modelo,
        X_test,
        y_test,
        display_labels=["Outro", "Inglês"],
        cmap="Blues",
        colorbar=False,
        ax=ax,
    )
    ax.set_title("Matriz de confusão - Regressão Logística")
    _salvar(fig, dir_saida, "03_matriz_confusao.png")


def plot_coeficientes(modelo, dir_saida: str):
    """Barras horizontais com os coeficientes da Regressão Logística."""
    nomes = modelo.named_steps["prep"].get_feature_names_out()
    coefs = modelo.named_steps["clf"].coef_[0]
    ordem = np.argsort(coefs)
    cores = ["#dc2626" if c < 0 else "#2563eb" for c in coefs[ordem]]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh(np.array(nomes)[ordem], coefs[ordem], color=cores)
    ax.axvline(0, color="black", linewidth=0.8)
    ax.set_title(
        "Coeficientes da Regressão Logística\n(azul = aumenta a chance de inglês)"
    )
    ax.set_xlabel("Coeficiente")
    _salvar(fig, dir_saida, "04_coeficientes_logreg.png")


def gerar_graficos(modelo, X_test, y_test, y_full, dir_saida):
    """Gera todos os gráficos da análise em uma única chamada."""
    print("\n" + "#" * 60)
    print("GERANDO GRÁFICOS")
    print("#" * 60)
    plot_distribuicao_alvo(y_full, dir_saida)
    plot_curva_roc(modelo, X_test, y_test, dir_saida)
    plot_matriz_confusao(modelo, X_test, y_test, dir_saida)
    plot_coeficientes(modelo, dir_saida)


def main():
    parser = argparse.ArgumentParser(
        description="Mineração de dados (Regressão Logística) - Projeto Gutenberg"
    )
    parser.add_argument(
        "--db",
        default="data/gutenberg.db",
        help="Caminho para o banco SQLite gerado na fase 1 (padrão: data/gutenberg.db)",
    )
    parser.add_argument(
        "--graficos-dir",
        default=DIR_GRAFICOS,
        help=f"Pasta onde salvar os gráficos PNG (padrão: {DIR_GRAFICOS})",
    )
    parser.add_argument(
        "--sem-graficos",
        action="store_true",
        help="Não gerar os gráficos (apenas a saída em texto)",
    )
    args = parser.parse_args()

    caminho_copia = criar_copia_bd(args.db)
    try:
        print("Carregando dados da cópia do banco...")
        df = carregar_dados(caminho_copia)
        print(f"Registros carregados: {len(df)}")

        X, y, features_num, features_cat = preparar_features(df)
        print(f"Registros após limpeza: {len(X)}")
        print(f"Proporção de livros em inglês: {y.mean():.2%}")

        # ---- Classificação (supervisionado) ----
        modelo, X_test, y_test = treinar_avaliar(X, y, features_num, features_cat)

        print("\n" + "#" * 60)
        print("INTERPRETAÇÃO DO MODELO")
        print("#" * 60)
        interpretar_coeficientes(modelo)

        # ---- Gráficos ----
        if not args.sem_graficos:
            gerar_graficos(modelo, X_test, y_test, y, args.graficos_dir)
    finally:
        if os.path.exists(caminho_copia):
            os.remove(caminho_copia)
            print(f"\nCópia temporária removida: {caminho_copia}")


if __name__ == "__main__":
    main()
