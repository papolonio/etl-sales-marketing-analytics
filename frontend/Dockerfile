# ── Stage 1: instalar dependencias ────────────────────────────────────────────
# Usa python:3.11-slim (Debian Bookworm): binarios wheel pre-compilados para
# pandas/numpy disponiveis no PyPI — sem compilacao de C, build rapido.
# Alpine evitado propositalmente: nao tem wheels para pandas/numpy, forcando
# compilacao from source (10-15x mais lento).
FROM python:3.11-slim AS deps

# uv: instalador de pacotes em Rust, substitui pip para instalar dependencias.
# Instala wheels em paralelo diretamente de PyPI sem compilar nada.
COPY --from=ghcr.io/astral-sh/uv:0.4.25 /uv /usr/local/bin/uv

WORKDIR /app

# Copia APENAS o requirements antes do codigo-fonte.
# Docker reutiliza esta layer enquanto requirements.txt nao mudar —
# o `pip install` (lento) so re-executa quando as dependencias mudarem.
COPY requirements.txt .

# UV_SYSTEM_PYTHON=1  : instala no Python do sistema (sem virtualenv)
# UV_COMPILE_BYTECODE=1: pre-compila .pyc na build (startup mais rapido no container)
# UV_NO_CACHE=1       : sem cache em disco (imagem menor)
RUN UV_SYSTEM_PYTHON=1 UV_COMPILE_BYTECODE=1 UV_NO_CACHE=1 \
    uv pip install -r requirements.txt


# ── Stage 2: imagem final ──────────────────────────────────────────────────────
# Recopia apenas o necessario — sem uv, sem cache, sem artefatos de build.
FROM python:3.11-slim

WORKDIR /app

# Copia site-packages instalados no stage anterior (sem o uv binario)
COPY --from=deps /usr/local/lib/python3.11/site-packages \
                 /usr/local/lib/python3.11/site-packages

# Copia executaveis (streamlit, etc.) instalados pelo uv
COPY --from=deps /usr/local/bin /usr/local/bin

# Copia o codigo-fonte por ultimo — layer mais volatil, invalida cache so aqui
COPY app.py .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
