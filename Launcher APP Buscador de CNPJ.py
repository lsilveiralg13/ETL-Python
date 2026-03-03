from __future__ import annotations

import os
import sys
from pathlib import Path

from streamlit.web import cli as stcli

if __name__ == "__main__":
    # Garante que o Streamlit rode na pasta deste launcher (evita pegar app.py de outro projeto)
    here = Path(__file__).resolve().parent
    os.chdir(here)

    # Roda o arquivo certo do projeto CNPJ
    app_file = here / "APP Buscador de CNPJ.py"

    # Equivalente a:
    # streamlit run "APP Buscador de CNPJ.py" --server.headless=true --server.port=8502
    sys.argv = [
        "streamlit",
        "run",
        str(app_file),
        "--server.headless=true",
        "--server.port=8502",
    ]
    sys.exit(stcli.main())
