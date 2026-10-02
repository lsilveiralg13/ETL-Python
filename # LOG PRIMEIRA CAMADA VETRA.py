# LOG PRIMEIRA CAMADA VETRA

--> Backend

python -m uvicorn utils.meu_agente_dados.main:app --host 0.0.0.0 --port 8000

--> Frontend

python -m streamlit run utils/meu_agente_dados/app.py --server.port 8501 --server.address