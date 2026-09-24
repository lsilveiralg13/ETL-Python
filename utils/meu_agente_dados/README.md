# 🤖 Vetra — Agente Consultivo de Dados & GenAI (Enterprise Platform)

A **Vetra** é uma plataforma consultiva de inteligência de dados desenvolvida para atuar como assistente de Engenharia de Dados e BI. Ela combina arquitetura desacoplada via **Model Context Protocol (MCP)**, busca semântica **RAG no Qdrant Cloud**, validação de segurança **SQL AST (Schema-Aware)** e **resiliência com fallback multi-modelo** usando a Google Gemini API.

---

## 🏛️ Arquitetura do Sistema

```mermaid
graph TD
    A[🧑‍💻 Usuário / Analytics UI] -->|Prompt Sanitizado & PII Masking| B[Streamlit App - app.py]
    B -->|Model Context Protocol - stdio| C[Servidor MCP - servidor_mcp.py]
    
    subgraph "Subprocesso & Engine MCP"
        C -->|Busca Semântica Híbrida| D[(Qdrant Cloud Vector DB)]
        C -->|Validação AST & Guardrails| E[Engine SQL sqlglot]
        C -->|Cálculo de Indicadores| F[Métricas BI - OTIF / Lead Time]
    end
    
    B -->|Orquestração de Fallback 503/429| G[Google Gemini API]
    G -->|gemini-3-flash / gemini-2.5-flash| B
    B -->|Exportação de Relatórios| H[Excel / CSV / Markdown]