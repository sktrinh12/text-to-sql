"""
Configuration loaded from environment variables.

Free-tier model options (set LLM_PROVIDER env var):
  groq    → llama-3.1-8b-instant  (default, no credit card needed)
  gemini  → gemini-1.5-flash       (Google AI Studio free tier)
"""

import os

from dotenv import load_dotenv

# Load .env from the project root (walks up from this file's location)
load_dotenv()

# ── Database ──────────────────────────────────────────────────────────────────
DB_URI: str = os.getenv("DB_URI", "")
DB_DIALECT: str = os.getenv("DB_DIALECT", "sqlite")

# ── LLM ───────────────────────────────────────────────────────────────────────
LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "groq").lower()

# Groq (free tier) — https://console.groq.com
GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL: str = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

# Google Gemini (free tier via AI Studio) — https://aistudio.google.com
GOOGLE_API_KEY: str = os.getenv("GOOGLE_API_KEY", "")
GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")

# ── MCP ───────────────────────────────────────────────────────────────────────
# Path to the MCP server entry point; used by the MultiServerMCPClient.
MCP_SERVER_SCRIPT: str = os.path.join(os.path.dirname(__file__), "mcp_server.py")
MCP_SERVER_URL: str = os.environ.get("MCP_SERVER_URL", "http://localhost:3001/mcp")

# ── Loop guard ────────────────────────────────────────────────────────────────
MAX_CORRECTION_ITERATIONS: int = int(os.getenv("MAX_CORRECTION_ITERATIONS", "3"))

JSON_COLUMN_HINTS: dict = {
    "eln_writeup_api_extract": {
        "summary_data": {
            # ── Who did what ──────────────────────────────────────────────
            "ISID":              "User/login ID of the experiment owner (e.g. 'r.shetty')",
            "COMPLETED_ISID":    "User ID of the person who marked the experiment complete",
            "COUNTERSIGNER":     "User ID of the person who countersigned the experiment (e.g. 'h.lin')",
            # ── Experiment identity ───────────────────────────────────────
            "EXPERIMENT_ID":     "Numeric experiment identifier stored as text (e.g. '138844')",
            "EXPERIMENT_NAME":   "Human-readable experiment name / title",
            "PROTOCOL":          "Protocol type used (e.g. 'ChemELN')",
            "PROTOCOL_ID":       "Numeric protocol identifier stored as text (e.g. '81')",
            # ── Notebook location ─────────────────────────────────────────
            "BOOK":              "Lab notebook book number (e.g. '6')",
            "PAGE":              "Page number within the lab notebook (e.g. '15')",
            # ── Dates (format DD/MM/YYYY) ─────────────────────────────────
            "CREATED_DATE":      "Date the experiment record was created (DD/MM/YYYY)",
            "MODIFIED_DATE":     "Date the record was last modified (DD/MM/YYYY)",
            "COMPLETED_DATE":    "Date the experiment was completed (DD/MM/YYYY)",
            "COUNTERSIGNED_DATE":"Date the experiment was countersigned (DD/MM/YYYY)",
            # ── Status flags ──────────────────────────────────────────────
            "STATUS":            "Workflow status code as text — '0' means completed",
            "CURRENT_VERSION":   "Record version number as text (e.g. '1')",
            "PDF_COMPLETE":      "'Y' if the PDF has been generated, 'N' otherwise",
        }
    }
}
