"""
LLM factory — returns a LangChain chat model for the configured free-tier provider.

Supported providers (set LLM_PROVIDER env var):
  groq   → llama-3.1-8b-instant   (default)
  gemini → gemini-1.5-flash
"""
from __future__ import annotations

import logging

from langchain_core.language_models.chat_models import BaseChatModel

from .config import (
    GEMINI_MODEL,
    GOOGLE_API_KEY,
    GROQ_API_KEY,
    GROQ_MODEL,
    LLM_PROVIDER,
)

logger = logging.getLogger(__name__)


def get_llm(temperature: float = 0.0) -> BaseChatModel:
    """Return a configured LangChain chat model (zero cost for POC)."""

    if LLM_PROVIDER == "gemini":
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "Install langchain-google-genai: pip install langchain-google-genai"
            ) from exc

        if not GOOGLE_API_KEY:
            raise ValueError(
                "Set GOOGLE_API_KEY for the Gemini provider. "
                "Get a free key at https://aistudio.google.com"
            )
        logger.info("Using Gemini free-tier model: %s", GEMINI_MODEL)
        return ChatGoogleGenerativeAI(
            model=GEMINI_MODEL,
            google_api_key=GOOGLE_API_KEY,
            temperature=temperature,
        )

    # Default: Groq free tier
    try:
        from langchain_groq import ChatGroq  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "Install langchain-groq: pip install langchain-groq"
        ) from exc

    if not GROQ_API_KEY:
        raise ValueError(
            "Set GROQ_API_KEY for the Groq provider. "
            "Get a free key at https://console.groq.com"
        )
    logger.info("Using Groq free-tier model: %s", GROQ_MODEL)
    return ChatGroq(
        model=GROQ_MODEL,
        api_key=GROQ_API_KEY,
        temperature=temperature,
    )
