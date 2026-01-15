"""
Configuration settings for the multi-agent data cleaning pipeline.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# API Configuration
REGOLO_API_KEY = os.getenv("REGOLO-API-KEY")
REGOLO_BASE_URL = "https://api.regolo.ai/v1"

# Model Configuration
OCR_MODEL = "deepseek-ocr"

# Per-agent model selection
MODEL_BIG = "gpt-oss-120b"
MODEL_LIGHT = "mistral-small3.2"

MODEL_STRUCTURING = MODEL_BIG
MODEL_NORMALIZATION = MODEL_LIGHT
MODEL_LAYOUT = MODEL_BIG
MODEL_HUMAN_REVIEW = MODEL_LIGHT

# Pipeline Configuration
MAX_RETRIES = 3
CONFIDENCE_THRESHOLD = 0.7
INITIAL_BACKOFF = 2

# Base directory
BASE_DIR = Path(__file__).parent


def get_output_dir(input_file: str | None = None) -> Path:
    """Create output directory based on input filename."""
    if input_file is None:
        return BASE_DIR
    input_path = Path(input_file)
    output_name = f"output-{input_path.stem}"
    output_dir = BASE_DIR / output_name
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "checkpoints").mkdir(exist_ok=True)
    (output_dir / "final").mkdir(exist_ok=True)
    return output_dir


def get_checkpoint_path(output_dir: Path, filename: str) -> Path:
    return output_dir / "checkpoints" / filename


def get_final_path(output_dir: Path, filename: str) -> Path:
    return output_dir / "final" / filename


# Default paths (can be overridden per-run)
SOURCE_PDF = BASE_DIR / "document.pdf"
SOURCE_MD = BASE_DIR / "document.md"

# Valid section types
SECTION_TYPES = [
    "ANAGRAFICA",
    "AMMINISTRATIVI",
    "TRANSAZIONI",
    "TICKET",
    "ALTRO"
]

# Currency normalization map
CURRENCY_MAP = {
    "€": "EUR",
    "Euro": "EUR",
    "EUR": "EUR",
    "euro": "EUR",
    "dollar": "USD",
    "$": "USD",
    "USD": "USD",
    "£": "GBP",
    "GBP": "GBP",
}

# DB Schema definitions
DB_SCHEMA = {
    "customer": {
        "fields": [
            "nome", "cognome", "ragione_sociale", "codice_fiscale", "partita_iva",
            "email", "telefono", "cellulare", "indirizzo", "citta", "cap",
            "provincia", "nazione", "data_nascita", "luogo_nascita"
        ],
        "required": ["nome", "cognome", "codice_fiscale"]
    },
    "policy": {
        "fields": [
            "polizza_numero", "tipo", "stato", "data_decorrenza", "data_scadenza",
            "premio", "premio_annuale", "franchigia", "massimale", "compagnia",
            "agente", "rata_pagamento"
        ],
        "required": ["polizza_numero", "tipo"]
    },
    "transaction": {
        "fields": [
            "transazione_id", "data", "importo", "tipo", "descrizione",
            "metodo_pagamento", "riferimento_polizza", "stato"
        ],
        "required": ["data", "importo", "tipo"]
    },
    "ticket": {
        "fields": [
            "ticket_id", "data_apertura", "data_chiusura", "stato", "priorita",
            "categoria", "descrizione", "risoluzione", "assegnato_a"
        ],
        "required": ["ticket_id", "stato"]
    }
}
