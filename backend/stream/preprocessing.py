"""P1: derive a cleaned copy and features without changing the original text."""

import unicodedata


def preprocess_prompt(text, source_context):
    """Return the P1/P2 snapshot; model inference can still use the original text."""
    return {
        "cleaned_text": " ".join(unicodedata.normalize("NFC", text).split()),
        "features": {
            "char_count": len(text),
            "word_count": len(text.split()),
            "symbol_ratio": sum(not char.isalnum() and not char.isspace() for char in text) / len(text),
        },
        "source_context": dict(source_context),
    }
