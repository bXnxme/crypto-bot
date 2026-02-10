import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

def str_to_bool(v: str) -> bool:
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}

@dataclass(frozen=True)
class Settings:
    api_key: str = os.getenv("BINANCE_API_KEY", "")
    api_secret: str = os.getenv("BINANCE_API_SECRET", "")

    # Binance Spot Testnet base URL:
    base_url: str = "https://testnet.binance.vision"

    dry_run: bool = str_to_bool(os.getenv("DRY_RUN", "true"))

settings = Settings()
