from loguru import logger
from pathlib import Path

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logger.add(LOG_DIR / "bot.log", rotation="1 MB", retention="7 days")

def main() -> None:
    logger.info("Bot started")
    logger.info("Environment OK ✅")

if __name__ == "__main__":
    main()
