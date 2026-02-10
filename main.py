"""ChimeraSQL — Cross-Database Fuzzing Tool entry point."""

from src.utils.config_loader import ConfigLoader
from src.utils.logger import get_logger


def main():
    logger = get_logger("chimera")

    logger.info("ChimeraSQL starting...")

    # Load configuration (Singleton)
    config = ConfigLoader()

    logger.info("Configuration loaded successfully:")
    for section, values in config.all.items():
        logger.info("  [%s] %s", section, values)

    # Verify dot-notation access
    logger.info("Oracle host: %s", config.get("oracle.host"))
    logger.info("SQLite path: %s", config.get("sqlite.db_path"))
    logger.info("Log file: %s", config.get("logging.log_file"))

    # Verify singleton behavior
    config2 = ConfigLoader()
    assert config is config2, "Singleton violated!"
    logger.debug("Singleton check passed (this line goes to log file only)")

    logger.info("Hello World complete — environment is ready!")


if __name__ == "__main__":
    main()
