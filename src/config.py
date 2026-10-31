"""
Configuration module for the Change Data Capture (CDC) pipeline.
Provides centralized path configuration, pipeline defaults, and domain constants.
"""

from dataclasses import dataclass
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent

@dataclass(frozen=True)
class PipelineConfig:
    """Central configuration for pipeline execution."""
    db_path: str = os.getenv("CDC_DB_PATH", str(BASE_DIR / "data" / "cdc_pipeline.db"))
    batch_size: int = int(os.getenv("CDC_BATCH_SIZE", "1000"))
    input_dir: Path = BASE_DIR / "data" / "input"
    output_dir: Path = BASE_DIR / "data" / "output"
    rejected_dir: Path = BASE_DIR / "data" / "rejected"
    log_level: str = os.getenv("CDC_LOG_LEVEL", "INFO")
    
    # Domain specific constraints
    allowed_currencies: tuple = ("USD", "EUR", "GBP", "CAD", "AUD", "INR")
    allowed_event_types: tuple = ("INSERT", "UPDATE", "DELETE")
    allowed_statuses: tuple = ("PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED")
    
    # Status hierarchy and terminal constraints
    status_ranks: tuple = (
        ("PENDING", 1),
        ("CONFIRMED", 2),
        ("SHIPPED", 3),
        ("DELIVERED", 4),
        ("CANCELLED", 4),
    )
    terminal_statuses: tuple = ("DELIVERED", "CANCELLED")


def get_default_config() -> PipelineConfig:
    """Returns a default pipeline configuration instance."""
    return PipelineConfig()
