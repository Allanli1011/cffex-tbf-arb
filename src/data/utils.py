"""Shared utilities: logger, retry decorator."""

from __future__ import annotations

import functools
import sys
import time
from pathlib import Path
from typing import Callable, TypeVar

from loguru import logger

from .storage import REPO_ROOT

T = TypeVar("T")

_LOGGER_CONFIGURED = False


def configure_logger(level: str = "INFO", to_file: bool = True) -> None:
    """Configure loguru once. Idempotent."""
    global _LOGGER_CONFIGURED
    if _LOGGER_CONFIGURED:
        return

    logger.remove()
    logger.add(
        sys.stderr,
        level=level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <7}</level> | "
        "<cyan>{name}</cyan>:{function}:{line} - {message}",
    )
    if to_file:
        log_dir = REPO_ROOT / "data" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        logger.add(
            log_dir / "app_{time:YYYY-MM-DD}.log",
            level="DEBUG",
            rotation="00:00",
            retention="30 days",
            compression="zip",
            enqueue=True,
        )
    _LOGGER_CONFIGURED = True


def retry(
    max_attempts: int = 3,
    initial_wait: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Retry decorator with exponential backoff.

    Use for network-bound calls (AKShare, scrapers). Raises the last
    exception after ``max_attempts`` failures.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            wait = initial_wait
            last_exc: BaseException | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        logger.error(
                            f"{func.__name__} failed after {attempt} attempts: {exc}"
                        )
                        raise
                    logger.warning(
                        f"{func.__name__} attempt {attempt}/{max_attempts} "
                        f"failed: {exc}. Retrying in {wait:.1f}s."
                    )
                    time.sleep(wait)
                    wait *= backoff
            assert last_exc is not None  # for type checker
            raise last_exc

        return wrapper

    return decorator
