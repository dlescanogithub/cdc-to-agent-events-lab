"""Synthetic CDC-to-agent pipeline lab."""

import logging

__version__ = "0.1.1"

_logger = logging.getLogger("cdc_lab")
_logger.addHandler(logging.NullHandler())
_logger.propagate = False
