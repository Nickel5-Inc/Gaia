"""
Database service for the Gaia Validator service architecture.

This module provides thread-safe database operations with connection pooling,
async query processing, and health monitoring for the validator.
"""

from .service import DatabaseService

__all__ = [
    'DatabaseService',
] 