#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Text-to-SQL adapter for the Meta Demo project.
This module provides a unified interface for different text-to-SQL backends.
"""

import os
import logging
from typing import Dict, Any, Optional, List, Union
from enum import Enum

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TextToSQLBackend(Enum):
    """Supported text-to-SQL backends."""
    LLAMAINDEX = "llamaindex"

class TextToSQLAdapter:
    """
    Adapter for text-to-SQL backends.
    Provides a unified interface for different text-to-SQL implementations.
    """
    
    def __init__(
        self, 
        backend: Union[TextToSQLBackend, str] = TextToSQLBackend.LLAMAINDEX,
        api_key: Optional[str] = None, 
        model: Optional[str] = None, 
        temperature: Optional[float] = None
    ):
        """
        Initialize the text-to-SQL adapter.
        
        Args:
            backend: Text-to-SQL backend to use (currently only LlamaIndex is supported)
            api_key: API key for the LLM service
            model: Model name to use
            temperature: Temperature for the model
        """
        # Convert string to enum if needed
        if isinstance(backend, str):
            try:
                backend = TextToSQLBackend(backend.lower())
            except ValueError:
                logger.warning(f"Invalid backend '{backend}', defaulting to LlamaIndex")
                backend = TextToSQLBackend.LLAMAINDEX
        
        self.backend_type = backend
        logger.info(f"Initializing text-to-SQL adapter with backend: {backend.value}")
        
        # Initialize LlamaIndex backend
        try:
            from app.llamaindex_core import initialize_llamaindex
            self.backend = initialize_llamaindex(api_key=api_key, model=model, temperature=temperature)
            logger.info("LlamaIndex backend initialized successfully")
        except ImportError as e:
            logger.error(f"Error importing llamaindex_core: {e}")
            raise ImportError(f"Could not import LlamaIndex backend: {e}")
    
    def ask(self, question: str) -> Dict[str, Any]:
        """
        Ask a natural language question and get SQL and results.
        
        Args:
            question: Natural language question about the data
            
        Returns:
            Dictionary with question, SQL, and results
        """
        return self.backend.ask(question)
    

    
    @property
    def backend_name(self) -> str:
        """Get the name of the current backend."""
        return self.backend_type.value


def initialize_text_to_sql(
    backend: Union[TextToSQLBackend, str] = TextToSQLBackend.LLAMAINDEX,
    api_key: Optional[str] = None, 
    model: Optional[str] = None, 
    temperature: Optional[float] = None
) -> TextToSQLAdapter:
    """
    Initialize the text-to-SQL adapter.
    
    Args:
        backend: Text-to-SQL backend to use (currently only LlamaIndex is supported)
        api_key: API key for the LLM service
        model: Model name to use
        temperature: Temperature for the model
        
    Returns:
        Initialized TextToSQLAdapter instance
    """
    return TextToSQLAdapter(
        backend=backend,
        api_key=api_key,
        model=model,
        temperature=temperature
    )
