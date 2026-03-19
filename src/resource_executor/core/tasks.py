#!/usr/bin/env python3
"""
Abstract base class for all tasks
"""
import os
import sys
from typing import List, Dict, Any, Optional, Callable, Union
from abc import ABC, abstractmethod
import logging
from .config import ResourceConfig

class BaseTask(ABC):
    """Abstract base class for all tasks"""
    
    def __init__(self, task_id: str, **kwargs):
        self.task_id = task_id
        self.kwargs = kwargs
        self.logger = logging.getLogger(f"{self.__class__.__name__}.{task_id}")
        
    @abstractmethod
    def execute(self) -> Any:
        """Execute the task - must be implemented by subclasses"""
        pass
    
    def validate_inputs(self) -> bool:
        """Validate task inputs - can be overridden by subclasses"""
        return True
    
    def setup(self):
        """Setup before execution - can be overridden by subclasses"""
        pass
    
    def cleanup(self):
        """Cleanup after execution - can be overridden by subclasses"""
        pass
    
    def get_estimated_resources(self) -> ResourceConfig:
        """Get estimated resource requirements - can be overridden"""
        return ResourceConfig()
		