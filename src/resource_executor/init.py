"""
Resource Execution Framework

A Python framework for executing tasks with comprehensive resource monitoring and management.
"""

from .core.config import ResourceConfig
from .core.monitor import ResourceMonitor, TaskMetrics, SystemResourceMonitor, ResourceThresholdMonitor
from .core.executor import TaskExecutor, TaskResult
from .core.tasks import BaseTask

# Import example tasks if they exist
try:
    from .examples.basic_tasks import CPUIntensiveTask, MemoryIntensiveTask, IOIntensiveTask
    from .examples.specialized_executors import BioinformaticsExecutor
except ImportError:
    # Examples might not be implemented yet
    pass

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

__all__ = [
    "ResourceConfig",
    "ResourceMonitor", 
    "TaskMetrics",
    "SystemResourceMonitor",
    "ResourceThresholdMonitor",
    "TaskExecutor",
    "TaskResult", 
    "BaseTask",
    # Add example classes if available
    "CPUIntensiveTask",
    "MemoryIntensiveTask", 
    "IOIntensiveTask",
    "BioinformaticsExecutor"
]
