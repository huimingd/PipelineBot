"""
Resource Execution Framework

A Python framework for executing tasks with comprehensive resource monitoring and management.
"""

from .core.config import ResourceConfig
from .core.monitor import ResourceMonitor, TaskMetrics
from .core.executor import TaskExecutor, TaskResult
from .core.tasks import BaseTask

from .examples.basic_tasks import CPUIntensiveTask, MemoryIntensiveTask, IOIntensiveTask
from .examples.specialized_executors import BioinformaticsExecutor

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

__all__ = [
    "ResourceConfig",
    "ResourceMonitor", 
    "TaskMetrics",
    "TaskExecutor",
    "TaskResult", 
    "BaseTask",
    "CPUIntensiveTask",
    "MemoryIntensiveTask", 
    "IOIntensiveTask",
    "BioinformaticsExecutor"
]
