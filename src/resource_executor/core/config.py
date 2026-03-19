"""
Resource configuration classes and utilities.
"""

from dataclasses import dataclass

@dataclass
class ResourceConfig:
    """Configuration for resource allocation"""
    cpus: int = 1
    memory_gb: float = 1.0
    max_processes: int = 4
    timeout_seconds: int = 300
    priority: str = "normal"  # low, normal, high
    execution_mode: str = "thread"  # thread, process
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if self.cpus < 1:
            raise ValueError("CPUs must be at least 1")
        if self.memory_gb <= 0:
            raise ValueError("Memory must be positive")
        if self.max_processes < 1:
            raise ValueError("Max processes must be at least 1")
        if self.priority not in ["low", "normal", "high"]:
            raise ValueError("Priority must be 'low', 'normal', or 'high'")
        if self.execution_mode not in ["thread", "process"]:
            raise ValueError("Execution mode must be 'thread' or 'process'")
        