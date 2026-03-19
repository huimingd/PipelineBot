"""
Tests for ResourceConfig class
"""

import pytest
from resource_executor.core.config import ResourceConfig

def test_default_config():
    """Test default configuration values"""
    config = ResourceConfig()
    assert config.cpus == 1
    assert config.memory_gb == 1.0
    assert config.max_processes == 4
    assert config.timeout_seconds == 300
    assert config.priority == "normal"
    assert config.execution_mode == "thread"

def test_custom_config():
    """Test custom configuration values"""
    config = ResourceConfig(
        cpus=4,
        memory_gb=2.0,
        max_processes=8,
        timeout_seconds=600,
        priority="high",
        execution_mode="process"
    )
    assert config.cpus == 4
    assert config.memory_gb == 2.0
    assert config.max_processes == 8
    assert config.timeout_seconds == 600
    assert config.priority == "high"
    assert config.execution_mode == "process"

def test_invalid_cpus():
    """Test validation of CPU count"""
    with pytest.raises(ValueError, match="CPUs must be at least 1"):
        ResourceConfig(cpus=0)

def test_invalid_memory():
    """Test validation of memory"""
    with pytest.raises(ValueError, match="Memory must be positive"):
        ResourceConfig(memory_gb=0)

def test_invalid_processes():
    """Test validation of max processes"""
    with pytest.raises(ValueError, match="Max processes must be at least 1"):
        ResourceConfig(max_processes=0)

def test_invalid_priority():
    """Test validation of priority"""
    with pytest.raises(ValueError, match="Priority must be"):
        ResourceConfig(priority="invalid")

def test_invalid_execution_mode():
    """Test validation of execution mode"""
    with pytest.raises(ValueError, match="Execution mode must be"):
        ResourceConfig(execution_mode="invalid")
		