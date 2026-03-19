"""
Tests for ResourceConfig class
"""

import pytest
from resource_executor.core.config import ResourceConfig


class TestResourceConfig:
    """Test ResourceConfig functionality"""
    
    def test_default_config(self):
        """Test default configuration values"""
        config = ResourceConfig()
        assert config.cpus == 1
        assert config.memory_gb == 1.0
        assert config.max_processes == 4
        assert config.timeout_seconds == 300
        assert config.priority == "normal"
        assert config.execution_mode == "thread"
    
    def test_custom_config(self):
        """Test custom configuration values"""
        config = ResourceConfig(
            cpus=8,
            memory_gb=4.0,
            max_processes=16,
            timeout_seconds=1200,
            priority="high",
            execution_mode="process"
        )
        assert config.cpus == 8
        assert config.memory_gb == 4.0
        assert config.max_processes == 16
        assert config.timeout_seconds == 1200
        assert config.priority == "high"
        assert config.execution_mode == "process"
    
    def test_invalid_cpus(self):
        """Test validation of CPU count"""
        with pytest.raises(ValueError, match="CPUs must be at least 1"):
            ResourceConfig(cpus=0)
        
        with pytest.raises(ValueError, match="CPUs must be at least 1"):
            ResourceConfig(cpus=-1)
    
    def test_invalid_memory(self):
        """Test validation of memory"""
        with pytest.raises(ValueError, match="Memory must be positive"):
            ResourceConfig(memory_gb=0)
        
        with pytest.raises(ValueError, match="Memory must be positive"):
            ResourceConfig(memory_gb=-1.0)
    
    def test_invalid_processes(self):
        """Test validation of max processes"""
        with pytest.raises(ValueError, match="Max processes must be at least 1"):
            ResourceConfig(max_processes=0)
        
        with pytest.raises(ValueError, match="Max processes must be at least 1"):
            ResourceConfig(max_processes=-1)
    
    def test_invalid_priority(self):
        """Test validation of priority"""
        with pytest.raises(ValueError, match="Priority must be"):
            ResourceConfig(priority="invalid")
        
        with pytest.raises(ValueError, match="Priority must be"):
            ResourceConfig(priority="NORMAL")  # Case sensitive
    
    def test_invalid_execution_mode(self):
        """Test validation of execution mode"""
        with pytest.raises(ValueError, match="Execution mode must be"):
            ResourceConfig(execution_mode="invalid")
        
        with pytest.raises(ValueError, match="Execution mode must be"):
            ResourceConfig(execution_mode="THREAD")  # Case sensitive
    
    def test_valid_priorities(self):
        """Test all valid priority values"""
        for priority in ["low", "normal", "high"]:
            config = ResourceConfig(priority=priority)
            assert config.priority == priority
    
    def test_valid_execution_modes(self):
        """Test all valid execution modes"""
        for mode in ["thread", "process"]:
            config = ResourceConfig(execution_mode=mode)
            assert config.execution_mode == mode
    
    def test_config_immutability_after_validation(self):
        """Test that config can be modified after creation"""
        config = ResourceConfig(cpus=2)
        # Should be able to modify (dataclass is mutable by default)
        config.cpus = 4
        assert config.cpus == 4
        
        # But validation should still work if we create a new instance
        with pytest.raises(ValueError):
            ResourceConfig(cpus=0)
			