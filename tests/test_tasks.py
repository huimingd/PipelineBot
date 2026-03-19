"""
Tests for BaseTask and task-related functionality
"""

import pytest
import time
from unittest.mock import Mock, patch

from resource_executor.core.tasks import BaseTask
from resource_executor.core.config import ResourceConfig


class TestTask(BaseTask):
    """Test implementation of BaseTask"""
    
    def __init__(self, task_id: str, duration: float = 0.1, should_fail: bool = False, result_value: str = None):
        super().__init__(task_id)
        self.duration = duration
        self.should_fail = should_fail
        self.result_value = result_value or f"Result from {task_id}"
        self.setup_called = False
        self.cleanup_called = False
        self.validation_result = True
    
    def execute(self):
        time.sleep(self.duration)
        if self.should_fail:
            raise RuntimeError(f"Task {self.task_id} failed intentionally")
        return self.result_value
    
    def validate_inputs(self):
        return self.validation_result
    
    def setup(self):
        self.setup_called = True
    
    def cleanup(self):
        self.cleanup_called = True
    
    def get_estimated_resources(self):
        return ResourceConfig(cpus=1, memory_gb=0.5)


class TestBaseTask:
    """Test BaseTask functionality"""
    
    def test_task_creation(self):
        """Test basic task creation"""
        task = TestTask("test_task_1")
        
        assert task.task_id == "test_task_1"
        assert hasattr(task, 'logger')
        assert task.logger.name.endswith('test_task_1')
    
    def test_task_execution(self):
        """Test basic task execution"""
        task = TestTask("test_task_2", duration=0.05, result_value="test_result")
        
        result = task.execute()
        assert result == "test_result"
    
    def test_task_failure(self):
        """Test task execution failure"""
        task = TestTask("failing_task", should_fail=True)
        
        with pytest.raises(RuntimeError, match="failed intentionally"):
            task.execute()
    
    def test_task_validation(self):
        """Test input validation"""
        task = TestTask("validation_task")
        
        # Test successful validation
        assert task.validate_inputs() is True
        
        # Test failed validation
        task.validation_result = False
        assert task.validate_inputs() is False
    
    def test_task_setup_cleanup(self):
        """Test setup and cleanup methods"""
        task = TestTask("setup_cleanup_task")
        
        assert task.setup_called is False
        assert task.cleanup_called is False
        
        task.setup()
        assert task.setup_called is True
        
        task.cleanup()
        assert task.cleanup_called is True
    
    def test_estimated_resources(self):
        """Test resource estimation"""
        task = TestTask("resource_task")
        
        resources = task.get_estimated_resources()
        assert isinstance(resources, ResourceConfig)
        assert resources.cpus == 1
        assert resources.memory_gb == 0.5
    
    def test_task_with_kwargs(self):
        """Test task creation with keyword arguments"""
        task = TestTask("kwargs_task", custom_param="test_value", number_param=42)
        
        assert task.kwargs["custom_param"] == "test_value"
        assert task.kwargs["number_param"] == 42
    
    def test_abstract_methods(self):
        """Test that BaseTask is properly abstract"""
        # Should not be able to instantiate BaseTask directly
        with pytest.raises(TypeError):
            BaseTask("abstract_task")
    
    def test_task_logger_configuration(self):
        """Test that task logger is properly configured"""
        task = TestTask("logger_task")
        
        assert task.logger is not None
        assert "logger_task" in task.logger.name
        assert hasattr(task.logger, 'info')
        assert hasattr(task.logger, 'error')
        assert hasattr(task.logger, 'warning')
        assert hasattr(task.logger, 'debug')


class ParameterizedTask(BaseTask):
    """Task for testing parameterization"""
    
    def __init__(self, task_id: str, multiplier: int = 1, base_value: int = 10):
        super().__init__(task_id, multiplier=multiplier, base_value=base_value)
        self.multiplier = multiplier
        self.base_value = base_value
    
    def execute(self):
        return self.base_value * self.multiplier
    
    def validate_inputs(self):
        return self.multiplier > 0 and self.base_value >= 0


class TestParameterizedTask:
    """Test parameterized task functionality"""
    
    def test_parameterized_execution(self):
        """Test execution with different parameters"""
        task1 = ParameterizedTask("param_task_1", multiplier=2, base_value=5)
        task2 = ParameterizedTask("param_task_2", multiplier=3, base_value=7)
        
        assert task1.execute() == 10
        assert task2.execute() == 21
    
    def test_parameterized_validation(self):
        """Test validation with different parameters"""
        valid_task = ParameterizedTask("valid_task", multiplier=2, base_value=5)
        invalid_task1 = ParameterizedTask("invalid_task_1", multiplier=0, base_value=5)
        invalid_task2 = ParameterizedTask("invalid_task_2", multiplier=2, base_value=-1)
        
        assert valid_task.validate_inputs() is True
        assert invalid_task1.validate_inputs() is False
        assert invalid_task2.validate_inputs() is False
    
    def test_default_parameters(self):
        """Test task with default parameters"""
        task = ParameterizedTask("default_task")
        
        assert task.multiplier == 1
        assert task.base_value == 10
        assert task.execute() == 10


class ResourceAwareTask(BaseTask):
    """Task that provides different resource estimates"""
    
    def __init__(self, task_id: str, cpu_intensive: bool = False, memory_intensive: bool = False):
        super().__init__(task_
		