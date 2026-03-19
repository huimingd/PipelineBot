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
    """Task that provides different resource estimates based on configuration"""
    
    def __init__(self, task_id: str, cpu_intensive: bool = False, memory_intensive: bool = False, 
                 io_intensive: bool = False, duration: float = 1.0):
        super().__init__(task_id, cpu_intensive=cpu_intensive, memory_intensive=memory_intensive,
                        io_intensive=io_intensive, duration=duration)
        self.cpu_intensive = cpu_intensive
        self.memory_intensive = memory_intensive
        self.io_intensive = io_intensive
        self.duration = duration
        self.allocated_memory = []
    
    def execute(self):
        """Execute task based on its characteristics"""
        start_time = time.time()
        
        # Simulate different types of work
        if self.cpu_intensive:
            # CPU-intensive computation
            end_time = time.time() + self.duration
            while time.time() < end_time:
                _ = sum(i * i for i in range(1000))
        
        if self.memory_intensive:
            # Allocate memory (10MB chunks)
            for _ in range(10):
                chunk = bytearray(1024 * 1024)  # 1MB
                self.allocated_memory.append(chunk)
            time.sleep(self.duration * 0.5)  # Hold memory for half the duration
        
        if self.io_intensive:
            # Simulate I/O operations
            import tempfile
            import os
            
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            try:
                # Write some data
                data = b'x' * (1024 * 1024)  # 1MB of data
                for _ in range(5):  # Write 5MB total
                    temp_file.write(data)
                temp_file.flush()
                
                # Read it back
                temp_file.close()
                with open(temp_file.name, 'rb') as f:
                    while f.read(1024 * 1024):  # Read in 1MB chunks
                        pass
            finally:
                os.unlink(temp_file.name)
        
        if not any([self.cpu_intensive, self.memory_intensive, self.io_intensive]):
            # Default: just sleep
            time.sleep(self.duration)
        
        execution_time = time.time() - start_time
        return f"Task {self.task_id} completed in {execution_time:.2f}s"
    
    def cleanup(self):
        """Clean up allocated resources"""
        self.allocated_memory.clear()
    
    def get_estimated_resources(self) -> ResourceConfig:
        """Provide resource estimates based on task characteristics"""
        cpus = 1
        memory_gb = 0.1
        
        if self.cpu_intensive:
            cpus = 2
            memory_gb = 0.2
        
        if self.memory_intensive:
            memory_gb = max(memory_gb, 0.5)  # At least 500MB for memory-intensive tasks
        
        if self.io_intensive:
            memory_gb = max(memory_gb, 0.3)  # I/O tasks need some buffer memory
        
        return ResourceConfig(
            cpus=cpus,
            memory_gb=memory_gb,
            timeout_seconds=int(self.duration * 10)  # 10x duration as timeout
        )


class TestResourceAwareTask:
    """Test ResourceAwareTask functionality"""
    
    def test_cpu_intensive_task(self):
        """Test CPU-intensive task configuration"""
        task = ResourceAwareTask("cpu_task", cpu_intensive=True, duration=0.1)
        
        # Test resource estimation
        resources = task.get_estimated_resources()
        assert resources.cpus == 2
        assert resources.memory_gb >= 0.2
        
        # Test execution
        result = task.execute()
        assert "cpu_task" in result
        assert "completed" in result
    
    def test_memory_intensive_task(self):
        """Test memory-intensive task configuration"""
        task = ResourceAwareTask("memory_task", memory_intensive=True, duration=0.1)
        
        # Test resource estimation
        resources = task.get_estimated_resources()
        assert resources.memory_gb >= 0.5
        
        # Test execution
        result = task.execute()
        assert "memory_task" in result
        
        # Test cleanup
        task.cleanup()
        assert len(task.allocated_memory) == 0
    
    def test_io_intensive_task(self):
        """Test I/O-intensive task configuration"""
        task = ResourceAwareTask("io_task", io_intensive=True, duration=0.1)
        
        # Test resource estimation
        resources = task.get_estimated_resources()
        assert resources.memory_gb >= 0.3
        
        # Test execution
        result = task.execute()
        assert "io_task" in result
    
    def test_combined_intensive_task(self):
        """Test task with multiple intensive characteristics"""
        task = ResourceAwareTask("combined_task", 
                                cpu_intensive=True, 
                                memory_intensive=True, 
                                io_intensive=True,
                                duration=0.1)
        
        # Test resource estimation
        resources = task.get_estimated_resources()
        assert resources.cpus == 2
        assert resources.memory_gb >= 0.5  # Should take the maximum
        
        # Test execution
        result = task.execute()
        assert "combined_task" in result
    
    def test_default_task(self):
        """Test task with no intensive characteristics"""
        task = ResourceAwareTask("default_task", duration=0.05)
        
        # Test resource estimation
        resources = task.get_estimated_resources()
        assert resources.cpus == 1
        assert resources.memory_gb == 0.1
        
        # Test execution
        result = task.execute()
        assert "default_task" in result
    
    def test_timeout_estimation(self):
        """Test timeout estimation based on duration"""
        task = ResourceAwareTask("timeout_task", duration=2.0)
        
        resources = task.get_estimated_resources()
        assert resources.timeout_seconds == 20  # 10x duration


class AsyncTask(BaseTask):
    """Task that simulates asynchronous behavior"""
    
    def __init__(self, task_id: str, delay: float = 0.1, callback_data: str = None):
        super().__init__(task_id, delay=delay, callback_data=callback_data)
        self.delay = delay
        self.callback_data = callback_data or f"callback_data_{task_id}"
        self.callback_called = False
    
    def execute(self):
        """Simulate async execution with callback"""
        time.sleep(self.delay)
        self._simulate_callback()
        return f"Async task {self.task_id} completed"
    
    def _simulate_callback(self):
        """Simulate a callback function"""
        self.callback_called = True
        return self.callback_data
    
    def validate_inputs(self):
        """Validate that delay is reasonable"""
        return 0 <= self.delay <= 10.0


class TestAsyncTask:
    """Test AsyncTask functionality"""
    
    def test_async_task_execution(self):
        """Test async task execution"""
        task = AsyncTask("async_task", delay=0.05, callback_data="test_callback")
        
        result = task.execute()
        
        assert "async_task" in result
        assert task.callback_called is True
    
    def test_async_task_validation(self):
        """Test async task validation"""
        valid_task = AsyncTask("valid_async", delay=1.0)
        invalid_task = AsyncTask("invalid_async", delay=15.0)
        
        assert valid_task.validate_inputs() is True
        assert invalid_task.validate_inputs() is False
    
    def test_callback_data(self):
        """Test callback data handling"""
        task = AsyncTask("callback_task", callback_data="custom_data")
        
        callback_result = task._simulate_callback()
        
        assert callback_result == "custom_data"
        assert task.callback_called is True


class TaskWithDependencies(BaseTask):
    """Task that has dependencies on other tasks"""
    
    def __init__(self, task_id: str, dependencies: list = None, dependency_results: dict = None):
        super().__init__(task_id, dependencies=dependencies, dependency_results=dependency_results)
        self.dependencies = dependencies or []
        self.dependency_results = dependency_results or {}
    
    def execute(self):
        """Execute task using dependency results"""
        if not self._check_dependencies():
            raise RuntimeError(f"Dependencies not satisfied for {self.task_id}")
        
        # Use dependency results in computation
        total = sum(self.dependency_results.values()) if self.dependency_results else 0
        return f"Task {self.task_id} result: {total}"
    
    def validate_inputs(self):
        """Validate that all dependencies are provided"""
        return self._check_dependencies()
    
    def _check_dependencies(self):
        """Check if all dependencies are satisfied"""
        for dep in self.dependencies:
            if dep not in self.dependency_results:
                return False
        return True


class TestTaskWithDependencies:
    """Test TaskWithDependencies functionality"""
    
    def test_task_with_satisfied_dependencies(self):
        """Test task with all dependencies satisfied"""
        task = TaskWithDependencies(
            "dependent_task",
            dependencies=["task1", "task2"],
            dependency_results={"task1": 10, "task2": 20}
        )
        
        assert task.validate_inputs() is True
        result = task.execute()
        assert "30" in result  # 10 + 20
    
    def test_task_with_missing_dependencies(self):
        """Test task with missing dependencies"""
        task = TaskWithDependencies(
            "dependent_task",
            dependencies=["task1", "task2"],
            dependency_results={"task1": 10}  # Missing task2
        )
        
        assert task.validate_inputs() is False
        
        with pytest.raises(RuntimeError, match="Dependencies not satisfied"):
            task.execute()
    
    def test_task_with_no_dependencies(self):
        """Test task with no dependencies"""
        task = TaskWithDependencies("independent_task")
        
        assert task.validate_inputs() is True
        result = task.execute()
        assert "0" in result  # No dependencies, sum is 0


class TestTaskErrorHandling:
    """Test various error handling scenarios"""
    
    def test_task_execution_timeout_simulation(self):
        """Test task that simulates timeout behavior"""
        class TimeoutTask(BaseTask):
            def __init__(self, task_id: str, should_timeout: bool = False):
                super().__init__(task_id)
                self.should_timeout = should_timeout
            
            def execute(self):
                if self.should_timeout:
                    time.sleep(10)  # Long sleep to simulate timeout
                return "completed"
        
        # Normal execution
        normal_task = TimeoutTask("normal_task", should_timeout=False)
        result = normal_task.execute()
        assert result == "completed"
        
        # Timeout task (we won't actually wait for it)
        timeout_task = TimeoutTask("timeout_task", should_timeout=True)
        # In real usage, this would be handled by the executor's timeout mechanism
        assert timeout_task.should_timeout is True
    
    def test_task_resource_cleanup_on_error(self):
        """Test that cleanup is called even when execution fails"""
        class CleanupTask(BaseTask):
            def __init__(self, task_id: str, should_fail: bool = False):
                super().__init__(task_id)
                self.should_fail = should_fail
                self.resource_allocated = False
                self.resource_cleaned = False
            
            def setup(self):
                self.resource_allocated = True
            
            def execute(self):
                if self.should_fail:
                    raise RuntimeError("Task failed")
                return "success"
            
            def cleanup(self):
                self.resource_cleaned = True
        
        # Test successful execution
        success_task = CleanupTask("success_task", should_fail=False)
        success_task.setup()
        result = success_task.execute()
        success_task.cleanup()
        
        assert result == "success"
        assert success_task.resource_allocated is True
        assert success_task.resource_cleaned is True
        
        # Test failed execution (cleanup should still be possible)
        failed_task = CleanupTask("failed_task", should_fail=True)
        failed_task.setup()
        
        with pytest.raises(RuntimeError):
            failed_task.execute()
        
        failed_task.cleanup()  # Should still work
        assert failed_task.resource_allocated is True
        assert failed_task.resource_cleaned is True
    
    def test_task_with_invalid_configuration(self):
        """Test task behavior with invalid configuration"""
        class ConfigurableTask(BaseTask):
            def __init__(self, task_id: str, config_value: int):
                super().__init__(task_id, config_value=config_value)
                self.config_value = config_value
            
            def validate_inputs(self):
                return 0 <= self.config_value <= 100
            
            def execute(self):
                return f"Config value: {self.config_value}"
        
        # Valid configuration
        valid_task = ConfigurableTask("valid_config", 50)
        assert valid_task.validate_inputs() is True
        
        # Invalid configurations
        invalid_task1 = ConfigurableTask("invalid_low", -1)
        invalid_task2 = ConfigurableTask("invalid_high", 101)
        
        assert invalid_task1.validate_inputs() is False
        assert invalid_task2.validate_inputs() is False
        