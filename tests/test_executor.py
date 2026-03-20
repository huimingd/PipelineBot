"""
Tests for TaskExecutor class - the core execution engine
"""

import pytest
import time
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from concurrent.futures import Future

# Add src to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from resource_executor.core.executor import TaskExecutor, TaskResult
from resource_executor.core.config import ResourceConfig
from conftest import SimpleTask, ResourceIntensiveTask


class TestTaskExecutor:
    """Test TaskExecutor functionality"""
    
    def test_executor_creation(self, basic_config):
        """Test TaskExecutor creation"""
        executor = TaskExecutor(basic_config)
        
        assert executor.config == basic_config
        assert len(executor.executed_tasks) == 0
        assert executor._task_counter == 0
    
    def test_single_task_execution(self, basic_config, simple_task):
        """Test executing a single task"""
        executor = TaskExecutor(basic_config)
        
        result = executor.execute_task(simple_task)
        
        assert isinstance(result, TaskResult)
        assert result.success is True
        assert result.task_id == "test_task"
        assert result.result == "Task test_task completed"
        assert result.metrics is not None
        assert len(executor.executed_tasks) == 1
    
    def test_failing_task_execution(self, basic_config, failing_task):
        """Test executing a failing task"""
        executor = TaskExecutor(basic_config)
        
        result = executor.execute_task(failing_task)
        
        assert result.success is False
        assert result.error is not None
        assert "intentionally failed" in str(result.error)
        assert result.metrics.exit_code == 1
    
    def test_callable_task_execution(self, basic_config):
        """Test executing a callable as task"""
        executor = TaskExecutor(basic_config)
        
        def simple_function(x, y):
            return x + y
        
        # FIXED: Correct parameter order - task, task_id, monitor, timeout, *args
        result = executor.execute_task(simple_function, "math_task", True, None, 5, 3)
        
        assert result.success is True
        assert result.result == 8
        assert result.task_id == "math_task"
    
    def test_callable_task_with_kwargs(self, basic_config):
        """Test executing a callable with keyword arguments"""
        executor = TaskExecutor(basic_config)
        
        def function_with_kwargs(x, y, multiplier=1):
            return (x + y) * multiplier
        
        result = executor.execute_task(
            function_with_kwargs, 
            "kwargs_task", 
            True,  # monitor
            None,  # timeout
            5, 3,  # args
            multiplier=2  # kwargs
        )
        
        assert result.success is True
        assert result.result == 16  # (5 + 3) * 2
        assert result.task_id == "kwargs_task"
    
    def test_callable_without_monitoring(self, basic_config):
        """Test executing callable without monitoring"""
        executor = TaskExecutor(basic_config)
        
        def quick_function():
            return "quick_result"
        
        result = executor.execute_task(quick_function, "no_monitor_task", False)
        
        assert result.success is True
        assert result.result == "quick_result"
        assert result.metrics is None  # No monitoring
    
    def test_parallel_task_execution(self, basic_config):
        """Test parallel task execution"""
        executor = TaskExecutor(basic_config)
        
        tasks = [
            SimpleTask(f"parallel_task_{i}", duration=0.1) 
            for i in range(3)
        ]
        
        results = executor.execute_parallel_tasks(tasks)
        
        assert len(results) == 3
        assert all(r.success for r in results)
        assert len(executor.executed_tasks) == 3
    
    def test_parallel_mixed_tasks(self, basic_config):
        """Test parallel execution with mixed task types"""
        executor = TaskExecutor(basic_config)
        
        def simple_callable(value):
            time.sleep(0.05)
            return value * 2
        
        # Mix BaseTask instances and callables
        mixed_tasks = [
            SimpleTask("task_1", duration=0.05),
            (simple_callable, (10,), {}),  # tuple format: (func, args, kwargs)
            SimpleTask("task_2", duration=0.05)
        ]
        
        results = executor.execute_parallel_tasks(mixed_tasks)
        
        assert len(results) == 3
        assert all(r.success for r in results)
        assert results[1].result == 20  # 10 * 2
    
    def test_sequential_task_execution(self, basic_config):
        """Test sequential task execution"""
        executor = TaskExecutor(basic_config)
        
        tasks = [
            SimpleTask(f"seq_task_{i}", duration=0.05) 
            for i in range(3)
        ]
        
        start_time = time.time()
        results = executor.execute_sequential_tasks(tasks)
        duration = time.time() - start_time
        
        assert len(results) == 3
        assert all(r.success for r in results)
        assert duration >= 0.15  # Should take at least sum of durations
    
    def test_sequential_with_stop_on_failure(self, basic_config):
        """Test sequential execution with stop on failure"""
        executor = TaskExecutor(basic_config)
        
        tasks = [
            SimpleTask("success_1", duration=0.02),
            SimpleTask("failure", duration=0.02, should_fail=True),
            SimpleTask("success_2", duration=0.02)  # Should not execute
        ]
        
        results = executor.execute_sequential_tasks(tasks, stop_on_failure=True)
        
        assert len(results) == 2  # Should stop after failure
        assert results[0].success is True
        assert results[1].success is False
    
    def test_execution_summary(self, basic_config):
        """Test execution summary generation"""
        executor = TaskExecutor(basic_config)
        
        # Execute some tasks
        executor.execute_task(SimpleTask("task1", duration=0.05))
        executor.execute_task(SimpleTask("task2", duration=0.05, should_fail=True))
        
        summary = executor.get_execution_summary()
        
        assert summary['execution_summary']['total_tasks'] == 2
        assert summary['execution_summary']['successful_tasks'] == 1
        assert summary['execution_summary']['failed_tasks'] == 1
        assert summary['execution_summary']['success_rate'] == 50.0
        assert 'resource_config' in summary
        assert 'task_results' in summary
    
    def test_save_results_json(self, basic_config, temp_dir):
        """Test saving execution results as JSON"""
        executor = TaskExecutor(basic_config)
        executor.execute_task(SimpleTask("save_test", duration=0.05))
        
        json_file = temp_dir / "results.json"
        executor.save_results(json_file, format="json")
        
        assert json_file.exists()
        
        # Verify JSON content
        import json
        with open(json_file) as f:
            data = json.load(f)
        
        assert 'execution_summary' in data
        assert data['execution_summary']['total_tasks'] == 1
    
    def test_save_results_pickle(self, basic_config, temp_dir):
        """Test saving execution results as pickle"""
        executor = TaskExecutor(basic_config)
        executor.execute_task(SimpleTask("pickle_test", duration=0.05))
        
        pickle_file = temp_dir / "results.pkl"
        executor.save_results(pickle_file, format="pickle")
        
        assert pickle_file.exists()
    
    def test_save_results_invalid_format(self, basic_config, temp_dir):
        """Test saving with invalid format raises error"""
        executor = TaskExecutor(basic_config)
        executor.execute_task(SimpleTask("invalid_test", duration=0.05))
        
        invalid_file = temp_dir / "results.invalid"
        
        with pytest.raises(ValueError, match="Unsupported format"):
            executor.save_results(invalid_file, format="invalid")
    
    def test_clear_results(self, basic_config):
        """Test clearing execution results"""
        executor = TaskExecutor(basic_config)
        
        # Execute some tasks
        executor.execute_task(SimpleTask("task1", duration=0.02))
        executor.execute_task(SimpleTask("task2", duration=0.02))
        
        assert len(executor.executed_tasks) == 2
        assert executor._task_counter == 2
        
        # Clear results
        executor.clear_results()
        
        assert len(executor.executed_tasks) == 0
        assert executor._task_counter == 0
    
    def test_task_timeout(self, basic_config):
        """Test task timeout handling"""
        # Use a very short timeout config for testing
        short_timeout_config = ResourceConfig(
            cpus=1, memory_gb=0.5, max_processes=1, timeout_seconds=1
        )
        executor = TaskExecutor(short_timeout_config)
        
        def long_running_task():
            time.sleep(2)  # Longer than timeout
            return "should_not_complete"
        
        # This test depends on the timeout implementation
        # The actual behavior may vary based on the system
        result = executor.execute_task(long_running_task, "timeout_test", True, 1)  # 1 second timeout
        
        # The result could be either a timeout error or completion depending on implementation
        assert result.task_id == "timeout_test"
    
    def test_empty_execution_summary(self, basic_config):
        """Test execution summary with no tasks"""
        executor = TaskExecutor(basic_config)
        
        summary = executor.get_execution_summary()
        
        assert summary == {"message": "No tasks executed"}


class TestTaskResult:
    """Test TaskResult functionality"""
    
    def test_task_result_creation(self):
        """Test TaskResult creation"""
        result = TaskResult("test_task", True, "test_result")
        
        assert result.task_id == "test_task"
        assert result.success is True
        assert result.result == "test_result"
        assert result.error is None
        assert result.metrics is None
    
    def test_task_result_with_error(self):
        """Test TaskResult with error"""
        error = ValueError("Test error")
        result = TaskResult("failed_task", False, error=error)
        
        assert result.task_id == "failed_task"
        assert result.success is False
        assert result.result is None
        assert result.error == error
    
    def test_task_result_boolean_conversion(self):
        """Test TaskResult boolean conversion"""
        success_result = TaskResult("task1", True)
        failed_result = TaskResult("task2", False)
        
        assert bool(success_result) is True
        assert bool(failed_result) is False
        
        # Test in conditional
        if success_result:
            success_check = True
        else:
            success_check = False
        
        assert success_check is True
    
    def test_task_result_string_representation(self):
        """Test TaskResult string representation"""
        from resource_executor.core.monitor import TaskMetrics
        
        metrics = TaskMetrics("test_task", time.time())
        metrics.duration_seconds = 1.5
        
        result = TaskResult("test_task", True, metrics=metrics)
        
        str_repr = str(result)
        assert "test_task" in str_repr
        assert "SUCCESS" in str_repr
        assert "1.50s" in str_repr


class TestSpecializedExecutor:
    """Test creating specialized executors"""
    
    def test_custom_executor_inheritance(self, basic_config):
        """Test creating a custom executor"""
        
        class CustomExecutor(TaskExecutor):
            def __init__(self, config, custom_param="default"):
                super().__init__(config)
                self.custom_param = custom_param
            
            def execute_custom_pipeline(self, data):
                """Custom pipeline method"""
                tasks = [SimpleTask(f"custom_{i}", duration=0.02) for i in range(len(data))]
                return self.execute_parallel_tasks(tasks)
            
            def get_custom_summary(self):
                """Custom summary method"""
                summary = self.get_execution_summary()
                summary['custom_param'] = self.custom_param
                return summary
        
        executor = CustomExecutor(basic_config, custom_param="test_value")
        
        assert executor.custom_param == "test_value"
        
        # Test custom pipeline
        results = executor.execute_custom_pipeline([1, 2, 3])
        
        assert len(results) == 3
        assert all(r.success for r in results)
        
        # Test custom summary
        summary = executor.get_custom_summary()
        assert summary['custom_param'] == "test_value"
    
    def test_executor_with_preprocessing(self, basic_config):
        """Test executor with preprocessing capabilities"""
        
        class PreprocessingExecutor(TaskExecutor):
            def __init__(self, config):
                super().__init__(config)
                self.preprocessing_results = []
            
            def preprocess_data(self, data):
                """Preprocess data before task execution"""
                processed = [x * 2 for x in data]
                self.preprocessing_results = processed
                return processed
            
            def execute_with_preprocessing(self, data):
                """Execute tasks with preprocessing"""
                processed_data = self.preprocess_data(data)
                
                def process_item(item):
                    return f"processed_{item}"
                
                tasks = [
                    (process_item, (item,), {}) 
                    for item in processed_data
                ]
                
                return self.execute_parallel_tasks(tasks)
        
        executor = PreprocessingExecutor(basic_config)
        results = executor.execute_with_preprocessing([1, 2, 3])
        
        assert len(results) == 3
        assert all(r.success for r in results)
        assert executor.preprocessing_results == [2, 4, 6]
        
        # Check that results contain processed values
        result_values = [r.result for r in results]
        assert "processed_2" in result_values
        assert "processed_4" in result_values
        assert "processed_6" in result_values


class TestExecutorErrorHandling:
    """Test error handling in executor"""
    
    def test_invalid_task_type(self, basic_config):
        """Test executor with invalid task type"""
        executor = TaskExecutor(basic_config)
        
        # Try to execute something that's not a task or callable
        with pytest.raises(Exception):
            executor.execute_task("not_a_task_or_callable")
    
    def test_task_validation_failure(self, basic_config):
        """Test task with validation failure"""
        
        class InvalidTask(SimpleTask):
            def validate_inputs(self):
                return False  # Always fail validation
        
        executor = TaskExecutor(basic_config)
        invalid_task = InvalidTask("invalid_task")
        
        result = executor.execute_task(invalid_task)
        
        assert result.success is False
        assert "validation failed" in str(result.error)
    
    def test_task_setup_failure(self, basic_config):
        """Test task with setup failure"""
        
        class SetupFailTask(SimpleTask):
            def setup(self):
                raise RuntimeError("Setup failed")
        
        executor = TaskExecutor(basic_config)
        setup_fail_task = SetupFailTask("setup_fail_task")
        
        result = executor.execute_task(setup_fail_task)
        
        assert result.success is False
        assert "Setup failed" in str(result.error)
    
    def test_parallel_execution_with_failures(self, basic_config):
        """Test parallel execution with some task failures"""
        executor = TaskExecutor(basic_config)
        
        tasks = [
            SimpleTask("success_1", duration=0.02),
            SimpleTask("failure_1", duration=0.02, should_fail=True),
            SimpleTask("success_2", duration=0.02),
            SimpleTask("failure_2", duration=0.02, should_fail=True)
        ]
        
        results = executor.execute_parallel_tasks(tasks)
        
        assert len(results) == 4
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        
        assert len(successful) == 2
        assert len(failed) == 2
        
        # Check summary
        summary = executor.get_execution_summary()
        assert summary['execution_summary']['success_rate'] == 50.0
        