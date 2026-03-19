"""
Tests for TaskExecutor class - the core execution engine
"""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from concurrent.futures import Future

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
        
        result = executor.execute_task(simple_function, "math_task", False, 5, 3)
        
        assert result.success is True
        assert result.result == 8
        assert result.task_id == "math_task"
    
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
    
    def test_save_results(self, basic_config, temp_dir):
        """Test saving execution results"""
        executor = TaskExecutor(basic_config)
        executor.execute_task(SimpleTask("save_test", duration=0.05))
        
        json_file = temp_dir / "results.json"
        executor.save_results(json_file, format="json")
        
        assert json_file.exists()
        
        # Test pickle format
        pickle_file = temp_dir / "results.pkl"
        executor.save_results(pickle_file, format="pickle")
        
        assert pickle_file.exists()
    
    @patch('resource_executor.core.executor.signal')
    def test_timeout_handling(self, mock_signal, basic_config):
        """Test task timeout handling"""
        executor = TaskExecutor(basic_config)
        
        def long_running_task():
            time.sleep(2)  # Longer than timeout
            return "completed"
        
        # Mock signal for timeout
        mock_signal.SIGALRM = 14
        mock_signal.alarm = Mock()
        mock_signal.signal = Mock()
        
        result = executor.execute_task(long_running_task, "timeout_task", True, 1)  # 1 second timeout
        
        # The actual timeout behavior depends on the implementation
        # This test ensures the timeout mechanism is called
        assert mock_signal.alarm.called


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
    
    def test_task_result_boolean_conversion(self):
        """Test TaskResult boolean conversion"""
        success_result = TaskResult("task1", True)
        failed_result = TaskResult("task2", False)
        
        assert bool(success_result) is True
        assert bool(failed_result) is False
    
    def test_task_result_string_representation(self):
        """Test TaskResult string representation"""
        result = TaskResult("test_task", True)
        
        str_repr = str(result)
        assert "test_task" in str_repr
        assert "SUCCESS" in str_repr


class TestSpecializedExecutor:
    """Test creating specialized executors"""
    
    def test_custom_executor_inheritance(self, basic_config):
        """Test creating a custom executor"""
        
        class CustomExecutor(TaskExecutor):
            def execute_custom_pipeline(self, data):
                tasks = [SimpleTask(f"custom_{i}") for i in range(len(data))]
                return self.execute_parallel_tasks(tasks)
        
        executor = CustomExecutor(basic_config)
        results = executor.execute_custom_pipeline([1, 2, 3])
        
        assert len(results) == 3
        assert all(r.success for r in results)
		