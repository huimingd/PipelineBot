"""
Task execution classes and utilities.
"""

import time
import logging
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Callable, Union
from pathlib import Path
import json
import pickle

from .config import ResourceConfig
from .monitor import ResourceMonitor, TaskMetrics, SystemResourceMonitor, ResourceThresholdMonitor
from .tasks import BaseTask

logger = logging.getLogger(__name__)

@dataclass
class TaskResult:
    """Result of task execution"""
    task_id: str
    success: bool
    result: Any = None
    error: Optional[Exception] = None
    metrics: Optional[TaskMetrics] = None
    
    def __bool__(self):
        return self.success
    
    def __str__(self):
        status = "SUCCESS" if self.success else "FAILED"
        duration = f"{self.metrics.duration_seconds:.2f}s" if self.metrics else "N/A"
        return f"TaskResult({self.task_id}: {status}, Duration: {duration})"

class TaskExecutor:
    """Base executor class that can be inherited for specific task execution"""
    
    def __init__(self, config: ResourceConfig, logger_instance: Optional[logging.Logger] = None):
        """
        Initialize task executor
        
        Args:
            config: Resource configuration
            logger_instance: Optional logger instance
        """
        self.config = config
        self.logger = logger_instance or logging.getLogger(self.__class__.__name__)
        self.executed_tasks: List[TaskResult] = []
        self._task_counter = 0
        self.system_monitor = SystemResourceMonitor()
        self.threshold_monitor = ResourceThresholdMonitor()
        
        # Setup threshold callbacks
        self.threshold_monitor.add_callback(self._threshold_violation_callback)
        
    def execute_task(self, task: Union[BaseTask, Callable], task_id: Optional[str] = None, 
        monitor: bool = True, timeout: Optional[int] = None, *args, **kwargs) -> TaskResult:
        """
        Execute a single task with optional monitoring
        
        Args:
            task: Task to execute (BaseTask instance or callable)
            task_id: Optional task identifier
            monitor: Whether to monitor resource usage
            timeout: Task timeout in seconds (overrides config)
            *args: Arguments for callable tasks
            **kwargs: Keyword arguments for callable tasks
            
        Returns:
            TaskResult containing execution results and metrics
        """
        
        # Handle different task types
        if isinstance(task, BaseTask):
            actual_task_id = task.task_id
            task_func = task.execute
            task_args = ()
            task_kwargs = {}
            estimated_resources = task.get_estimated_resources()
        else:
            self._task_counter += 1
            actual_task_id = task_id or f"task_{self._task_counter}"
            task_func = task
            task_args = args
            task_kwargs = kwargs
            estimated_resources = None
        
        # Use provided timeout or config timeout
        actual_timeout = timeout or self.config.timeout_seconds
        
        self.logger.info(f"Starting task {actual_task_id}")
        if estimated_resources:
            self.logger.debug(f"Estimated resources for {actual_task_id}: {estimated_resources}")
        
        # Initialize monitoring
        monitor_obj = ResourceMonitor(actual_task_id, interval=0.5) if monitor else None
        
        try:
            # Setup task if it's a BaseTask
            if isinstance(task, BaseTask):
                if not task.validate_inputs():
                    raise ValueError(f"Task {actual_task_id} input validation failed")
                task.setup()
            
            # Start monitoring
            if monitor_obj:
                monitor_obj.start_monitoring()
            
            # Execute the task with timeout handling
            start_time = time.time()
            try:
                # Simple timeout implementation for direct execution
                result = self._execute_with_timeout(task_func, actual_timeout, *task_args, **task_kwargs)
                
                task_result = TaskResult(
                    task_id=actual_task_id,
                    success=True,
                    result=result,
                    metrics=monitor_obj.metrics if monitor_obj else None
                )
                self.logger.info(f"Task {actual_task_id} completed successfully in {time.time() - start_time:.2f}s")
                
            except TimeoutError:
                task_result = TaskResult(
                    task_id=actual_task_id,
                    success=False,
                    error=TimeoutError(f"Task {actual_task_id} timed out after {actual_timeout}s"),
                    metrics=monitor_obj.metrics if monitor_obj else None
                )
                if monitor_obj:
                    monitor_obj.metrics.exit_code = 124  # Standard timeout exit code
                self.logger.error(f"Task {actual_task_id} timed out after {actual_timeout}s")
                
            except Exception as e:
                task_result = TaskResult(
                    task_id=actual_task_id,
                    success=False,
                    error=e,
                    metrics=monitor_obj.metrics if monitor_obj else None
                )
                if monitor_obj:
                    monitor_obj.metrics.exit_code = 1
                self.logger.error(f"Task {actual_task_id} failed: {e}")
                
        except Exception as e:
            task_result = TaskResult(
                task_id=actual_task_id,
                success=False,
                error=e
            )
            self.logger.error(f"Error setting up task {actual_task_id}: {e}")
            
        finally:
            # Stop monitoring and cleanup
            if monitor_obj:
                monitor_obj.stop_monitoring()
                # Check thresholds
                if monitor_obj.metrics:
                    self.threshold_monitor.check_thresholds(monitor_obj.metrics)
            
            if isinstance(task, BaseTask):
                try:
                    task.cleanup()
                except Exception as e:
                    self.logger.warning(f"Cleanup failed for task {actual_task_id}: {e}")
        
        self.executed_tasks.append(task_result)
        return task_result
    
    def execute_parallel_tasks(self, tasks: List[Union[BaseTask, tuple]], 
        monitor: bool = True, start_system_monitor: bool = True) -> List[TaskResult]:
        """
        Execute multiple tasks in parallel
        
        Args:
            tasks: List of tasks to execute (BaseTask instances or tuples)
            monitor: Whether to monitor individual task resources
            start_system_monitor: Whether to monitor overall system resources
            
        Returns:
            List of TaskResult objects
        """
        self.logger.info(f"Executing {len(tasks)} tasks in parallel (max_workers={self.config.max_processes})")
        
        # Start system monitoring if requested
        if start_system_monitor:
            self.system_monitor.start_monitoring()
        
        results = []
        executor_class = ProcessPoolExecutor if self.config.execution_mode == "process" else ThreadPoolExecutor
        
        try:
            with executor_class(max_workers=self.config.max_processes) as executor:
                # Submit all tasks
                future_to_task = {}
                
                for i, task_spec in enumerate(tasks):
                    if isinstance(task_spec, BaseTask):
                        task_id = task_spec.task_id
                        future = executor.submit(self._execute_single_task_wrapper, task_spec, monitor)
                    else:
                        # Handle tuple format (task_func, args, kwargs)
                        if len(task_spec) == 3:
                            task_func, task_args, task_kwargs = task_spec
                        elif len(task_spec) == 2:
                            task_func, task_args = task_spec
                            task_kwargs = {}
                        else:
                            raise ValueError(f"Invalid task specification format: {task_spec}")
                        
                        task_id = f"parallel_task_{i+1}"
                        future = executor.submit(
                            self._execute_callable_wrapper, 
                            task_func, task_id, task_args, task_kwargs, monitor
                        )
                    
                    future_to_task[future] = task_id
                
                # Collect results with timeout
                try:
                    for future in as_completed(future_to_task, timeout=self.config.timeout_seconds):
                        task_id = future_to_task[future]
                        try:
                            result = future.result()
                            results.append(result)
                            self.logger.debug(f"Parallel task {task_id} completed")
                        except Exception as e:
                            self.logger.error(f"Parallel task {task_id} failed with exception: {e}")
                            failed_result = TaskResult(
                                task_id=task_id,
                                success=False,
                                error=e
                            )
                            results.append(failed_result)
                            
                except TimeoutError:
                    self.logger.error(f"Parallel execution timed out after {self.config.timeout_seconds}s")
                    # Handle remaining futures
                    for future, task_id in future_to_task.items():
                        if not future.done():
                            future.cancel()
                            timeout_result = TaskResult(
                                task_id=task_id,
                                success=False,
                                error=TimeoutError(f"Task {task_id} cancelled due to overall timeout")
                            )
                            results.append(timeout_result)
                        elif future not in [f for f in as_completed(future_to_task, timeout=0)]:
                            # Future completed but we didn't process it
                            try:
                                result = future.result()
                                results.append(result)
                            except Exception as e:
                                failed_result = TaskResult(
                                    task_id=task_id,
                                    success=False,
                                    error=e
                                )
                                results.append(failed_result)
                    
        finally:
            # Stop system monitoring
            if start_system_monitor:
                self.system_monitor.stop_monitoring()
        
        # Add results to executed tasks
        self.executed_tasks.extend(results)
        
        successful = sum(1 for r in results if r.success)
        self.logger.info(f"Parallel execution completed: {successful}/{len(results)} tasks successful")
        
        return results
    
    def execute_sequential_tasks(self, tasks: List[Union[BaseTask, tuple]], 
        monitor: bool = True, stop_on_failure: bool = False) -> List[TaskResult]:
        """
        Execute tasks sequentially
        
        Args:
            tasks: List of tasks to execute
            monitor: Whether to monitor resource usage
            stop_on_failure: Whether to stop execution on first failure
            
        Returns:
            List of TaskResult objects
        """
        self.logger.info(f"Executing {len(tasks)} tasks sequentially")
        
        results = []
        
        for i, task_spec in enumerate(tasks):
            if isinstance(task_spec, BaseTask):
                result = self.execute_task(task_spec, monitor=monitor)
            else:
                # Handle tuple format
                if len(task_spec) == 3:
                    task_func, task_args, task_kwargs = task_spec
                elif len(task_spec) == 2:
                    task_func, task_args = task_spec
                    task_kwargs = {}
                else:
                    raise ValueError(f"Invalid task specification format: {task_spec}")
                
                task_id = f"sequential_task_{i+1}"
                result = self.execute_task(task_func, task_id, monitor, *task_args, **task_kwargs)
            
            results.append(result)
            
            if stop_on_failure and not result.success:
                self.logger.warning(f"Stopping sequential execution due to failure in {result.task_id}")
                break
        
        successful = sum(1 for r in results if r.success)
        self.logger.info(f"Sequential execution completed: {successful}/{len(results)} tasks successful")
        
        return results
    
    def _execute_with_timeout(self, func: Callable, timeout: int, *args, **kwargs):
        """Execute function with timeout (simple implementation)"""
        # Note: This is a basic implementation. For more robust timeout handling,
        # consider using signal (Unix) or threading with proper cancellation
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Function execution timed out after {timeout} seconds")
        
        # Set up timeout (Unix-like systems only)
        if hasattr(signal, 'SIGALRM'):
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout)
            
            try:
                result = func(*args, **kwargs)
                signal.alarm(0)  # Cancel the alarm
                return result
            finally:
                signal.signal(signal.SIGALRM, old_handler)
        else:
            # Fallback for systems without SIGALRM (like Windows)
            # This is less reliable but better than nothing
            return func(*args, **kwargs)
    
    def _execute_single_task_wrapper(self, task: BaseTask, monitor: bool) -> TaskResult:
        """Wrapper for executing BaseTask in parallel"""
        temp_executor = TaskExecutor(self.config, self.logger)
        return temp_executor.execute_task(task, monitor=monitor)
    
    def _execute_callable_wrapper(self, task_func: Callable, task_id: str, 
                                 args: tuple, kwargs: dict, monitor: bool) -> TaskResult:
        """Wrapper for executing callable in parallel"""
        temp_executor = TaskExecutor(self.config, self.logger)
        return temp_executor.execute_task(task_func, task_id, monitor, *args, **kwargs)
    
    def _threshold_violation_callback(self, violation: Dict[str, Any]):
        """Callback for threshold violations"""
        self.logger.warning(f"Resource threshold violation: {violation['type']} usage "
                          f"{violation['actual']:.1f}% exceeds threshold {violation['threshold']:.1f}% "
                          f"for task {violation['task_id']}")
    
    def get_execution_summary(self) -> Dict[str, Any]:
        """Get summary of all executed tasks"""
        if not self.executed_tasks:
            return {"message": "No tasks executed"}
        
        total_tasks = len(self.executed_tasks)
        successful_tasks = sum(1 for task in self.executed_tasks if task.success)
        failed_tasks = total_tasks - successful_tasks
        
        # Calculate metrics for successful tasks with metrics
        tasks_with_metrics = [task for task in self.executed_tasks if task.metrics]
        
        if tasks_with_metrics:
            total_duration = sum(task.metrics.duration_seconds for task in tasks_with_metrics)
            avg_duration = total_duration / len(tasks_with_metrics)
            
            avg_cpu = sum(
                sum(task.metrics.cpu_percent) / len(task.metrics.cpu_percent) 
                if task.metrics.cpu_percent else 0 
                for task in tasks_with_metrics
            ) / len(tasks_with_metrics)
            
            peak_memory = max(task.metrics.peak_memory_mb for task in tasks_with_metrics)
        else:
            total_duration = avg_duration = avg_cpu = peak_memory = 0
        
        return {
            "execution_summary": {
                "total_tasks": total_tasks,
                "successful_tasks": successful_tasks,
                "failed_tasks": failed_tasks,
                "success_rate": (successful_tasks / total_tasks * 100) if total_tasks > 0 else 0,
                "total_duration_seconds": total_duration,
                "average_duration_seconds": avg_duration,
                "average_cpu_percent": avg_cpu,
                "peak_memory_mb": peak_memory
            },
            "resource_config": {
                "cpus": self.config.cpus,
                "memory_gb": self.config.memory_gb,
                "max_processes": self.config.max_processes,
                "timeout_seconds": self.config.timeout_seconds,
                "execution_mode": self.config.execution_mode
            },
            "task_results": [
                {
                    "task_id": task.task_id,
                    "success": task.success,
                    "error": str(task.error) if task.error else None,
                    "metrics": task.metrics.to_dict() if task.metrics else None
                }
                for task in self.executed_tasks
            ]
        }
    
    def save_results(self, filepath: Union[str, Path], format: str = "json"):
        """Save execution results to file"""
        filepath = Path(filepath)
        
        if format.lower() == "json":
            with open(filepath, 'w') as f:
                json.dump(self.get_execution_summary(), f, indent=2)
        elif format.lower() == "pickle":
            with open(filepath, 'wb') as f:
                pickle.dump(self.executed_tasks, f)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        self.logger.info(f"Results saved to {filepath}")
    
    def clear_results(self):
        """Clear all execution results"""
        self.executed_tasks.clear()
        self._task_counter = 0
