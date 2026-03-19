#!/usr/bin/env python3
"""
Resource Execution Framework - Inheritable Classes

This module provides a framework of classes that can be inherited to execute
any tasks with resource monitoring and configuration management.
"""

import os
import sys
import time
import psutil
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Callable, Union
from abc import ABC, abstractmethod
import logging
from datetime import datetime
import json
import pickle
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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

@dataclass
class TaskResult:
    """Result of task execution"""
    task_id: str
    success: bool
    result: Any = None
    error: Optional[Exception] = None
    metrics: Optional['TaskMetrics'] = None
    
    def __bool__(self):
        return self.success

@dataclass
class TaskMetrics:
    """Metrics collected during task execution"""
    task_id: str
    start_time: float
    end_time: Optional[float] = None
    cpu_percent: List[float] = field(default_factory=list)
    memory_mb: List[float] = field(default_factory=list)
    peak_memory_mb: float = 0.0
    exit_code: int = 0
    duration_seconds: float = 0.0
    custom_metrics: Dict[str, Any] = field(default_factory=dict)
    
    def finalize(self):
        """Finalize metrics calculation"""
        if self.end_time is None:
            self.end_time = time.time()
        self.duration_seconds = self.end_time - self.start_time
        if self.memory_mb:
            self.peak_memory_mb = max(self.memory_mb)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary for serialization"""
        return {
            'task_id': self.task_id,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration_seconds': self.duration_seconds,
            'avg_cpu_percent': sum(self.cpu_percent) / len(self.cpu_percent) if self.cpu_percent else 0,
            'peak_cpu_percent': max(self.cpu_percent) if self.cpu_percent else 0,
            'avg_memory_mb': sum(self.memory_mb) / len(self.memory_mb) if self.memory_mb else 0,
            'peak_memory_mb': self.peak_memory_mb,
            'exit_code': self.exit_code,
            'custom_metrics': self.custom_metrics
        }

class ResourceMonitor:
    """Monitor system resources during task execution"""
    
    def __init__(self, task_id: str, interval: float = 1.0):
        self.task_id = task_id
        self.interval = interval
        self.metrics = TaskMetrics(task_id=task_id, start_time=time.time())
        self.monitoring = False
        self.monitor_thread = None
        self._lock = threading.Lock()
        
    def start_monitoring(self, process: Optional[psutil.Process] = None):
        """Start monitoring the given process or current process"""
        if process is None:
            try:
                process = psutil.Process()
            except psutil.NoSuchProcess:
                logging.warning(f"Cannot monitor process for task {self.task_id}")
                return
                
        with self._lock:
            if self.monitoring:
                return
            self.monitoring = True
            
        self.monitor_thread = threading.Thread(
            target=self._monitor_process, 
            args=(process,),
            daemon=True
        )
        self.monitor_thread.start()
        
    def stop_monitoring(self):
        """Stop monitoring and finalize metrics"""
        with self._lock:
            self.monitoring = False
            
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5.0)
        
        self.metrics.finalize()
            
    def add_custom_metric(self, key: str, value: Any):
        """Add custom metric"""
        self.metrics.custom_metrics[key] = value
        
    def _monitor_process(self, process: psutil.Process):
        """Internal method to monitor process resources"""
        while self.monitoring:
            try:
                cpu_percent = process.cpu_percent()
                memory_info = process.memory_info()
                memory_mb = memory_info.rss / (1024 * 1024)
                
                with self._lock:
                    if self.monitoring:  # Double-check while holding lock
                        self.metrics.cpu_percent.append(cpu_percent)
                        self.metrics.memory_mb.append(memory_mb)
                
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break
            except Exception as e:
                logging.warning(f"Error monitoring process for task {self.task_id}: {e}")
                
            time.sleep(self.interval)

class BaseTask(ABC):
    """Abstract base class for all tasks"""
    
    def __init__(self, task_id: str, **kwargs):
        self.task_id = task_id
        self.kwargs = kwargs
        self.logger = logging.getLogger(f"{self.__class__.__name__}.{task_id}")
        
    @abstractmethod
    def execute(self) -> Any:
        """Execute the task - must be implemented by subclasses"""
        pass
    
    def validate_inputs(self) -> bool:
        """Validate task inputs - can be overridden by subclasses"""
        return True
    
    def setup(self):
        """Setup before execution - can be overridden by subclasses"""
        pass
    
    def cleanup(self):
        """Cleanup after execution - can be overridden by subclasses"""
        pass
    
    def get_estimated_resources(self) -> ResourceConfig:
        """Get estimated resource requirements - can be overridden"""
        return ResourceConfig()

class TaskExecutor:
    """Base executor class that can be inherited for specific task execution"""
    
    def __init__(self, config: ResourceConfig, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.executed_tasks: List[TaskResult] = []
        self._task_counter = 0
        
    def execute_task(self, task: Union[BaseTask, Callable], task_id: Optional[str] = None, 
                    monitor: bool = True, *args, **kwargs) -> TaskResult:
        """Execute a single task with optional monitoring"""
        
        # Handle different task types
        if isinstance(task, BaseTask):
            actual_task_id = task.task_id
            task_func = task.execute
            task_args = ()
            task_kwargs = {}
        else:
            self._task_counter += 1
            actual_task_id = task_id or f"task_{self._task_counter}"
            task_func = task
            task_args = args
            task_kwargs = kwargs
        
        self.logger.info(f"Starting task {actual_task_id}")
        
        # Initialize monitoring
        monitor_obj = ResourceMonitor(actual_task_id) if monitor else None
        
        try:
            # Setup task if it's a BaseTask
            if isinstance(task, BaseTask):
                if not task.validate_inputs():
                    raise ValueError(f"Task {actual_task_id} input validation failed")
                task.setup()
            
            # Start monitoring
            if monitor_obj:
                monitor_obj.start_monitoring()
            
            # Execute the task
            start_time = time.time()
            try:
                result = task_func(*task_args, **task_kwargs)
                task_result = TaskResult(
                    task_id=actual_task_id,
                    success=True,
                    result=result,
                    metrics=monitor_obj.metrics if monitor_obj else None
                )
                self.logger.info(f"Task {actual_task_id} completed successfully")
                
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
            
            if isinstance(task, BaseTask):
                try:
                    task.cleanup()
                except Exception as e:
                    self.logger.warning(f"Cleanup failed for task {actual_task_id}: {e}")
        
        self.executed_tasks.append(task_result)
        return task_result
    
    def execute_parallel_tasks(self, tasks: List[Union[BaseTask, tuple]], 
                             monitor: bool = True) -> List[TaskResult]:
        """Execute multiple tasks in parallel"""
        self.logger.info(f"Executing {len(tasks)} tasks in parallel (max_workers={self.config.max_processes})")
        
        results = []
        executor_class = ProcessPoolExecutor if self.config.execution_mode == "process" else ThreadPoolExecutor
        
        with executor_class(max_workers=self.config.max_processes) as executor:
            # Submit all tasks
            future_to_task = {}
            
            for i, task_spec in enumerate(tasks):
                if isinstance(task_spec, BaseTask):
                    task_id = task_spec.task_id
                    future = executor.submit(self._execute_single_task_wrapper, task_spec, monitor)
                else:
                    # Handle tuple format (task_func, args, kwargs)
                    task_func, task_args, task_kwargs = task_spec
                    task_id = f"parallel_task_{i+1}"
                    future = executor.submit(
                        self._execute_callable_wrapper, 
                        task_func, task_id, task_args, task_kwargs, monitor
                    )
                
                future_to_task[future] = task_id
            
            # Collect results
            for future in as_completed(future_to_task, timeout=self.config.timeout_seconds):
                task_id = future_to_task[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    self.logger.error(f"Task {task_id} failed with exception: {e}")
                    failed_result = TaskResult(
                        task_id=task_id,
                        success=False,
                        error=e
                    )
                    results.append(failed_result)
                    
        return results
    
    def _execute_single_task_wrapper(self, task: BaseTask, monitor: bool) -> TaskResult:
        """Wrapper for executing BaseTask in parallel"""
        temp_executor = TaskExecutor(self.config, self.logger)
        return temp_executor.execute_task(task, monitor=monitor)
    
    def _execute_callable_wrapper(self, task_func: Callable, task_id: str, 
                                 args: tuple, kwargs: dict, monitor: bool) -> TaskResult:
        """Wrapper for executing callable in parallel"""
        temp_executor = TaskExecutor(self.config, self.logger)
        return temp_executor.execute_task(task_func, task_id, monitor, *args, **kwargs)
    
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

# Example task implementations
class CPUIntensiveTask(BaseTask):
    """Example CPU-intensive task"""
    
    def __init__(self, task_id: str, duration: int = 10, intensity: float = 0.8):
        super().__init__(task_id, duration=duration, intensity=intensity)
        self.duration = duration
        self.intensity = intensity
    
    def execute(self) -> str:
        self.logger.info(f"Starting CPU-intensive computation for {self.duration} seconds")
        
        end_time = time.time() + self.duration
        iterations = 0
        
        while time.time() < end_time:
            # CPU-intensive computation
            compute_time = self.intensity
            idle_time = 1.0 - self.intensity
            
            start = time.time()
            while time.time() - start < compute_time:
                _ = sum(i * i for i in range(1000))
                iterations += 1
            
            time.sleep(idle_time)
        
        result = f"CPU task completed: {iterations} iterations in {self.duration} seconds"
        self.logger.info(result)
        return result
    
    def get_estimated_resources(self) -> ResourceConfig:
        return ResourceConfig(cpus=1, memory_gb=0.1)

class MemoryIntensiveTask(BaseTask):
    """Example memory-intensive task"""
    
    def __init__(self, task_id: str, memory_mb: int = 100, duration: int = 10):
        super().__init__(task_id, memory_mb=memory_mb, duration=duration)
        self.memory_mb = memory_mb
        self.duration = duration
        self.allocated_data = []
    
    def execute(self) -> str:
        self.logger.info(f"Allocating {self.memory_mb}MB of memory for {self.duration} seconds")
        
        # Allocate memory in chunks
        chunk_size = 1024 * 1024  # 1MB chunks
        
        for i in range(self.memory_mb):
            chunk = bytearray(chunk_size)
            # Fill with data to ensure actual allocation
            for j in range(0, chunk_size, 1024):
                chunk[j:j+100] = b'x' * 100
            self.allocated_data.append(chunk)
        
        self.logger.info(f"Memory allocated, holding for {self.duration} seconds")
        time.sleep(self.duration)
        
        result = f"Memory task completed: {self.memory_mb}MB for {self.duration}s"
        self.logger.info(result)
        return result
    
    def cleanup(self):
        """Clean up allocated memory"""
        self.allocated_data.clear()
        self.logger.info("Memory cleaned up")
    
    def get_estimated_resources(self) -> ResourceConfig:
        return ResourceConfig(cpus=1, memory_gb=self.memory_mb / 1024)

class IOIntensiveTask(BaseTask):
    """Example I/O intensive task"""
    
    def __init__(self, task_id: str, file_size_mb: int = 10, duration: int = 5):
        super().__init__(task_id, file_size_mb=file_size_mb, duration=duration)
        self.file_size_mb = file_size_mb
        self.duration = duration
        self.temp_file = None
    
    def execute(self) -> str:
        self.logger.info(f"Starting I/O operations: {self.file_size_mb}MB")
        
        self.temp_file = f"temp_io_{self.task_id}_{int(time.time())}.dat"
        
        # Write data
        with open(self.temp_file, 'wb') as f:
            chunk = b'x' * (1024 * 1024)  # 1MB chunk
            for i in range(self.file_size_mb):
                f.write(chunk)
        
        # Read data back
        with open(self.temp_file, 'rb') as f:
            while f.read(1024 * 1024):
                pass
        
        # Additional processing time
        time.sleep(self.duration)
        
        result = f"I/O task completed: {self.file_size_mb}MB"
        self.logger.info(result)
        return result
    
    def cleanup(self):
        """Clean up temporary files"""
        if self.temp_file and os.path.exists(self.temp_file):
            os.remove(self.temp_file)
            self.logger.info(f"Temporary file {self.temp_file} removed")

# Custom executor example
class BioinformaticsExecutor(TaskExecutor):
    """Example of a specialized executor for bioinformatics tasks"""
    
    def __init__(self, config: ResourceConfig, reference_genome: Optional[str] = None):
        super().__init__(config)
        self.reference_genome = reference_genome
        self.logger = logging.getLogger("BioinformaticsExecutor")
    
    def execute_alignment_pipeline(self, fastq_files: List[str]) -> List[TaskResult]:
        """Execute a series of alignment tasks"""
        tasks = []
        
        for i, fastq_file in enumerate(fastq_files):
            # Create alignment task (this would be your actual alignment implementation)
            task = CPUIntensiveTask(f"alignment_{i}", duration=30, intensity=0.9)
            tasks.append(task)
        
        return self.execute_parallel_tasks(tasks)
    
    def get_pipeline_summary(self) -> Dict[str, Any]:
        """Get specialized summary for bioinformatics pipeline"""
        summary = self.get_execution_summary()
        summary["pipeline_type"] = "bioinformatics"
        summary["reference_genome"] = self.reference_genome
        return summary

# Usage example and demonstration
def main():
    """Demonstrate the usage of the inheritable task execution framework"""
    print("=== Inheritable Task Execution Framework Demo ===\n")
    
    # Create different configurations
    configs = [
        ResourceConfig(cpus=2, memory_gb=1.0, max_processes=2, execution_mode="thread"),
        ResourceConfig(cpus=4, memory_gb=2.0, max_processes=4, execution_mode="process"),
    ]
    
    for i, config in enumerate(configs, 1):
        print(f"\n--- Configuration {i}: {config.execution_mode} mode ---")
        
        # Create executor
        executor = TaskExecutor(config)
        
        # Example 1: Execute individual tasks
        print("\n1. Individual Task Execution:")
        
        cpu_task = CPUIntensiveTask("cpu_demo", duration=5, intensity=0.7)
        result1 = executor.execute_task(cpu_task)
        print(f"CPU Task Result: {result1.success}")
        
        memory_task = MemoryIntensiveTask("memory_demo", memory_mb=50, duration=3)
        result2 = executor.execute_task(memory_task)
        print(f"Memory Task Result: {result2.success}")
        
        # Example 2: Execute parallel tasks
        print("\n2. Parallel Task Execution:")
        
        parallel_tasks = [
            CPUIntensiveTask("parallel_cpu_1", duration=3, intensity=0.6),
            CPUIntensiveTask("parallel_cpu_2", duration=4, intensity=0.8),
            MemoryIntensiveTask("parallel_mem_1", memory_mb=30, duration=3),
            IOIntensiveTask("parallel_io_1", file_size_mb=5, duration=2)
        ]
        
        parallel_results = executor.execute_parallel_tasks(parallel_tasks)
        successful = sum(1 for r in parallel_results if r.success)
        print(f"Parallel execution: {successful}/{len(parallel_results)} tasks successful")
        
        # Example 3: Mixed callable and BaseTask execution
        print("\n3. Mixed Task Types:")
        
        def simple_function(x, y):
            time.sleep(2)
            return x + y
        
        mixed_tasks = [
            CPUIntensiveTask("mixed_cpu", duration=2),
            (simple_function, (10, 20), {}),
            MemoryIntensiveTask("mixed_mem", memory_mb=20, duration=2)
        ]
        
        mixed_results = executor.execute_parallel_tasks(mixed_tasks)
        print(f"Mixed execution completed: {len(mixed_results)} tasks")
        
        # Generate and save report
        summary = executor.get_execution_summary()
        print(f"\n4. Execution Summary:")
        print(f"  Total Tasks: {summary['execution_summary']['total_tasks']}")
        print(f"  Success Rate: {summary['execution_summary']['success_rate']:.1f}%")
        print(f"  Peak Memory: {summary['execution_summary']['peak_memory_mb']:.1f}MB")
        
        # Save results
        executor.save_results(f"execution_results_config_{i}.json")
        
        print("-" * 60)
    
    # Example 4: Custom specialized executor
    print("\n--- Specialized Executor Example ---")
    
    bio_config = ResourceConfig(cpus=4, memory_gb=4.0, max_processes=3)
    bio_executor = BioinformaticsExecutor(bio_config, reference_genome="hg38")
    
    # Simulate alignment pipeline
    fastq_files = ["sample1.fastq", "sample2.fastq", "sample3.fastq"]
    alignment_results = bio_executor.execute_alignment_pipeline(fastq_files)
    
    pipeline_summary = bio_executor.get_pipeline_summary()
    print(f"Bioinformatics pipeline completed: {len(alignment_results)} alignments")
    print(f"Pipeline type: {pipeline_summary['pipeline_type']}")

if __name__ == "__main__":
    main()
	
	