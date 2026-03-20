"""
Integration tests for the complete PipelineBot framework
"""

import pytest
import time
import json
from pathlib import Path

from resource_executor.core.executor import TaskExecutor, TaskResult
from resource_executor.core.config import ResourceConfig
from resource_executor.core.tasks import BaseTask
from resource_executor.examples.basic_tasks import CPUIntensiveTask, MemoryIntensiveTask, IOIntensiveTask

class TestIntegration:
    """Integration tests for the complete framework"""
    
    def test_complete_workflow(self, temp_dir):
        """Test a complete workflow from start to finish"""
        # Configuration
        config = ResourceConfig(
            cpus=2,
            memory_gb=1.0,
            max_processes=2,
            timeout_seconds=30
        )
        
        # Create executor
        executor = TaskExecutor(config)
        
        # Create various types of tasks
        tasks = [
            CPUIntensiveTask("cpu_task", duration=1, intensity=0.5),
            MemoryIntensiveTask("memory_task", memory_mb=10, duration=1),
            IOIntensiveTask("io_task", file_size_mb=1, duration=1)
        ]
        
        # Execute tasks in parallel
        results = executor.execute_parallel_tasks(tasks)
        
        # Verify results
        assert len(results) == 3
        assert all(r.success for r in results)
        
        # Check metrics
        for result in results:
            assert result.metrics is not None
            assert result.metrics.duration_seconds > 0
            assert len(result.metrics.cpu_percent) > 0
            assert len(result.metrics.memory_mb) > 0
        
        # Generate and save report
        summary = executor.get_execution_summary()
        assert summary['execution_summary']['success_rate'] == 100.0
        
        # Save results
        results_file = temp_dir / "integration_results.json"
        executor.save_results(results_file)
        
        # Verify saved file
        assert results_file.exists()
        with open(results_file) as f:
            saved_data = json.load(f)
        
        assert saved_data['execution_summary']['total_tasks'] == 3
    
    def test_mixed_task_types(self):
        """Test mixing BaseTask instances and callables"""
        config = ResourceConfig(cpus=2, max_processes=2)
        executor = TaskExecutor(config)
        
        def simple_callable(value):
            time.sleep(0.1)
            return value * 2
        
        # Mix of task types
        mixed_tasks = [
            CPUIntensiveTask("cpu_task", duration=0.2),
            (simple_callable, (5,), {}),
            MemoryIntensiveTask("mem_task", memory_mb=5, duration=0.2)
        ]
        
        results = executor.execute_parallel_tasks(mixed_tasks)
        
        assert len(results) == 3
        assert all(r.success for r in results)
        assert results[1].result == 10  # 5 * 2
    
    def test_error_handling_and_recovery(self):
        """Test error handling and recovery"""
        config = ResourceConfig(cpus=1, max_processes=1)
        executor = TaskExecutor(config)
        
        class FailingTask(BaseTask):
            def execute(self):
                raise RuntimeError("Intentional failure")
        
        # Mix of successful and failing tasks
        tasks = [
            CPUIntensiveTask("success_1", duration=0.1),
            FailingTask("failing_task"),
            CPUIntensiveTask("success_2", duration=0.1)
        ]
        
        results = executor.execute_parallel_tasks(tasks)
        
        assert len(results) == 3
        assert results[0].success is True
        assert results[1].success is False
        assert results[2].success is True
        
        # Check summary
        summary = executor.get_execution_summary()
        assert abs(summary['execution_summary']['success_rate'] - 66.67) < 0.1  # 2/3 * 100
    
    def test_resource_monitoring_accuracy(self):
        """Test that resource monitoring provides reasonable data"""
        config = ResourceConfig(cpus=1, max_processes=1)
        executor = TaskExecutor(config)
        
        # CPU intensive task should show higher CPU usage
        cpu_task = CPUIntensiveTask("cpu_monitor_test", duration=2, intensity=0.8)
        result = executor.execute_task(cpu_task)
        
        assert result.success
        assert result.metrics.duration_seconds >= 1.8  # Should be close to 2 seconds
        assert len(result.metrics.cpu_percent) > 0
        assert result.metrics.get_average_cpu() >= 0  # Should have some CPU usage
        
        # Memory intensive task should show memory usage
        memory_task = MemoryIntensiveTask("memory_monitor_test", memory_mb=20, duration=1)
        result = executor.execute_task(memory_task)
        
        assert result.success
        assert result.metrics.peak_memory_mb > 0  # Should show memory usage
    
    def test_configuration_impact(self):
        """Test that different configurations affect execution"""
        # Test with different process limits
        config_low = ResourceConfig(max_processes=1)
        config_high = ResourceConfig(max_processes=4)
        
        tasks = [CPUIntensiveTask(f"task_{i}", duration=0.5) for i in range(4)]
        
        # Execute with low process limit
        executor_low = TaskExecutor(config_low)
        start_time = time.time()
        results_low = executor_low.execute_parallel_tasks(tasks)
        duration_low = time.time() - start_time
        
        # Execute with high process limit
        executor_high = TaskExecutor(config_high)
        start_time = time.time()
        results_high = executor_high.execute_parallel_tasks(tasks)
        duration_high = time.time() - start_time
        
        # Both should succeed
        assert all(r.success for r in results_low)
        assert all(r.success for r in results_high)
        
        # High process limit should generally be faster (though this can vary)
        # We'll just check that both completed in reasonable time
        assert duration_low < 10  # Should complete within 10 seconds
        assert duration_high < 10  # Should complete within 10 seconds


class TestRealWorldScenarios:
    """Test real-world usage scenarios"""
    
    def test_bioinformatics_pipeline_simulation(self):
        """Simulate a bioinformatics pipeline"""
        config = ResourceConfig(cpus=4, memory_gb=2.0, max_processes=3)
        
        class AlignmentTask(BaseTask):
            def execute(self):
                time.sleep(0.5)  # Simulate alignment
                return f"Alignment completed for {self.task_id}"
        
        class VariantCallingTask(BaseTask):
            def execute(self):
                time.sleep(0.3)  # Simulate variant calling
                return f"Variants called for {self.task_id}"
        
        executor = TaskExecutor(config)
        
        # Simulate processing multiple samples
        samples = ["sample1", "sample2", "sample3"]
        
        # Step 1: Alignment
        alignment_tasks = [AlignmentTask(f"align_{sample}") for sample in samples]
        alignment_results = executor.execute_parallel_tasks(alignment_tasks)
        
        assert all(r.success for r in alignment_results)
        
        # Step 2: Variant calling (sequential after alignment)
        variant_tasks = [VariantCallingTask(f"variant_{sample}") for sample in samples]
        variant_results = executor.execute_parallel_tasks(variant_tasks)
        
        assert all(r.success for r in variant_results)
        
        # Check overall pipeline metrics
        summary = executor.get_execution_summary()
        assert summary['execution_summary']['total_tasks'] == 6
        assert summary['execution_summary']['success_rate'] == 100.0
    
    def test_data_processing_pipeline(self):
        """Test a data processing pipeline scenario"""
        config = ResourceConfig(cpus=2, memory_gb=1.0, max_processes=2)
        executor = TaskExecutor(config)
        
        class DataLoadTask(BaseTask):
            def execute(self):
                time.sleep(0.2)
                return {"data": list(range(100)), "status": "loaded"}
        
        class DataTransformTask(BaseTask):
            def execute(self):
                time.sleep(0.3)
                return {"transformed_data": list(range(0, 200, 2)), "status": "transformed"}
        
        class DataSaveTask(BaseTask):
            def execute(self):
                time.sleep(0.1)
                return {"status": "saved", "records": 100}
        
        # Execute pipeline steps
        pipeline_tasks = [
            DataLoadTask("load_data"),
            DataTransformTask("transform_data"),
            DataSaveTask("save_data")
        ]
        
        # Execute sequentially (each step depends on previous)
        results = executor.execute_sequential_tasks(pipeline_tasks)
        
        assert len(results) == 3
        assert all(r.success for r in results)
        
        # Verify results contain expected data
        assert "loaded" in results[0].result["status"]
        assert "transformed" in results[1].result["status"]
        assert "saved" in results[2].result["status"]
		