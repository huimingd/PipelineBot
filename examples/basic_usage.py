#!/usr/bin/env python3
"""
Basic usage example of the Resource Execution Framework
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from resource_executor import (
    TaskExecutor, ResourceConfig, BaseTask,
    CPUIntensiveTask, MemoryIntensiveTask
)

def main():
    print("=== Basic Usage Example ===\n")
    
    # Create resource configuration
    config = ResourceConfig(
        cpus=2,
        memory_gb=1.0,
        max_processes=2,
        timeout_seconds=60
    )
    
    # Create executor
    executor = TaskExecutor(config)
    
    # Example 1: Execute a simple CPU task
    print("1. Executing CPU-intensive task...")
    cpu_task = CPUIntensiveTask("demo_cpu", duration=5, intensity=0.7)
    result = executor.execute_task(cpu_task)
    
    print(f"   Success: {result.success}")
    print(f"   Duration: {result.metrics.duration_seconds:.2f}s")
    print(f"   Peak Memory: {result.metrics.peak_memory_mb:.1f}MB")
    
    # Example 2: Execute a memory task
    print("\n2. Executing memory-intensive task...")
    memory_task = MemoryIntensiveTask("demo_memory", memory_mb=50, duration=3)
    result = executor.execute_task(memory_task)
    
    print(f"   Success: {result.success}")
    print(f"   Duration: {result.metrics.duration_seconds:.2f}s")
    print(f"   Peak Memory: {result.metrics.peak_memory_mb:.1f}MB")
    
    # Example 3: Get execution summary
    print("\n3. Execution Summary:")
    summary = executor.get_execution_summary()
    exec_summary = summary['execution_summary']
    
    print(f"   Total Tasks: {exec_summary['total_tasks']}")
    print(f"   Success Rate: {exec_summary['success_rate']:.1f}%")
    print(f"   Average Duration: {exec_summary['average_duration_seconds']:.2f}s")
    
    # Save results
    executor.save_results("basic_usage_results.json")
    print("\n   Results saved to basic_usage_results.json")

if __name__ == "__main__":
    main()
	
	