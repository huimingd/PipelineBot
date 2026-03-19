# Create multiple tasks
tasks = [MyTask(f"task_{i}") for i in range(5)]

# Execute in parallel
results = executor.execute_parallel_tasks(tasks)

# Get execution summary
summary = executor.get_execution_summary()
print(f"Success rate: {summary['execution_summary']['success_rate']:.1f}%")
