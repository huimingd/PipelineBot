# PipelineBot Framework

A Python framework for executing tasks with comprehensive resource monitoring and management. This framework provides inheritable classes that can be extended for any type of task execution while maintaining consistent resource allocation, monitoring, and reporting.

## Features

- 🚀 **Flexible Task Execution**: Support for both single and parallel task execution
- 📊 **Resource Monitoring**: Real-time CPU and memory usage tracking
- 🔧 **Configurable Resources**: CPU, memory, and process limits
- 📈 **Comprehensive Reporting**: Detailed execution metrics and summaries
- 🧬 **Inheritable Design**: Easy to extend for specialized use cases
- 🔄 **Multiple Execution Modes**: Thread-based and process-based execution
- 💾 **Export Capabilities**: JSON and Pickle format support

## Installation

### From PyPI (when published)
```bash
pip install PipelineBot

From Source

git clone https://github.com/huimingd/PipelineBot
cd PipelineBot
pip install -e .

Requirements
pip install -r requirements.txt
```

## Quick Start
### Basic Usage
```bash
from resource_executor import TaskExecutor, ResourceConfig, BaseTask

# Create a custom task
class MyTask(BaseTask):
    def execute(self):
        # Your task implementation
        return "Task completed successfully!"

# Configure resources
config = ResourceConfig(
    cpus=2,
    memory_gb=1.0,
    max_processes=4
)

# Execute task
executor = TaskExecutor(config)
result = executor.execute_task(MyTask("my_task"))

print(f"Success: {result.success}")
print(f"Result: {result.result}")

Parallel Execution

# Create multiple tasks
tasks = [MyTask(f"task_{i}") for i in range(5)]

# Execute in parallel
results = executor.execute_parallel_tasks(tasks)

# Get execution summary
summary = executor.get_execution_summary()
print(f"Success rate: {summary['execution_summary']['success_rate']:.1f}%")

Custom Executor

class BioinformaticsExecutor(TaskExecutor):
    def execute_alignment_pipeline(self, fastq_files):
        # Custom pipeline implementation
        tasks = [AlignmentTask(f) for f in fastq_files]
        return self.execute_parallel_tasks(tasks)

# Use specialized executor
bio_executor = BioinformaticsExecutor(config)
results = bio_executor.execute_alignment_pipeline(["sample1.fastq", "sample2.fastq"])
```

## Examples

See the examples/ directory for more detailed usage examples:

    basic_usage.py: Simple task execution
    parallel_execution.py: Parallel task processing
    custom_executor.py: Creating specialized executors

## Documentation

    Installation Guide
    Quick Start Guide
    API Reference

## Testing

Run the test suite:
```bash
python -m pytest tests/
```
## Contributing

    Fork the repository
    Create a feature branch (git checkout -b feature/amazing-feature)
    Commit your changes (git commit -m 'Add amazing feature')
    Push to the branch (git push origin feature/amazing-feature)
    Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Changelog
v0.1.0

    Initial release
    Basic task execution framework
    Resource monitoring capabilities
    Parallel execution support

