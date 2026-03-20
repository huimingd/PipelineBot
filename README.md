# PipelineBot

A Python framework for executing tasks with comprehensive resource monitoring and management. PipelineBot provides an inheritable, extensible base that handles resource allocation, parallel execution, monitoring, and reporting — so you can focus on task logic.

## Features

- **Flexible Task Execution** — Single, parallel, and sequential task execution
- **Resource Monitoring** — Real-time CPU and memory tracking per task and system-wide
- **Configurable Resources** — CPU count, memory limits, process count, timeouts, and priority
- **Threshold Alerts** — Callbacks when CPU or memory usage exceeds defined limits
- **Inheritable Design** — Subclass `TaskExecutor` and `BaseTask` to build domain-specific pipelines
- **Multiple Execution Modes** — Thread-based or process-based parallelism
- **Export Results** — Save execution summaries as JSON or Pickle

## Installation

**From source:**

```bash
git clone https://github.com/huimingd/PipelineBot
cd PipelineBot
uv sync
```

**Install in editable mode (for development):**

```bash
uv pip install -e .[dev]
```

## Quick Start

### Define a task

```python
from resource_executor.core.tasks import BaseTask
from resource_executor.core.config import ResourceConfig
from resource_executor.core.executor import TaskExecutor

class MyTask(BaseTask):
    def execute(self):
        # Your task logic here
        return "Task completed successfully!"

# Configure resources
config = ResourceConfig(
    cpus=2,
    memory_gb=1.0,
    max_processes=4,
    timeout_seconds=30
)

# Execute
executor = TaskExecutor(config)
result = executor.execute_task(MyTask("my_task"))

print(f"Success: {result.success}")
print(f"Result:  {result.result}")
```

### Parallel execution

```python
tasks = [MyTask(f"task_{i}") for i in range(5)]
results = executor.execute_parallel_tasks(tasks)

summary = executor.get_execution_summary()
print(f"Success rate: {summary['execution_summary']['success_rate']:.1f}%")
```

### Sequential execution

```python
results = executor.execute_sequential_tasks(tasks, stop_on_failure=True)
```

### Mix tasks and callables

```python
def double(x):
    return x * 2

mixed = [
    MyTask("task_a"),
    (double, (21,), {}),   # (callable, args, kwargs)
    MyTask("task_b"),
]
results = executor.execute_parallel_tasks(mixed)
```

### Custom executor

Subclass `TaskExecutor` to add domain-specific pipeline methods:

```python
class BioinformaticsExecutor(TaskExecutor):
    def run_alignment_pipeline(self, fastq_files):
        tasks = [AlignmentTask(f) for f in fastq_files]
        return self.execute_parallel_tasks(tasks)

bio_executor = BioinformaticsExecutor(config)
results = bio_executor.run_alignment_pipeline(["sample1.fastq", "sample2.fastq"])
```

### Save results

```python
executor.save_results("results.json", format="json")    # JSON summary
executor.save_results("results.pkl",  format="pickle")  # Full TaskResult objects
```

## Architecture

```
src/resource_executor/
├── core/
│   ├── config.py     # ResourceConfig dataclass
│   ├── tasks.py      # BaseTask abstract class
│   ├── monitor.py    # ResourceMonitor, SystemResourceMonitor, ResourceThresholdMonitor
│   └── executor.py   # TaskExecutor — the main orchestrator
└── examples/
    ├── basic_tasks.py           # Ready-to-use task implementations
    └── specialized_executors.py # Domain-specific executor examples
```

| Component | Description |
|-----------|-------------|
| `ResourceConfig` | Defines CPU count, memory, `max_processes`, `timeout_seconds`, `priority`, `execution_mode` |
| `BaseTask` | Abstract base — implement `execute()`, optionally override `validate_inputs()`, `setup()`, `cleanup()` |
| `TaskExecutor` | Runs tasks, collects `TaskResult` objects, generates summaries |
| `ResourceMonitor` | Samples CPU/memory for a single task via a daemon thread |
| `SystemResourceMonitor` | Tracks system-wide metrics during parallel execution |
| `ResourceThresholdMonitor` | Fires user-defined callbacks when thresholds are exceeded |

## Task lifecycle

For every `BaseTask`, the executor runs:

```
validate_inputs() → setup() → execute() → cleanup()
```

## Testing

```bash
uv run pytest tests/ --cov=src/resource_executor --cov-report=xml
```

Run a single test:

```bash
uv run pytest tests/test_executor.py::TestTaskExecutor::test_parallel_task_execution
```

Lint:

```bash
uv run flake8 src tests --count --max-line-length=127 --statistics
```

## Examples

```bash
uv run src/resource_executor/examples/specialized_executors.py
```

Demonstrates four real-world pipelines: bioinformatics (RNA-seq), ETL data processing, machine learning training, and web scraping.

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Commit your changes: `git commit -m "Add my feature"`
4. Push: `git push origin feature/my-feature`
5. Open a Pull Request

## License

MIT License — see [LICENSE](LICENSE) for details.

## Changelog

### v0.1.0
- Initial release
- Core task execution framework with `BaseTask` and `TaskExecutor`
- Thread-based and process-based parallel execution
- Real-time resource monitoring (CPU, memory, I/O)
- Threshold alerts with user-defined callbacks
- JSON and Pickle result export
- Example pipelines: bioinformatics, ETL, ML, web scraping
