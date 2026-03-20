"""
Pytest configuration and shared fixtures for PipelineBot tests
"""

import pytest
import tempfile
import shutil
import time
import os
import sys
from pathlib import Path
from unittest.mock import Mock, patch

# Add src to path for imports - IMPORTANT FIX
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

# Now import after adding to path
try:
    from resource_executor.core.config import ResourceConfig
    from resource_executor.core.executor import TaskExecutor
    from resource_executor.core.tasks import BaseTask
    from resource_executor.core.monitor import ResourceMonitor
except ImportError as e:
    print(f"Import error in conftest.py: {e}")
    print(f"Python path: {sys.path}")
    print(f"Project root: {project_root}")
    print(f"Src path: {src_path}")
    raise

# Import psutil for mocking
try:
    import psutil
except ImportError:
    print("psutil not installed. Install with: pip install psutil")
    raise


@pytest.fixture
def basic_config():
    """Basic resource configuration for testing"""
    return ResourceConfig(
        cpus=2,
        memory_gb=1.0,
        max_processes=2,
        timeout_seconds=30,
        execution_mode="thread"
    )


@pytest.fixture
def process_config():
    """Process-based execution configuration"""
    return ResourceConfig(
        cpus=4,
        memory_gb=2.0,
        max_processes=3,
        timeout_seconds=60,
        execution_mode="process"
    )


@pytest.fixture
def temp_dir():
    """Temporary directory for test files"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def mock_psutil():
    """Mock psutil for testing without actual system monitoring"""
    with patch('resource_executor.core.monitor.psutil') as mock:
        # Mock process
        mock_process = Mock()
        mock_process.cpu_percent.return_value = 50.0
        mock_process.memory_info.return_value = Mock(rss=100 * 1024 * 1024)  # 100MB
        mock_process.num_threads.return_value = 4
        mock_process.num_fds.return_value = 10
        
        mock.Process.return_value = mock_process
        mock.NoSuchProcess = psutil.NoSuchProcess
        mock.AccessDenied = psutil.AccessDenied
        
        # Mock system info
        mock.virtual_memory.return_value = Mock(
            total=8 * 1024**3,  # 8GB
            available=4 * 1024**3,  # 4GB
            percent=50.0
        )
        mock.cpu_percent.return_value = 25.0
        mock.disk_usage.return_value = Mock(
            total=100 * 1024**3,  # 100GB
            free=50 * 1024**3,    # 50GB
            percent=50.0
        )
        
        yield mock


class SimpleTask(BaseTask):
    """Simple test task that just sleeps"""
    
    def __init__(self, task_id: str, duration: float = 0.1, should_fail: bool = False):
        super().__init__(task_id)
        self.duration = duration
        self.should_fail = should_fail
    
    def execute(self):
        time.sleep(self.duration)
        if self.should_fail:
            raise ValueError(f"Task {self.task_id} intentionally failed")
        return f"Task {self.task_id} completed"


class ResourceIntensiveTask(BaseTask):
    """Task that simulates resource usage"""
    
    def __init__(self, task_id: str, memory_mb: int = 10, cpu_duration: float = 0.1):
        super().__init__(task_id)
        self.memory_mb = memory_mb
        self.cpu_duration = cpu_duration
        self.allocated_data = []
    
    def execute(self):
        # Allocate memory
        for _ in range(self.memory_mb):
            self.allocated_data.append(bytearray(1024 * 1024))  # 1MB chunks
        
        # CPU intensive work
        end_time = time.time() + self.cpu_duration
        while time.time() < end_time:
            _ = sum(i * i for i in range(1000))
        
        return f"Resource task {self.task_id} completed"
    
    def cleanup(self):
        self.allocated_data.clear()


@pytest.fixture
def simple_task():
    """Simple task fixture"""
    return SimpleTask("test_task", duration=0.1)


@pytest.fixture
def failing_task():
    """Task that fails"""
    return SimpleTask("failing_task", duration=0.1, should_fail=True)


@pytest.fixture
def resource_task():
    """Resource intensive task"""
    return ResourceIntensiveTask("resource_task", memory_mb=5, cpu_duration=0.2)
