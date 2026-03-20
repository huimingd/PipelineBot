"""
Tests for ResourceMonitor and related monitoring classes
"""

import pytest
import time
import threading
import psutil
from unittest.mock import Mock, patch, MagicMock

from resource_executor.core.monitor import (
    TaskMetrics, ResourceMonitor, SystemResourceMonitor, ResourceThresholdMonitor
)


class TestTaskMetrics:
    """Test TaskMetrics functionality"""
    
    def test_task_metrics_creation(self):
        """Test basic TaskMetrics creation"""
        start_time = time.time()
        metrics = TaskMetrics("test_task", start_time)
        
        assert metrics.task_id == "test_task"
        assert metrics.start_time == start_time
        assert metrics.end_time is None
        assert metrics.duration_seconds == 0.0
        assert metrics.exit_code == 0
        assert len(metrics.cpu_percent) == 0
        assert len(metrics.memory_mb) == 0
    
    def test_metrics_finalize(self):
        """Test metrics finalization"""
        start_time = time.time()
        metrics = TaskMetrics("test_task", start_time)
        
        # Add some sample data
        metrics.cpu_percent = [10.0, 20.0, 30.0]
        metrics.memory_mb = [100.0, 150.0, 120.0]
        
        time.sleep(0.1)  # Small delay
        metrics.finalize()
        
        assert metrics.end_time is not None
        assert metrics.duration_seconds > 0
        assert metrics.peak_memory_mb == 150.0
    
    def test_custom_metrics(self):
        """Test custom metrics functionality"""
        metrics = TaskMetrics("test_task", time.time())
        
        metrics.add_custom_metric("custom_key", "custom_value")
        metrics.add_custom_metric("number_key", 42)
        
        assert metrics.custom_metrics["custom_key"] == "custom_value"
        assert metrics.custom_metrics["number_key"] == 42
    
    def test_average_calculations(self):
        """Test average calculation methods"""
        metrics = TaskMetrics("test_task", time.time())
        
        metrics.cpu_percent = [10.0, 20.0, 30.0]
        metrics.memory_mb = [100.0, 200.0, 300.0]
        
        assert metrics.get_average_cpu() == 20.0
        assert metrics.get_peak_cpu() == 30.0
        assert metrics.get_average_memory() == 200.0
    
    def test_empty_metrics_averages(self):
        """Test average calculations with empty data"""
        metrics = TaskMetrics("test_task", time.time())
        
        assert metrics.get_average_cpu() == 0.0
        assert metrics.get_peak_cpu() == 0.0
        assert metrics.get_average_memory() == 0.0
    
    def test_to_dict(self):
        """Test metrics serialization to dictionary"""
        start_time = time.time()
        metrics = TaskMetrics("test_task", start_time)
        metrics.cpu_percent = [10.0, 20.0]
        metrics.memory_mb = [100.0, 200.0]
        metrics.custom_metrics = {"test": "value"}
        metrics.finalize()
        
        result = metrics.to_dict()
        
        assert result["task_id"] == "test_task"
        assert result["start_time"] == start_time
        assert result["avg_cpu_percent"] == 15.0
        assert result["peak_cpu_percent"] == 20.0
        assert result["avg_memory_mb"] == 150.0
        assert result["peak_memory_mb"] == 200.0
        assert result["custom_metrics"] == {"test": "value"}


class TestResourceMonitor:
    """Test ResourceMonitor functionality"""
    
    def test_monitor_creation(self):
        """Test ResourceMonitor creation"""
        monitor = ResourceMonitor("test_task", interval=0.5)
        
        assert monitor.task_id == "test_task"
        assert monitor.interval == 0.5
        assert monitor.monitoring is False
        assert monitor.monitor_thread is None
    
    @patch('resource_executor.core.monitor.psutil.Process')
    def test_start_stop_monitoring(self, mock_process_class):
        """Test starting and stopping monitoring"""
        mock_process = Mock()
        mock_process.cpu_percent.return_value = 25.0
        mock_process.memory_info.return_value = Mock(rss=50 * 1024 * 1024)
        mock_process_class.return_value = mock_process
        
        monitor = ResourceMonitor("test_task", interval=0.1)
        
        # Start monitoring
        monitor.start_monitoring()
        assert monitor.monitoring is True
        assert monitor.monitor_thread is not None
        assert monitor.monitor_thread.is_alive()
        
        # Let it collect some data
        time.sleep(0.3)
        
        # Stop monitoring
        monitor.stop_monitoring()
        assert monitor.monitoring is False
        assert len(monitor.metrics.cpu_percent) > 0
        assert len(monitor.metrics.memory_mb) > 0
    
    @patch('resource_executor.core.monitor.psutil.Process')
    def test_monitoring_with_custom_process(self, mock_process_class):
        """Test monitoring with custom process"""
        mock_process = Mock()
        mock_process.cpu_percent.return_value = 75.0
        mock_process.memory_info.return_value = Mock(rss=200 * 1024 * 1024)
        
        monitor = ResourceMonitor("test_task", interval=0.1)
        monitor.start_monitoring(mock_process)
        
        time.sleep(0.2)
        monitor.stop_monitoring()
        
        assert len(monitor.metrics.cpu_percent) > 0
        assert all(cpu >= 0 for cpu in monitor.metrics.cpu_percent)
    
    def test_add_custom_metric(self):
        """Test adding custom metrics during monitoring"""
        monitor = ResourceMonitor("test_task")
        
        monitor.add_custom_metric("test_metric", 123)
        monitor.add_custom_metric("another_metric", "test_value")
        
        assert monitor.metrics.custom_metrics["test_metric"] == 123
        assert monitor.metrics.custom_metrics["another_metric"] == "test_value"
    
    @patch('resource_executor.core.monitor.psutil.Process')
    def test_monitoring_process_not_found(self, mock_process_class):
        """Test monitoring when process is not found"""
        mock_process = Mock()
        mock_process.cpu_percent.side_effect = psutil.NoSuchProcess(123)
        mock_process_class.return_value = mock_process
        
        monitor = ResourceMonitor("test_task", interval=0.1)
        monitor.start_monitoring()
        
        time.sleep(0.2)
        monitor.stop_monitoring()
        
        # Should handle the exception gracefully
        assert monitor.monitoring is False
    
    @patch('resource_executor.core.monitor.psutil.Process')
    def test_get_current_usage(self, mock_process_class):
        """Test getting current usage snapshot"""
        mock_process = Mock()
        mock_process.cpu_percent.return_value = 45.0
        mock_process.memory_info.return_value = Mock(
            rss=100 * 1024 * 1024,
            vms=200 * 1024 * 1024
        )
        mock_process.num_threads.return_value = 8
        mock_process.num_fds.return_value = 15
        mock_process_class.return_value = mock_process
        
        monitor = ResourceMonitor("test_task")
        monitor._process = mock_process
        
        usage = monitor.get_current_usage()
        
        assert usage["cpu_percent"] == 45.0
        assert usage["memory_mb"] == 100.0
        assert usage["memory_vms_mb"] == 200.0
        assert usage["num_threads"] == 8
        assert usage["num_fds"] == 15


class TestSystemResourceMonitor:
    """Test SystemResourceMonitor functionality"""
    
    @patch('resource_executor.core.monitor.psutil')
    def test_system_monitor_creation(self, mock_psutil):
        """Test SystemResourceMonitor creation"""
        monitor = SystemResourceMonitor(interval=1.0)
        
        assert monitor.interval == 1.0
        assert monitor.monitoring is False
        assert len(monitor.system_metrics['timestamps']) == 0
    
    @patch('resource_executor.core.monitor.psutil')
    def test_get_current_system_usage(self, mock_psutil):
        """Test getting current system usage"""
        # Mock psutil functions
        mock_psutil.cpu_percent.return_value = 35.0
        mock_psutil.virtual_memory.return_value = Mock(
            percent=60.0,
            available=4 * 1024**3,
            total=8 * 1024**3
        )
        mock_psutil.disk_usage.return_value = Mock(
            percent=45.0,
            free=50 * 1024**3
        )
        mock_psutil.getloadavg.return_value = [1.5, 1.2, 1.0]
        
        monitor = SystemResourceMonitor()
        usage = monitor.get_current_system_usage()
        
        assert usage["cpu_percent"] == 35.0
        assert usage["memory_percent"] == 60.0
        assert usage["memory_available_gb"] == 4.0
        assert usage["disk_usage_percent"] == 45.0
        assert usage["load_average"] == 1.5
        assert "timestamp" in usage
    
    @patch('resource_executor.core.monitor.psutil')
    def test_start_stop_system_monitoring(self, mock_psutil):
        """Test starting and stopping system monitoring"""
        mock_psutil.cpu_percent.return_value = 25.0
        mock_psutil.virtual_memory.return_value = Mock(
            percent=50.0,
            available=4 * 1024**3,
            total=8 * 1024**3
        )
        mock_psutil.disk_usage.return_value = Mock(
            percent=30.0,
            free=70 * 1024**3
        )
        
        monitor = SystemResourceMonitor(interval=0.1)
        
        monitor.start_monitoring()
        assert monitor.monitoring is True
        
        time.sleep(0.3)
        
        monitor.stop_monitoring()
        assert monitor.monitoring is False
        assert len(monitor.system_metrics['timestamps']) > 0
    
    @patch('resource_executor.core.monitor.psutil')
    def test_get_metrics_summary(self, mock_psutil):
        """Test getting system metrics summary"""
        monitor = SystemResourceMonitor()
        
        # Simulate some collected data
        current_time = time.time()
        monitor.system_metrics = {
            'timestamps': [current_time, current_time + 1, current_time + 2],
            'cpu_percent': [20.0, 30.0, 40.0],
            'memory_percent': [50.0, 55.0, 60.0],
            'memory_available_gb': [4.0, 3.8, 3.5],
            'disk_usage_percent': [30.0, 30.0, 30.0],
            'load_average': [1.0, 1.2, 1.1]
        }
        
        summary = monitor.get_metrics_summary()
        
        assert summary['monitoring_duration'] == 2.0
        assert summary['avg_cpu_percent'] == 30.0
        assert summary['max_cpu_percent'] == 40.0
        assert summary['avg_memory_percent'] == 55.0
        assert summary['max_memory_percent'] == 60.0
        assert summary['min_memory_available_gb'] == 3.5
        assert summary['sample_count'] == 3


class TestResourceThresholdMonitor:
    """Test ResourceThresholdMonitor functionality"""
    
    def test_threshold_monitor_creation(self):
        """Test ResourceThresholdMonitor creation"""
        monitor = ResourceThresholdMonitor(cpu_threshold=80.0, memory_threshold=85.0)
        
        assert monitor.cpu_threshold == 80.0
        assert monitor.memory_threshold == 85.0
        assert len(monitor.alerts) == 0
        assert len(monitor.callbacks) == 0
    
    def test_add_callback(self):
        """Test adding threshold callbacks"""
        monitor = ResourceThresholdMonitor()
        
        def test_callback(violation):
            pass
        
        monitor.add_callback(test_callback)
        assert len(monitor.callbacks) == 1
        assert monitor.callbacks[0] == test_callback
    
    @patch('resource_executor.core.monitor.psutil.virtual_memory')
    def test_check_cpu_threshold(self, mock_memory):
        """Test CPU threshold checking"""
        mock_memory.return_value = Mock(total=8 * 1024**3)
        
        monitor = ResourceThresholdMonitor(cpu_threshold=50.0)
        
        # Create metrics that exceed CPU threshold
        metrics = TaskMetrics("test_task", time.time())
        metrics.cpu_percent = [30.0, 60.0, 40.0]  # Peak of 60% > 50%
        
        violations = monitor.check_thresholds(metrics)
        
        assert len(violations) == 1
        assert violations[0]['type'] == 'cpu'
        assert violations[0]['actual'] == 60.0
        assert violations[0]['threshold'] == 50.0
        assert len(monitor.alerts) == 1
    
    @patch('resource_executor.core.monitor.psutil.virtual_memory')
    def test_check_memory_threshold(self, mock_memory):
        """Test memory threshold checking"""
        mock_memory.return_value = Mock(total=8 * 1024**3)  # 8GB system
        
        monitor = ResourceThresholdMonitor(memory_threshold=10.0)  # 10% threshold
        
        # Create metrics that exceed memory threshold
        metrics = TaskMetrics("test_task", time.time())
        metrics.peak_memory_mb = 1024  # 1GB = 12.5% of 8GB system
        
        violations = monitor.check_thresholds(metrics)
        
        assert len(violations) == 1
        assert violations[0]['type'] == 'memory'
        assert violations[0]['actual'] > 10.0
        assert violations[0]['threshold'] == 10.0
    
    def test_callback_execution(self):
        """Test that callbacks are executed on threshold violations"""
        monitor = ResourceThresholdMonitor(cpu_threshold=30.0)
        
        callback_called = []
        
        def test_callback(violation):
            callback_called.append(violation)
        
        monitor.add_callback(test_callback)
        
        # Create metrics that exceed threshold
        metrics = TaskMetrics("test_task", time.time())
        metrics.cpu_percent = [50.0]  # Exceeds 30% threshold
        
        with patch('resource_executor.core.monitor.psutil.virtual_memory'):
            monitor.check_thresholds(metrics)
        
        assert len(callback_called) == 1
        assert callback_called[0]['type'] == 'cpu'
    
    def test_no_violations(self):
        """Test when no thresholds are violated"""
        monitor = ResourceThresholdMonitor(cpu_threshold=80.0, memory_threshold=80.0)
        
        metrics = TaskMetrics("test_task", time.time())
        metrics.cpu_percent = [30.0, 40.0, 35.0]  # All below 80%
        metrics.peak_memory_mb = 100  # Low memory usage
        
        with patch('resource_executor.core.monitor.psutil.virtual_memory') as mock_memory:
            mock_memory.return_value = Mock(total=8 * 1024**3)
            violations = monitor.check_thresholds(metrics)
        
        assert len(violations) == 0
        assert len(monitor.alerts) == 0
    
    def test_get_and_clear_alerts(self):
        """Test getting and clearing alerts"""
        monitor = ResourceThresholdMonitor(cpu_threshold=20.0)
        
        # Generate some alerts
        metrics = TaskMetrics("test_task", time.time())
        metrics.cpu_percent = [50.0]
        
        with patch('resource_executor.core.monitor.psutil.virtual_memory'):
            monitor.check_thresholds(metrics)
        
        alerts = monitor.get_alerts()
        assert len(alerts) == 1
        
        monitor.clear_alerts()
        assert len(monitor.get_alerts()) == 0
		