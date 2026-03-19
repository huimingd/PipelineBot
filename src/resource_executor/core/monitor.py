"""
Resource monitoring classes and utilities.
"""

import time
import psutil
import threading
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

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
    
    def add_custom_metric(self, key: str, value: Any):
        """Add a custom metric"""
        self.custom_metrics[key] = value
    
    def get_average_cpu(self) -> float:
        """Get average CPU usage"""
        return sum(self.cpu_percent) / len(self.cpu_percent) if self.cpu_percent else 0.0
    
    def get_peak_cpu(self) -> float:
        """Get peak CPU usage"""
        return max(self.cpu_percent) if self.cpu_percent else 0.0
    
    def get_average_memory(self) -> float:
        """Get average memory usage in MB"""
        return sum(self.memory_mb) / len(self.memory_mb) if self.memory_mb else 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary for serialization"""
        return {
            'task_id': self.task_id,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration_seconds': self.duration_seconds,
            'avg_cpu_percent': self.get_average_cpu(),
            'peak_cpu_percent': self.get_peak_cpu(),
            'avg_memory_mb': self.get_average_memory(),
            'peak_memory_mb': self.peak_memory_mb,
            'exit_code': self.exit_code,
            'custom_metrics': self.custom_metrics
        }

class ResourceMonitor:
    """Monitor system resources during task execution"""
    
    def __init__(self, task_id: str, interval: float = 1.0, detailed_monitoring: bool = True):
        """
        Initialize resource monitor
        
        Args:
            task_id: Unique identifier for the task
            interval: Monitoring interval in seconds
            detailed_monitoring: Whether to collect detailed metrics
        """
        self.task_id = task_id
        self.interval = interval
        self.detailed_monitoring = detailed_monitoring
        self.metrics = TaskMetrics(task_id=task_id, start_time=time.time())
        self.monitoring = False
        self.monitor_thread = None
        self._lock = threading.Lock()
        self._process = None
        
    def start_monitoring(self, process: Optional[psutil.Process] = None):
        """
        Start monitoring the given process or current process
        
        Args:
            process: Process to monitor, defaults to current process
        """
        if process is None:
            try:
                process = psutil.Process()
            except psutil.NoSuchProcess:
                logger.warning(f"Cannot monitor process for task {self.task_id}")
                return
        
        self._process = process
        
        with self._lock:
            if self.monitoring:
                logger.warning(f"Monitoring already started for task {self.task_id}")
                return
            self.monitoring = True
            
        self.monitor_thread = threading.Thread(
            target=self._monitor_process, 
            args=(process,),
            daemon=True,
            name=f"Monitor-{self.task_id}"
        )
        self.monitor_thread.start()
        logger.debug(f"Started monitoring for task {self.task_id}")
        
    def stop_monitoring(self):
        """Stop monitoring and finalize metrics"""
        with self._lock:
            if not self.monitoring:
                return
            self.monitoring = False
            
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5.0)
            if self.monitor_thread.is_alive():
                logger.warning(f"Monitor thread for task {self.task_id} did not stop gracefully")
        
        self.metrics.finalize()
        logger.debug(f"Stopped monitoring for task {self.task_id}")
            
    def add_custom_metric(self, key: str, value: Any):
        """Add custom metric to the current metrics"""
        with self._lock:
            self.metrics.add_custom_metric(key, value)
    
    def get_current_usage(self) -> Dict[str, float]:
        """Get current resource usage snapshot"""
        if not self._process:
            return {"cpu_percent": 0.0, "memory_mb": 0.0}
        
        try:
            cpu_percent = self._process.cpu_percent()
            memory_info = self._process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)
            
            return {
                "cpu_percent": cpu_percent,
                "memory_mb": memory_mb,
                "memory_vms_mb": memory_info.vms / (1024 * 1024),
                "num_threads": self._process.num_threads(),
                "num_fds": self._process.num_fds() if hasattr(self._process, 'num_fds') else 0
            }
        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            logger.debug(f"Cannot get current usage for task {self.task_id}: {e}")
            return {"cpu_percent": 0.0, "memory_mb": 0.0}
    
    def _monitor_process(self, process: psutil.Process):
        """Internal method to monitor process resources"""
        logger.debug(f"Starting resource monitoring loop for task {self.task_id}")
        
        # Initial CPU measurement (psutil needs a baseline)
        try:
            process.cpu_percent()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            logger.warning(f"Process not accessible for initial CPU measurement: {self.task_id}")
            return
        
        while True:
            with self._lock:
                if not self.monitoring:
                    break
            
            try:
                # Get basic metrics
                cpu_percent = process.cpu_percent()
                memory_info = process.memory_info()
                memory_mb = memory_info.rss / (1024 * 1024)
                
                with self._lock:
                    if self.monitoring:  # Double-check while holding lock
                        self.metrics.cpu_percent.append(cpu_percent)
                        self.metrics.memory_mb.append(memory_mb)
                
                # Collect detailed metrics if enabled
                if self.detailed_monitoring:
                    try:
                        # Additional process information
                        num_threads = process.num_threads()
                        
                        # I/O statistics (if available)
                        if hasattr(process, 'io_counters'):
                            io_counters = process.io_counters()
                            with self._lock:
                                if self.monitoring:
                                    self.metrics.add_custom_metric('io_read_bytes', io_counters.read_bytes)
                                    self.metrics.add_custom_metric('io_write_bytes', io_counters.write_bytes)
                        
                        # File descriptors (Unix-like systems)
                        if hasattr(process, 'num_fds'):
                            num_fds = process.num_fds()
                            with self._lock:
                                if self.monitoring:
                                    self.metrics.add_custom_metric('num_fds', num_fds)
                        
                        with self._lock:
                            if self.monitoring:
                                self.metrics.add_custom_metric('num_threads', num_threads)
                                
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        # Process might have restricted access for detailed metrics
                        pass
                
                logger.debug(f"Task {self.task_id}: CPU={cpu_percent:.1f}%, Memory={memory_mb:.1f}MB")
                
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                logger.debug(f"Process ended or access denied for task {self.task_id}")
                break
            except Exception as e:
                logger.warning(f"Error monitoring process for task {self.task_id}: {e}")
                
            time.sleep(self.interval)
        
        logger.debug(f"Monitoring loop ended for task {self.task_id}")

class SystemResourceMonitor:
    """Monitor overall system resources"""
    
    def __init__(self, interval: float = 5.0):
        """
        Initialize system resource monitor
        
        Args:
            interval: Monitoring interval in seconds
        """
        self.interval = interval
        self.monitoring = False
        self.monitor_thread = None
        self.system_metrics = {
            'timestamps': [],
            'cpu_percent': [],
            'memory_percent': [],
            'memory_available_gb': [],
            'disk_usage_percent': [],
            'load_average': []
        }
        self._lock = threading.Lock()
    
    def start_monitoring(self):
        """Start system monitoring"""
        with self._lock:
            if self.monitoring:
                return
            self.monitoring = True
        
        self.monitor_thread = threading.Thread(
            target=self._monitor_system,
            daemon=True,
            name="SystemMonitor"
        )
        self.monitor_thread.start()
        logger.info("Started system resource monitoring")
    
    def stop_monitoring(self):
        """Stop system monitoring"""
        with self._lock:
            self.monitoring = False
        
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=10.0)
        
        logger.info("Stopped system resource monitoring")
    
    def get_current_system_usage(self) -> Dict[str, Any]:
        """Get current system resource usage"""
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            result = {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_available_gb': memory.available / (1024**3),
                'memory_total_gb': memory.total / (1024**3),
                'disk_usage_percent': disk.percent,
                'disk_free_gb': disk.free / (1024**3),
                'timestamp': time.time()
            }
            
            # Add load average on Unix-like systems
            if hasattr(psutil, 'getloadavg'):
                result['load_average'] = psutil.getloadavg()[0]  # 1-minute load average
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting system usage: {e}")
            return {}
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of collected system metrics"""
        with self._lock:
            if not self.system_metrics['timestamps']:
                return {"message": "No system metrics collected"}
            
            return {
                'monitoring_duration': self.system_metrics['timestamps'][-1] - self.system_metrics['timestamps'][0],
                'avg_cpu_percent': sum(self.system_metrics['cpu_percent']) / len(self.system_metrics['cpu_percent']),
                'max_cpu_percent': max(self.system_metrics['cpu_percent']),
                'avg_memory_percent': sum(self.system_metrics['memory_percent']) / len(self.system_metrics['memory_percent']),
                'max_memory_percent': max(self.system_metrics['memory_percent']),
                'min_memory_available_gb': min(self.system_metrics['memory_available_gb']),
                'sample_count': len(self.system_metrics['timestamps'])
            }
    
    def _monitor_system(self):
        """Internal method to monitor system resources"""
        logger.debug("Starting system monitoring loop")
        
        while True:
            with self._lock:
                if not self.monitoring:
                    break
            
            try:
                current_usage = self.get_current_system_usage()
                
                with self._lock:
                    if self.monitoring and current_usage:
                        self.system_metrics['timestamps'].append(current_usage['timestamp'])
                        self.system_metrics['cpu_percent'].append(current_usage['cpu_percent'])
                        self.system_metrics['memory_percent'].append(current_usage['memory_percent'])
                        self.system_metrics['memory_available_gb'].append(current_usage['memory_available_gb'])
                        self.system_metrics['disk_usage_percent'].append(current_usage['disk_usage_percent'])
                        
                        if 'load_average' in current_usage:
                            self.system_metrics['load_average'].append(current_usage['load_average'])
                
            except Exception as e:
                logger.warning(f"Error in system monitoring loop: {e}")
            
            time.sleep(self.interval)
        
        logger.debug("System monitoring loop ended")

class ResourceThresholdMonitor:
    """Monitor resources and trigger alerts when thresholds are exceeded"""
    
    def __init__(self, cpu_threshold: float = 90.0, memory_threshold: float = 90.0):
        """
        Initialize threshold monitor
        
        Args:
            cpu_threshold: CPU usage threshold percentage
            memory_threshold: Memory usage threshold percentage
        """
        self.cpu_threshold = cpu_threshold
        self.memory_threshold = memory_threshold
        self.alerts = []
        self.callbacks = []
    
    def add_callback(self, callback: callable):
        """Add callback function to be called when threshold is exceeded"""
        self.callbacks.append(callback)
    
    def check_thresholds(self, metrics: TaskMetrics) -> List[Dict[str, Any]]:
        """
        Check if task metrics exceed thresholds
        
        Args:
            metrics: Task metrics to check
            
        Returns:
            List of threshold violations
        """
        violations = []
        
        if metrics.cpu_percent:
            max_cpu = max(metrics.cpu_percent)
            if max_cpu > self.cpu_threshold:
                violation = {
                    'type': 'cpu',
                    'task_id': metrics.task_id,
                    'threshold': self.cpu_threshold,
                    'actual': max_cpu,
                    'timestamp': time.time()
                }
                violations.append(violation)
                self.alerts.append(violation)
        
        if metrics.peak_memory_mb > 0:
            # Convert to percentage if we have system memory info
            try:
                system_memory_gb = psutil.virtual_memory().total / (1024**3)
                memory_percent = (metrics.peak_memory_mb / 1024) / system_memory_gb * 100
                
                if memory_percent > self.memory_threshold:
                    violation = {
                        'type': 'memory',
                        'task_id': metrics.task_id,
                        'threshold': self.memory_threshold,
                        'actual': memory_percent,
                        'peak_memory_mb': metrics.peak_memory_mb,
                        'timestamp': time.time()
                    }
                    violations.append(violation)
                    self.alerts.append(violation)
            except Exception as e:
                logger.warning(f"Could not check memory threshold: {e}")
        
        # Trigger callbacks for violations
        for violation in violations:
            for callback in self.callbacks:
                try:
                    callback(violation)
                except Exception as e:
                    logger.error(f"Error in threshold callback: {e}")
        
        return violations
    
    def get_alerts(self) -> List[Dict[str, Any]]:
        """Get all recorded alerts"""
        return self.alerts.copy()
    
    def clear_alerts(self):
        """Clear all recorded alerts"""
        self.alerts.clear()
		