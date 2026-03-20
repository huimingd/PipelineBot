"""
Specialized executor implementations for domain-specific use cases

This module provides specialized executors that extend the base TaskExecutor
for specific domains like bioinformatics, data processing, machine learning, etc.
"""

import time
import logging
from typing import List, Dict, Any, Optional, Callable, Union
from pathlib import Path
import json
import tempfile
import concurrent.futures

#from ..core.executor import TaskExecutor, TaskResult
#from ..core.config import ResourceConfig
#from ..core.tasks import BaseTask
#from .basic_tasks import CPUIntensiveTask, MemoryIntensiveTask, IOIntensiveTask, CompositeTask

# With these absolute imports:
import sys
import os
from pathlib import Path

# Add the src directory to Python path
current_dir = Path(__file__).parent
src_dir = current_dir.parent.parent
sys.path.insert(0, str(src_dir))

from resource_executor.core.executor import TaskExecutor, TaskResult
from resource_executor.core.config import ResourceConfig
from resource_executor.core.tasks import BaseTask
from resource_executor.examples.basic_tasks import CPUIntensiveTask, MemoryIntensiveTask, IOIntensiveTask, CompositeTask, NetworkTask


logger = logging.getLogger(__name__)


# Specialized Task Classes for Bioinformatics

class AlignmentTask(BaseTask):
    """Task for sequence alignment simulation"""
    
    def __init__(self, task_id: str, fastq_file: str, reference_genome: str,
        aligner: str = "bwa", threads: int = 4, output_dir: str = "."):
        super().__init__(task_id, fastq_file=fastq_file, reference_genome=reference_genome,
                        aligner=aligner, threads=threads, output_dir=output_dir)
        self.fastq_file = fastq_file
        self.reference_genome = reference_genome
        self.aligner = aligner
        self.threads = threads
        self.output_dir = Path(output_dir)
        self.output_file = None
        
    def validate_inputs(self) -> bool:
        """Validate alignment inputs"""
        if not self.fastq_file:
            self.logger.error("FASTQ file path is required")
            return False
        if not self.reference_genome:
            self.logger.error("Reference genome path is required")
            return False
        if self.aligner not in ['bwa', 'bowtie2', 'star', 'minimap2']:
            self.logger.error(f"Unsupported aligner: {self.aligner}")
            return False
        return True
    
    def execute(self) -> Dict[str, Any]:
        """Execute alignment simulation"""
        self.logger.info(f"Starting alignment: {self.fastq_file} using {self.aligner}")
        
        start_time = time.time()
        
        # Simulate alignment process with CPU-intensive work
        # Duration varies by aligner
        aligner_durations = {
            'bwa': 10,
            'bowtie2': 8,
            'star': 15,
            'minimap2': 5
        }
        
        duration = aligner_durations.get(self.aligner, 10)
        
        # Simulate CPU-intensive alignment
        cpu_task = CPUIntensiveTask(f"{self.task_id}_cpu", duration=duration, intensity=0.8)
        cpu_result = cpu_task.execute()
        
        # Create output file
        self.output_file = self.output_dir / f"{self.task_id}_aligned.bam"
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Simulate creating BAM file
        with open(self.output_file, 'w') as f:
            f.write(f"# Simulated BAM file from {self.aligner} alignment\n")
            f.write(f"# Input: {self.fastq_file}\n")
            f.write(f"# Reference: {self.reference_genome}\n")
            f.write(f"# Threads: {self.threads}\n")
        
        actual_duration = time.time() - start_time
        
        result = {
            "task_id": self.task_id,
            "aligner": self.aligner,
            "input_file": self.fastq_file,
            "output_file": str(self.output_file),
            "threads_used": self.threads,
            "duration": actual_duration,
            "alignment_rate": 0.95,  # Simulated alignment rate
            "reads_processed": 1000000,  # Simulated read count
            "cpu_metrics": cpu_result
        }
        
        self.logger.info(f"Alignment completed: {result['reads_processed']} reads in {actual_duration:.2f}s")
        return result
    
    def cleanup(self):
        """Clean up output files"""
        if self.output_file and self.output_file.exists():
            self.output_file.unlink()
    
    def get_estimated_resources(self) -> ResourceConfig:
        return ResourceConfig(
            cpus=self.threads,
            memory_gb=4.0,
            timeout_seconds=1800,  # 30 minutes
            priority="high"
        )


class VariantCallingTask(BaseTask):
    """Task for variant calling simulation"""
    
    def __init__(self, task_id: str, bam_file: str, reference_genome: str,
        caller: str = "gatk", output_dir: str = "."):
        super().__init__(task_id, bam_file=bam_file, reference_genome=reference_genome,
                        caller=caller, output_dir=output_dir)
        self.bam_file = bam_file
        self.reference_genome = reference_genome
        self.caller = caller
        self.output_dir = Path(output_dir)
        self.output_file = None
        
    def execute(self) -> Dict[str, Any]:
        """Execute variant calling simulation"""
        self.logger.info(f"Starting variant calling: {self.bam_file} using {self.caller}")
        
        start_time = time.time()
        
        # Simulate variant calling with mixed workload
        composite_task = CompositeTask(
            f"{self.task_id}_composite",
            cpu_duration=8,
            memory_mb=200,
            io_file_size_mb=10,
            execution_order="sequential"
        )
        composite_result = composite_task.execute()
        
        # Create VCF output
        self.output_file = self.output_dir / f"{self.task_id}_variants.vcf"
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(self.output_file, 'w') as f:
            f.write("##fileformat=VCFv4.2\n")
            f.write(f"##source={self.caller}\n")
            f.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")
            # Simulate some variants
            for i in range(100):
                f.write(f"chr1\t{1000+i*100}\t.\tA\tT\t60\tPASS\tDP=30\n")
        
        actual_duration = time.time() - start_time
        
        result = {
            "task_id": self.task_id,
            "caller": self.caller,
            "input_file": self.bam_file,
            "output_file": str(self.output_file),
            "duration": actual_duration,
            "variants_called": 100,
            "composite_metrics": composite_result
        }
        
        self.logger.info(f"Variant calling completed: {result['variants_called']} variants in {actual_duration:.2f}s")
        return result
    
    def cleanup(self):
        if self.output_file and self.output_file.exists():
            self.output_file.unlink()


class QualityControlTask(BaseTask):
    """Task for quality control simulation"""
    
    def __init__(self, task_id: str, fastq_file: str):
        super().__init__(task_id, fastq_file=fastq_file)
        self.fastq_file = fastq_file
        
    def execute(self) -> Dict[str, Any]:
        """Execute QC simulation"""
        self.logger.info(f"Running QC on {self.fastq_file}")
        
        # Simulate QC with I/O intensive task
        io_task = IOIntensiveTask(f"{self.task_id}_io", file_size_mb=5, duration=3)
        io_task.setup()
        io_result = io_task.execute()
        
        result = {
            "task_id": self.task_id,
            "input_file": self.fastq_file,
            "total_reads": 500000,
            "quality_score": 35.2,
            "gc_content": 42.1,
            "io_metrics": io_result
        }
        
        return result


class QuantificationTask(BaseTask):
    """Task for RNA-seq quantification simulation"""
    
    def __init__(self, task_id: str, fastq_file: str, reference: str, method: str = "salmon"):
        super().__init__(task_id, fastq_file=fastq_file, reference=reference, method=method)
        self.fastq_file = fastq_file
        self.reference = reference
        self.method = method
        
    def execute(self) -> Dict[str, Any]:
        """Execute quantification simulation"""
        self.logger.info(f"Quantifying {self.fastq_file} using {self.method}")
        
        # Simulate quantification
        cpu_task = CPUIntensiveTask(f"{self.task_id}_quant", duration=6, intensity=0.7)
        cpu_result = cpu_task.execute()
        
        result = {
            "task_id": self.task_id,
            "method": self.method,
            "input_file": self.fastq_file,
            "genes_quantified": 20000,
            "mapping_rate": 0.87,
            "cpu_metrics": cpu_result
        }
        
        return result


class DifferentialExpressionTask(BaseTask):
    """Task for differential expression analysis simulation"""
    
    def __init__(self, task_id: str, count_files: List[str], conditions: List[str]):
        super().__init__(task_id, count_files=count_files, conditions=conditions)
        self.count_files = count_files
        self.conditions = conditions
        
    def execute(self) -> Dict[str, Any]:
        """Execute differential expression analysis"""
        self.logger.info(f"Running DE analysis on {len(self.count_files)} samples")
        
        # Simulate statistical analysis
        memory_task = MemoryIntensiveTask(f"{self.task_id}_stats", memory_mb=100, duration=8)
        memory_result = memory_task.execute()
        
        result = {
            "task_id": self.task_id,
            "samples_analyzed": len(self.count_files),
            "differentially_expressed_genes": 1250,
            "significant_genes": 890,
            "memory_metrics": memory_result
        }
        
        return result


# Specialized Executors

class BioinformaticsExecutor(TaskExecutor):
    """
    Specialized executor for bioinformatics workflows
    
    This executor provides methods for common bioinformatics operations
    like sequence alignment, variant calling, and genomic analysis.
    """
    
    def __init__(self, config: ResourceConfig, reference_genome: Optional[str] = None,
        annotation_file: Optional[str] = None, output_dir: Optional[str] = None):
        """
        Initialize bioinformatics executor
        
        Args:
            config: Resource configuration
            reference_genome: Path to reference genome file
            annotation_file: Path to annotation file (GTF/GFF)
            output_dir: Output directory for results
        """
        super().__init__(config)
        self.reference_genome = reference_genome
        self.annotation_file = annotation_file
        self.output_dir = Path(output_dir) if output_dir else Path("bioinformatics_results")
        self.logger = logging.getLogger("BioinformaticsExecutor")
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def execute_alignment_pipeline(self, fastq_files: List[str], 
        aligner: str = "bwa", threads_per_sample: int = 4) -> List[TaskResult]:
        """
        Execute sequence alignment pipeline
        
        Args:
            fastq_files: List of FASTQ file paths
            aligner: Alignment algorithm ('bwa', 'bowtie2', 'star')
            threads_per_sample: Number of threads per alignment
            
        Returns:
            List of alignment results
        """
        self.logger.info(f"Starting alignment pipeline for {len(fastq_files)} samples using {aligner}")
        
        alignment_tasks = []
        for i, fastq_file in enumerate(fastq_files):
            task = AlignmentTask(
                task_id=f"alignment_{i}_{Path(fastq_file).stem}",
                fastq_file=fastq_file,
                reference_genome=self.reference_genome or "reference.fa",
                aligner=aligner,
                threads=threads_per_sample,
                output_dir=str(self.output_dir)
            )
            alignment_tasks.append(task)
        
        # Execute alignments in parallel
        results = self.execute_parallel_tasks(alignment_tasks)
        
        successful_alignments = sum(1 for r in results if r.success)
        self.logger.info(f"Alignment pipeline completed: {successful_alignments}/{len(results)} successful")
        
        return results
    
    def execute_variant_calling_pipeline(self, bam_files: List[str], 
        caller: str = "gatk") -> List[TaskResult]:
        """
        Execute variant calling pipeline
        
        Args:
            bam_files: List of BAM file paths
            caller: Variant caller ('gatk', 'freebayes', 'samtools')
            
        Returns:
            List of variant calling results
        """
        self.logger.info(f"Starting variant calling pipeline for {len(bam_files)} samples using {caller}")
        
        variant_tasks = []
        for i, bam_file in enumerate(bam_files):
            task = VariantCallingTask(
                task_id=f"variant_calling_{i}_{Path(bam_file).stem}",
                bam_file=bam_file,
                reference_genome=self.reference_genome or "reference.fa",
                caller=caller,
                output_dir=str(self.output_dir)
            )
            variant_tasks.append(task)
        
        results = self.execute_parallel_tasks(variant_tasks)
        
        successful_calls = sum(1 for r in results if r.success)
        self.logger.info(f"Variant calling pipeline completed: {successful_calls}/{len(results)} successful")
        
        return results
    
    def execute_rna_seq_pipeline(self, fastq_files: List[str], 
        quantification_method: str = "salmon") -> Dict[str, Any]:
        """
        Execute RNA-seq analysis pipeline
        
        Args:
            fastq_files: List of FASTQ file paths
            quantification_method: Quantification method ('salmon', 'kallisto', 'featurecounts')
            
        Returns:
            Pipeline results summary
        """
        self.logger.info(f"Starting RNA-seq pipeline for {len(fastq_files)} samples")
        
        pipeline_start = time.time()
        
        # Step 1: Quality control
        qc_tasks = [
            QualityControlTask(f"qc_{i}", fastq_file) 
            for i, fastq_file in enumerate(fastq_files)
        ]
        qc_results = self.execute_parallel_tasks(qc_tasks)
        
        # Step 2: Quantification
        quant_tasks = [
            QuantificationTask(f"quant_{i}", fastq_file, 
                self.reference_genome or "transcriptome.fa", quantification_method)
            for i, fastq_file in enumerate(fastq_files)
        ]
        quant_results = self.execute_parallel_tasks(quant_tasks)
        
        # Step 3: Differential expression analysis
        count_files = [f"counts_{i}.txt" for i in range(len(fastq_files))]
        conditions = ["control", "treatment"] * (len(fastq_files) // 2 + 1)
        conditions = conditions[:len(fastq_files)]
        
        de_task = DifferentialExpressionTask("differential_expression", count_files, conditions)
        de_result = self.execute_task(de_task)
        
        pipeline_duration = time.time() - pipeline_start
        
        # Compile results
        successful_qc = sum(1 for r in qc_results if r.success)
        successful_quant = sum(1 for r in quant_results if r.success)
        
        pipeline_summary = {
            "pipeline_type": "RNA-seq",
            "samples_processed": len(fastq_files),
            "quantification_method": quantification_method,
            "pipeline_duration": pipeline_duration,
            "qc_success_rate": successful_qc / len(qc_results) * 100,
            "quantification_success_rate": successful_quant / len(quant_results) * 100,
            "differential_expression_success": de_result.success,
            "total_tasks": len(qc_results) + len(quant_results) + 1,
            "output_directory": str(self.output_dir)
        }
        
        self.logger.info(f"RNA-seq pipeline completed in {pipeline_duration:.2f}s")
        return pipeline_summary
    
    def get_pipeline_summary(self) -> Dict[str, Any]:
        """Get specialized summary for bioinformatics pipeline"""
        summary = self.get_execution_summary()
        summary["pipeline_type"] = "bioinformatics"
        summary["reference_genome"] = self.reference_genome
        summary["annotation_file"] = self.annotation_file
        summary["output_directory"] = str(self.output_dir)
        return summary


class DataProcessingExecutor(TaskExecutor):
    """
    Specialized executor for data processing workflows
    
    This executor provides methods for common data processing operations
    like ETL, data transformation, and batch processing.
    """
    
    def __init__(self, config: ResourceConfig, input_dir: Optional[str] = None,
        output_dir: Optional[str] = None, chunk_size: int = 1000):
        super().__init__(config)
        self.input_dir = Path(input_dir) if input_dir else Path("input_data")
        self.output_dir = Path(output_dir) if output_dir else Path("processed_data")
        self.chunk_size = chunk_size
        self.logger = logging.getLogger("DataProcessingExecutor")
        
        # Create directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def execute_etl_pipeline(self, data_sources: List[str], 
        transformations: List[str] = None) -> Dict[str, Any]:
        """
        Execute ETL (Extract, Transform, Load) pipeline
        
        Args:
            data_sources: List of data source identifiers
            transformations: List of transformation operations
            
        Returns:
            ETL pipeline results
        """
        self.logger.info(f"Starting ETL pipeline for {len(data_sources)} data sources")
        
        transformations = transformations or ["clean", "normalize", "aggregate"]
        pipeline_start = time.time()
        
        # Extract phase - simulate data extraction
        extract_tasks = [
            IOIntensiveTask(f"extract_{i}", file_size_mb=20, duration=5)
            for i in range(len(data_sources))
        ]
        extract_results = self.execute_parallel_tasks(extract_tasks)
        
        # Transform phase - simulate data transformation
        transform_tasks = [
            CPUIntensiveTask(f"transform_{i}", duration=8, intensity=0.6)
            for i in range(len(data_sources))
        ]
        transform_results = self.execute_parallel_tasks(transform_tasks)
        
        # Load phase - simulate data loading
        load_tasks = [
            IOIntensiveTask(f"load_{i}", file_size_mb=15, duration=3)
            for i in range(len(data_sources))
        ]
        load_results = self.execute_parallel_tasks(load_tasks)
        
        pipeline_duration = time.time() - pipeline_start
        
        etl_summary = {
            "pipeline_type": "ETL",
            "data_sources": len(data_sources),
            "transformations": transformations,
            "pipeline_duration": pipeline_duration,
            "extract_success_rate": sum(1 for r in extract_results if r.success) / len(extract_results) * 100,
            "transform_success_rate": sum(1 for r in transform_results if r.success) / len(transform_results) * 100,
            "load_success_rate": sum(1 for r in load_results if r.success) / len(load_results) * 100,
            "total_records_processed": len(data_sources) * self.chunk_size,
            "output_directory": str(self.output_dir)
        }
        
        self.logger.info(f"ETL pipeline completed in {pipeline_duration:.2f}s")
        return etl_summary
    
    def execute_batch_processing(self, batch_files: List[str], 
        processing_function: str = "default") -> List[TaskResult]:
        """
        Execute batch processing on multiple files
        
        Args:
            batch_files: List of files to process
            processing_function: Type of processing to perform
            
        Returns:
            List of batch processing results
        """
        self.logger.info(f"Starting batch processing for {len(batch_files)} files")
        
        batch_tasks = []
        for i, batch_file in enumerate(batch_files):
            # Create composite task for each file
            task = CompositeTask(
                f"batch_process_{i}",
                cpu_duration=5,
                memory_mb=50,
                io_file_size_mb=10,
                execution_order="sequential"
            )
            batch_tasks.append(task)
        
        results = self.execute_parallel_tasks(batch_tasks)
        
        successful_batches = sum(1 for r in results if r.success)
        self.logger.info(f"Batch processing completed: {successful_batches}/{len(results)} successful")
        
        return results


class MachineLearningExecutor(TaskExecutor):
    """
    Specialized executor for machine learning workflows
    
    This executor provides methods for ML training, validation, and inference.
    """
    
    def __init__(self, config: ResourceConfig, model_dir: Optional[str] = None,
        data_dir: Optional[str] = None):
        super().__init__(config)
        self.model_dir = Path(model_dir) if model_dir else Path("models")
        self.data_dir = Path(data_dir) if data_dir else Path("ml_data")
        self.logger = logging.getLogger("MachineLearningExecutor")
        
        # Create directories
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def execute_training_pipeline(self, datasets: List[str], 
                                model_type: str = "neural_network",
                                epochs: int = 10) -> Dict[str, Any]:
        """
        Execute ML training pipeline
        
        Args:
            datasets: List of training datasets
            model_type: Type of model to train
            epochs: Number of training epochs
            
        Returns:
            Training pipeline results
        """
        self.logger.info(f"Starting ML training pipeline: {model_type} for {epochs} epochs")
        
        pipeline_start = time.time()
        
        # Data preprocessing
        preprocess_tasks = [
            CompositeTask(f"preprocess_{i}", cpu_duration=3, memory_mb=100, 
                io_file_size_mb=50, execution_order="sequential")
            for i in range(len(datasets))
        ]
        preprocess_results = self.execute_parallel_tasks(preprocess_tasks)
        
        # Model training (CPU and memory intensive)
        training_duration = epochs * 2  # 2 seconds per epoch simulation
        training_task = CompositeTask(
            "model_training",
            cpu_duration=training_duration,
            memory_mb=500,
            execution_order="parallel"
        )
        training_result = self.execute_task(training_task)
        
        # Model validation
        validation_task = CPUIntensiveTask("model_validation", duration=5, intensity=0.7)
        validation_result = self.execute_task(validation_task)
        
        pipeline_duration = time.time() - pipeline_start
        
        training_summary = {
            "pipeline_type": "ML_training",
            "model_type": model_type,
            "datasets": len(datasets),
            "epochs": epochs,
            "pipeline_duration": pipeline_duration,
            "preprocessing_success": all(r.success for r in preprocess_results),
            "training_success": training_result.success,
            "validation_success": validation_result.success,
            "model_accuracy": 0.92,  # Simulated accuracy
            "model_directory": str(self.model_dir)
        }
        
        self.logger.info(f"ML training pipeline completed in {pipeline_duration:.2f}s")
        return training_summary


class WebScrapingExecutor(TaskExecutor):
    """
    Specialized executor for web scraping workflows
    
    This executor provides methods for web scraping, data extraction, and processing.
    """
    
    def __init__(self, config: ResourceConfig, output_dir: Optional[str] = None,
        rate_limit: float = 1.0):
        super().__init__(config)
        self.output_dir = Path(output_dir) if output_dir else Path("scraped_data")
        self.rate_limit = rate_limit  # Seconds between requests
        self.logger = logging.getLogger("WebScrapingExecutor")
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def execute_scraping_pipeline(self, urls: List[str], 
                                extraction_rules: List[str] = None) -> Dict[str, Any]:
        """
        Execute web scraping pipeline
        
        Args:
            urls: List of URLs to scrape
            extraction_rules: List of data extraction rules
            
        Returns:
            Scraping pipeline results
        """
        #from .basic_tasks import NetworkTask
        
        self.logger.info(f"Starting web scraping pipeline for {len(urls)} URLs")
        
        extraction_rules = extraction_rules or ["title", "content", "links"]
        pipeline_start = time.time()
        
        # Web scraping phase
        scraping_tasks = []
        for i in range(0, len(urls), 10):  # Process in batches of 10
            batch_urls = urls[i:i+10]
            task = NetworkTask(
                f"scrape_batch_{i//10}",
                urls=batch_urls,
                num_requests=len(batch_urls),
                concurrent_requests=min(3, len(batch_urls)),  # Respect rate limits
                mock_mode=True  # Use mock mode for testing
            )
            scraping_tasks.append(task)
        
        scraping_results = self.execute_parallel_tasks(scraping_tasks)
        
        # Data processing phase
        processing_tasks = [
            CPUIntensiveTask(f"process_batch_{i}", duration=3, intensity=0.5)
            for i in range(len(scraping_results))
        ]
        processing_results = self.execute_parallel_tasks(processing_tasks)
        
        # Data storage phase
        storage_tasks = [
            IOIntensiveTask(f"store_batch_{i}", file_size_mb=5, duration=2)
            for i in range(len(processing_results))
        ]
        storage_results = self.execute_parallel_tasks(storage_tasks)
        
        pipeline_duration = time.time() - pipeline_start
        
        scraping_summary = {
            "pipeline_type": "web_scraping",
            "urls_processed": len(urls),
            "extraction_rules": extraction_rules,
            "pipeline_duration": pipeline_duration,
            "scraping_success_rate": sum(1 for r in scraping_results if r.success) / len(scraping_results) * 100,
            "processing_success_rate": sum(1 for r in processing_results if r.success) / len(processing_results) * 100,
            "storage_success_rate": sum(1 for r in storage_results if r.success) / len(storage_results) * 100,
            "total_batches": len(scraping_results),
            "rate_limit": self.rate_limit,
            "output_directory": str(self.output_dir)
        }
        
        self.logger.info(f"Web scraping pipeline completed in {pipeline_duration:.2f}s")
        return scraping_summary


# Utility functions for creating specialized workflows

def create_bioinformatics_workflow(samples: List[str], workflow_type: str = "variant_calling") -> BioinformaticsExecutor:
    """
    Create a pre-configured bioinformatics workflow
    
    Args:
        samples: List of sample identifiers
        workflow_type: Type of workflow ('variant_calling', 'rna_seq', 'alignment')
        
    Returns:
        Configured BioinformaticsExecutor
    """
    # Configure resources based on workflow type
    if workflow_type == "variant_calling":
        config = ResourceConfig(cpus=8, memory_gb=16.0, max_processes=4, timeout_seconds=3600)
    elif workflow_type == "rna_seq":
        config = ResourceConfig(cpus=6, memory_gb=12.0, max_processes=3, timeout_seconds=2400)
    else:  # alignment
        config = ResourceConfig(cpus=4, memory_gb=8.0, max_processes=2, timeout_seconds=1800)
    
    executor = BioinformaticsExecutor(
        config=config,
        reference_genome="reference_genome.fa",
        annotation_file="annotations.gtf",
        output_dir=f"{workflow_type}_results"
    )
    
    return executor


def create_data_processing_workflow(data_size: str = "medium") -> DataProcessingExecutor:
    """
    Create a pre-configured data processing workflow
    
    Args:
        data_size: Size of data to process ('small', 'medium', 'large')
        
    Returns:
        Configured DataProcessingExecutor
    """
    size_configs = {
        "small": ResourceConfig(cpus=2, memory_gb=4.0, max_processes=2, timeout_seconds=600),
        "medium": ResourceConfig(cpus=4, memory_gb=8.0, max_processes=4, timeout_seconds=1200),
        "large": ResourceConfig(cpus=8, memory_gb=16.0, max_processes=6, timeout_seconds=2400)
    }
    
    config = size_configs.get(data_size, size_configs["medium"])
    
    executor = DataProcessingExecutor(
        config=config,
        input_dir="input_data",
        output_dir=f"processed_data_{data_size}",
        chunk_size=1000 if data_size == "small" else 5000 if data_size == "medium" else 10000
    )
    
    return executor


def create_ml_workflow(model_complexity: str = "medium") -> MachineLearningExecutor:
    """
    Create a pre-configured machine learning workflow
    
    Args:
        model_complexity: Complexity of the model ('simple', 'medium', 'complex')
        
    Returns:
        Configured MachineLearningExecutor
    """
    complexity_configs = {
        "simple": ResourceConfig(cpus=2, memory_gb=4.0, max_processes=2, timeout_seconds=900),
        "medium": ResourceConfig(cpus=4, memory_gb=8.0, max_processes=3, timeout_seconds=1800),
        "complex": ResourceConfig(cpus=8, memory_gb=16.0, max_processes=4, timeout_seconds=3600)
    }
    
    config = complexity_configs.get(model_complexity, complexity_configs["medium"])
    
    executor = MachineLearningExecutor(
        config=config,
        model_dir=f"models_{model_complexity}",
        data_dir=f"ml_data_{model_complexity}"
    )
    
    return executor


def create_web_scraping_workflow(scale: str = "medium", rate_limit: float = 1.0) -> WebScrapingExecutor:
    """
    Create a pre-configured web scraping workflow
    
    Args:
        scale: Scale of scraping operation ('small', 'medium', 'large')
        rate_limit: Rate limit between requests (seconds)
        
    Returns:
        Configured WebScrapingExecutor
    """
    scale_configs = {
        "small": ResourceConfig(cpus=2, memory_gb=2.0, max_processes=2, timeout_seconds=600),
        "medium": ResourceConfig(cpus=4, memory_gb=4.0, max_processes=3, timeout_seconds=1200),
        "large": ResourceConfig(cpus=6, memory_gb=8.0, max_processes=4, timeout_seconds=2400)
    }
    
    config = scale_configs.get(scale, scale_configs["medium"])
    
    executor = WebScrapingExecutor(
        config=config,
        output_dir=f"scraped_data_{scale}",
        rate_limit=rate_limit
    )
    
    return executor


# Example usage and demonstration functions

def demonstrate_bioinformatics_pipeline():
    """Demonstrate bioinformatics pipeline usage"""
    print("=== Bioinformatics Pipeline Demo ===")
    
    # Create executor
    executor = create_bioinformatics_workflow(
        samples=["sample1", "sample2", "sample3"],
        workflow_type="rna_seq"
    )
    
    # Simulate sample files
    fastq_files = ["sample1.fastq", "sample2.fastq", "sample3.fastq"]
    
    # Execute RNA-seq pipeline
    results = executor.execute_rna_seq_pipeline(fastq_files, quantification_method="salmon")
    
    print(f"Pipeline completed: {results['samples_processed']} samples processed")
    print(f"QC success rate: {results['qc_success_rate']:.1f}%")
    print(f"Quantification success rate: {results['quantification_success_rate']:.1f}%")
    print(f"Total duration: {results['pipeline_duration']:.2f} seconds")
    
    return results


def demonstrate_data_processing_pipeline():
    """Demonstrate data processing pipeline usage"""
    print("\n=== Data Processing Pipeline Demo ===")
    
    # Create executor
    executor = create_data_processing_workflow(data_size="medium")
    
    # Simulate data sources
    data_sources = ["database1", "api_endpoint1", "file_source1", "stream1"]
    transformations = ["clean", "normalize", "aggregate", "validate"]
    
    # Execute ETL pipeline
    results = executor.execute_etl_pipeline(data_sources, transformations)
    
    print(f"ETL Pipeline completed: {results['data_sources']} sources processed")
    print(f"Extract success rate: {results['extract_success_rate']:.1f}%")
    print(f"Transform success rate: {results['transform_success_rate']:.1f}%")
    print(f"Load success rate: {results['load_success_rate']:.1f}%")
    print(f"Total records processed: {results['total_records_processed']}")
    print(f"Total duration: {results['pipeline_duration']:.2f} seconds")
    
    return results


def demonstrate_ml_pipeline():
    """Demonstrate machine learning pipeline usage"""
    print("\n=== Machine Learning Pipeline Demo ===")
    
    # Create executor
    executor = create_ml_workflow(model_complexity="medium")
    
    # Simulate datasets
    datasets = ["training_data.csv", "validation_data.csv", "test_data.csv"]
    
    # Execute training pipeline
    results = executor.execute_training_pipeline(
        datasets=datasets,
        model_type="neural_network",
        epochs=20
    )
    
    print(f"ML Training completed: {results['model_type']} model")
    print(f"Datasets processed: {results['datasets']}")
    print(f"Epochs: {results['epochs']}")
    print(f"Training success: {results['training_success']}")
    print(f"Validation success: {results['validation_success']}")
    print(f"Model accuracy: {results['model_accuracy']:.2%}")
    print(f"Total duration: {results['pipeline_duration']:.2f} seconds")
    
    return results


def demonstrate_web_scraping_pipeline():
    """Demonstrate web scraping pipeline usage"""
    print("\n=== Web Scraping Pipeline Demo ===")
    
    # Create executor
    executor = create_web_scraping_workflow(scale="medium", rate_limit=0.5)
    
    # Simulate URLs to scrape
    urls = [
        "https://example.com/page1",
        "https://example.com/page2", 
        "https://example.com/page3",
        "https://example.com/page4",
        "https://example.com/page5"
    ]
    
    extraction_rules = ["title", "content", "links", "images"]
    
    # Execute scraping pipeline
    results = executor.execute_scraping_pipeline(urls, extraction_rules)
    
    print(f"Web Scraping completed: {results['urls_processed']} URLs processed")
    print(f"Scraping success rate: {results['scraping_success_rate']:.1f}%")
    print(f"Processing success rate: {results['processing_success_rate']:.1f}%")
    print(f"Storage success rate: {results['storage_success_rate']:.1f}%")
    print(f"Total batches: {results['total_batches']}")
    print(f"Total duration: {results['pipeline_duration']:.2f} seconds")
    
    return results


def run_all_demonstrations():
    """Run all pipeline demonstrations"""
    print("Running all specialized executor demonstrations...\n")
    
    try:
        bio_results = demonstrate_bioinformatics_pipeline()
    except Exception as e:
        print(f"Bioinformatics demo failed: {e}")
        bio_results = None
    
    try:
        data_results = demonstrate_data_processing_pipeline()
    except Exception as e:
        print(f"Data processing demo failed: {e}")
        data_results = None
    
    try:
        ml_results = demonstrate_ml_pipeline()
    except Exception as e:
        print(f"ML demo failed: {e}")
        ml_results = None
    
    try:
        scraping_results = demonstrate_web_scraping_pipeline()
    except Exception as e:
        print(f"Web scraping demo failed: {e}")
        scraping_results = None
    
    print("\n=== All Demonstrations Completed ===")
    
    return {
        "bioinformatics": bio_results,
        "data_processing": data_results,
        "machine_learning": ml_results,
        "web_scraping": scraping_results
    }


if __name__ == "__main__":
    # Run demonstrations if script is executed directly
    results = run_all_demonstrations()
    
    # Print summary
    print("\n=== Final Summary ===")
    successful_demos = sum(1 for result in results.values() if result is not None)
    print(f"Successfully completed {successful_demos}/4 demonstrations")
    
    for demo_name, result in results.items():
        if result:
            print(f"✅ {demo_name.replace('_', ' ').title()}: Success")
        else:
            print(f"❌ {demo_name.replace('_', ' ').title()}: Failed")
