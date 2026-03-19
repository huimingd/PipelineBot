class BioinformaticsExecutor(TaskExecutor):
    def execute_alignment_pipeline(self, fastq_files):
        # Custom pipeline implementation
        tasks = [AlignmentTask(f) for f in fastq_files]
        return self.execute_parallel_tasks(tasks)

# Use specialized executor
bio_executor = BioinformaticsExecutor(config)
results = bio_executor.execute_alignment_pipeline(["sample1.fastq", "sample2.fastq"])
