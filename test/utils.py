
def _process_jobs(partition_index, records, job):
    for record in records:
        for result in job.process_record(record):
            yield result
