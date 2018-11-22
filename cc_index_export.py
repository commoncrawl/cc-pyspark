from sparkcc import CCIndexSparkJob


class CCIndexExportJob(CCIndexSparkJob):
    """ Export rows matching a SQL query on the columnar URL index """

    name = "CCIndexExport"


if __name__ == '__main__':
    job = CCIndexExportJob()
    job.run()
