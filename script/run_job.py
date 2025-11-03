import argparse
import importlib
import inspect
import sys

from cc_pyspark.sparkcc import CCSparkJob


def load_and_run_job(module_name: str):
    job_module = importlib.import_module(module_name)

    # Find the job class in the module
    job_class = None
    for name, obj in inspect.getmembers(job_module, inspect.isclass):
        if obj.__module__ == job_module.__name__ and issubclass(obj, CCSparkJob):
            print("found job class:", obj)
            job_class = obj
            break

    if job_class is None:
        raise ValueError(f"No CCSparkJob subclass found in module {module_name}")

    job_instance = job_class()
    print("running job:", job_instance)
    job_instance.run()


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--job_module', type=str, required=True,)

    args, remaining = arg_parser.parse_known_args()

    # remove wrapper args from sys.argv so that job class can parse its own args cleanly
    sys.argv = [sys.argv[0]] + remaining

    load_and_run_job(args.job_module)


if __name__ == '__main__':
    main()


