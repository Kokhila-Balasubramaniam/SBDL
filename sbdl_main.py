import sys
from lib.logger import Log4j
from lib import Utils

if __name__=='__main.py__':
    if(len(sys.argv)<3):
        print("Usage: spdl{prod,local,dev}{load_date}:Arguments missing")
        sys.exit(-1)

    job_run_env=sys.argv[1].upper()
    load_date=sys.argv[2]

    spark=Utils.get_spark_session(job_run_env)

    logger=Log4j(spark)
    logger.warn("finished creating spark session")
    logger.info("one more msg")