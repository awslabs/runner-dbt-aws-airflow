"""Worker entry-point for dbt-aws.

Uploaded to S3 once. Used by every backend that expects a top-level
``.py`` to exec:

* Glue Spark Job  -> ``ScriptLocation``
* Glue Python Shell Job  -> ``ScriptLocation``
* EMR Serverless (PySpark)  -> ``entryPoint``
* EMR cluster step (spark-submit)  -> application path

The real logic lives in :mod:`dbt_aws.common.runtime`, installed on
the worker via ``--additional-python-modules`` (Glue) or
``--py-files`` / ``spark.archives`` (EMR).

Exit-code handling note: Glue PySpark jobs (``Command.Name='glueetl'``)
interpret ``SystemExit`` with ANY code as abnormal termination -- even
``SystemExit(0)``. On success we let the script end naturally; only
non-zero return codes raise ``SystemExit`` to propagate failure to the
JobRun state.
"""

import sys

from dbt_aws.common.runtime import main

if __name__ == "__main__":
    rc = main()
    if rc != 0:
        sys.exit(rc)
