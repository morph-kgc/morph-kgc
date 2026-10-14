__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
The stateful UDF of udf.py, except that the worker process executing it is
killed, the way the operating system kills a process that runs out of memory.
"""

import multiprocessing
import os
import signal


def load_country_names(initialization):
    return {}


@stateful_udf(
    fun_id='http://example.com/countryName',
    initializer=load_country_names,
    code='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    resource='urn:morph:function:resource')
def country_name(code, context, resource=None):
    # Only a worker process is killed, never the process running the mapping.
    if multiprocessing.parent_process() is not None:
        os.kill(os.getpid(), signal.SIGKILL)
    return context.get(code)
