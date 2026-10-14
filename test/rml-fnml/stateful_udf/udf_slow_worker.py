__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
The stateful UDF of udf.py, except that the worker process executing it says so
on its standard output, and then keeps working until the process running the
mapping is gone, or for 30 seconds.
"""

import multiprocessing
import os
import time


def load_country_names(initialization):
    return {}


@stateful_udf(
    fun_id='http://example.com/countryName',
    initializer=load_country_names,
    code='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    resource='urn:morph:function:resource')
def country_name(code, context, resource=None):
    parent = multiprocessing.parent_process()
    if parent is not None:
        print('materializing', flush=True)
        deadline = time.monotonic() + 30
        # A process whose parent is gone gets a new parent.
        while os.getppid() == parent.pid and time.monotonic() < deadline:
            time.sleep(0.05)
    return context.get(code)
