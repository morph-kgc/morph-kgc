import rdflib

udf_dict = {}

def udf(fun_id, **params):
    """
    We borrow the idea of using decorators from pyRML by Andrea Giovanni Nuzzolese.
    """

    def wrapper(funct):
        udf_dict[fun_id] = {}
        udf_dict[fun_id]['function'] = funct
        udf_dict[fun_id]['parameters'] = params
        return funct
    return wrapper


@udf(
    fun_id='http://www.example.org/mapping-functions/timestampToXSDDatetime',
    timestamp='http://www.example.org/mapping-functions/timestamp'
)
def timestamp_to_datetime(timestamp: str):
    from datetime import datetime
    dt = datetime.fromtimestamp(int(timestamp))
    return rdflib.Literal(dt.isoformat(), datatype=rdflib.XSD.dateTime)