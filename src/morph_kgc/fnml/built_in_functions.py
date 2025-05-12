"""The functions are described mainly in the grel documentation: https://users.ugent.be/~bjdmeest/function/grel.ttl#
"""

__author__ = "Julián Arenas-Guerrero"
import re

__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"

from .grel.string_functions import bif_dict as string_bif
from .grel.array_functions import bif_dict as array_bif
from .grel.date_functions import bif_dict as date_bif
from .grel.control_functions import bif_dict as control_bif
from .grel.other_functions import bif_dict as other_bif
from .grel.math_functions import bif_dict as math_bif
from .function_decorator import bif

bif_dict = {}
bif_dict.update(string_bif)
bif_dict.update(array_bif)
bif_dict.update(date_bif)
bif_dict.update(math_bif)
bif_dict.update(control_bif)
bif_dict.update(other_bif)



