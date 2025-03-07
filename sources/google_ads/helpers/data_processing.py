from typing import Any, Iterator
from dlt.common.typing import TDataItem
import proto
import json
from google.protobuf import json_format

def to_dict(item: Any) -> Iterator[TDataItem]:
    """
    Processes a batch result (page of results per dimension) accordingly
    :param batch:
    :return:
    """
    json_str = json_format.MessageToJson(item._pb)
    yield json.loads(json_str)
