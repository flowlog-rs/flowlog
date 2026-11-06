import sys
from pathlib import Path
JOINT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(JOINT / "binary-raw"))
from search_raw import RawModel
sys.path.insert(0, str(JOINT / "binary-aggregate"))
from search_aggregate import AggregateModel
