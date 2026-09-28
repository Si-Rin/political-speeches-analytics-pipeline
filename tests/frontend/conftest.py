import sys
from pathlib import Path

FRONTEND = Path(__file__).resolve().parent.parent.parent / "frontend"
if str(FRONTEND) not in sys.path:
    sys.path.insert(0, str(FRONTEND))
