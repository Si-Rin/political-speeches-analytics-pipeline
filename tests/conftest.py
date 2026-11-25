import sys
import types

# faster_whisper is heavy and irrelevant to collection tests
sys.modules.setdefault("faster_whisper", types.SimpleNamespace(WhisperModel=object))
