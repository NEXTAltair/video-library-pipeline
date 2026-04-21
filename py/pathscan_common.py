"""Compatibility wrapper for the new package layout."""

from importlib import import_module as _import_module

_module = _import_module("video_pipeline.platform.pathscan_common")
globals().update({name: getattr(_module, name) for name in dir(_module) if not name.startswith("__")})
__all__ = [name for name in dir(_module) if not name.startswith("__")]
