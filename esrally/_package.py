from __future__ import annotations

import importlib
import pkgutil


# https://github.com/indygreg/PyOxidizer/issues/457
def _patch_pkgutil() -> None:
    """Teach pkgutil.get_data() how to read files from in-memory resources.
    This is required for jsonschema."""

    def get_data_custom(package: str, resource: str) -> bytes | None:
        try:
            module = importlib.import_module(package)
            reader = module.__loader__.get_resource_reader(package)  # type: ignore[attr-defined]
            with reader.open_resource(resource) as f:
                return f.read()
        except Exception:
            return None

    pkgutil.get_data = get_data_custom


def setup_frozen_package():
    _patch_pkgutil()
