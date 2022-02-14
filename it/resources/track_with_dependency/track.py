from track_with_dependency import noop

def register(registry):
    registry.register_runner("no-op", noop)