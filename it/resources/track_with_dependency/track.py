async def noop(es, params):
    # pylint: disable=import-outside-toplevel,import-error
    # We want to use a library that will never be a Rally dependency, otherwise the test
    # could succeed regardless of whether the dependencies function actually succeeds.
    # For this reason we're using pytoml, which was deprecated long ago
    import pytoml as toml

    ret_val = {}
    # no-ops is our correctness marker
    toml_values = ["weight = 1", 'unit = "no-ops"']
    await es.cluster.health()
    for toml_value in toml_values:
        ret_val.update(toml.loads(toml_value))
    return ret_val


def register(registry):
    registry.register_runner("no-op", noop, async_runner=True)
