async def noop(es, params):
    # pylint: disable=import-outside-toplevel,import-error
    import tomli as tomllib

    ret_val = {}
    # no-ops is our correctness marker
    toml_values = ["weight = 1", 'unit = "no-ops"']
    await es.cluster.health()
    for toml_value in toml_values:
        ret_val.update(tomllib.loads(toml_value))
    return ret_val


def register(registry):
    registry.register_runner("no-op", noop, async_runner=True)
