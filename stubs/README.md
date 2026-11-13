# Local type stubs

Type stubs (`.pyi` files) for third-party packages that ship neither inline annotations nor a
`py.typed` marker, so that mypy can type check Rally's use of them instead of treating them as
`Any`. The directory is wired into mypy through `mypy_path` in `pyproject.toml`.

Stubs here are partial on purpose: they describe the API Rally actually uses, with the exact
parameter names of the upstream library. Extend them as Rally starts using more of a package,
and keep them in sync when the pinned version of the package changes.

- `thespian/`: [Thespian](https://thespianpy.com/) actor library, pinned in `pyproject.toml`.
  Written against thespian 4.0.1 from `thespian/actors.py`.
