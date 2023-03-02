import nox


@nox.session(python=["3.8", "3.9", "3.10"])
def test(session: nox.Session) -> None:
    session.install(".[develop]")
    session.run("pytest")
