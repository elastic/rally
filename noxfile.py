import nox


@nox.session(python=["3.8", "3.9", "3.10"])
def test(session: nox.Session) -> None:
    session.install(".[develop]")
    session.run("pytest")


@nox.session(python=["3.8", "3.9", "3.10"])
def it(session: nox.Session) -> None:
    session.install(".[develop]")
    session.run("pytest", "-s", "it")
