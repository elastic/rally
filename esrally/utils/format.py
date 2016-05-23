def link(href):
    return "\033[4m%s\033[0m" % href


def red(message):
    return "\033[31;1m%s\033[0m" % message


def green(message):
    return "\033[32;1m%s\033[0m" % message


def yellow(message):
    return "\033[33;1m%s\033[0m" % message


def neutral(message):
    return "\033[39;1m%s\033[0m" % message