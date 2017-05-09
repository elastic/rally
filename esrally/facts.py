import json
import os

from esrally import version, actor, exceptions
from esrally.utils import sysstats, console, convert, process


def list_facts(cfg):
    console.warn("This command is deprecated and will be removed with the next release of Rally.", overline="!", underline="!")
    # provide a custom error message
    target_hosts = cfg.opts("facts", "hosts", mandatory=False)
    if not target_hosts:
        raise exceptions.SystemSetupError("Please define a target host with --target-hosts")
    if len(target_hosts) > 1:
        raise exceptions.SystemSetupError("Only one target host is supported at the moment but you provided %s" % target_hosts)

    # at this point an actor system has to run and we should only join
    actor_system = actor.bootstrap_actor_system(try_join=True)
    facts_actor = actor_system.createActor(FactsActor, targetActorRequirements={"ip": target_hosts[0]})
    result = actor_system.ask(facts_actor, GatherFacts())
    if isinstance(result, Facts):
        console.println(json.dumps(result.facts, indent="  "))
    else:
        raise exceptions.RallyError("Could not gather facts: [%s]." % str(result))


def _jvm_property(key):
    # maybe we should use `java -XshowSettings:properties` and do a little bit of parsing.
    # Also: We'd need to set JAVA_HOME to what's in the config!
    lines = process.run_subprocess_with_output("java -cp %s SysProps %s" % (os.path.join(os.path.dirname(__file__), "resources"), key))
    if len(lines) == 1:
        return lines[0].strip()
    else:
        return lines


def _facts():
    disks = []
    for disk in sysstats.disks():
        dev, _, fstype, _ = disk
        disks.append({
            "device": dev,
            "type": "TODO: Provide one of 'ssd', 'spinning'",
            "file-system": fstype,
        })
    return {
        "environment": {
            "type": "TODO: Provide one of 'cloud', 'bare-metal' + details about the environment (EC2, instance type)",
        },
        "hardware": {
            "cpu_model": sysstats.cpu_model(),
            "disk": disks,
            "memory": "%dgb" % convert.bytes_to_gb(sysstats.total_memory())
        },
        "software": {
            "jvm_vendor": _jvm_property("java.vm.vendor"),
            "jvm_version": _jvm_property("java.runtime.version"),
            "os_name": sysstats.os_name(),
            "os_version": sysstats.os_version(),
            "rally_version": version.version(),
            "distribution_version": "TODO: Provide Elasticsearch distribution version"
        }
    }


class GatherFacts:
    pass


class Facts:
    def __init__(self, facts):
        self.facts = facts


class FactsActor(actor.RallyActor):
    def receiveMessage(self, msg, sender):
        if isinstance(msg, GatherFacts):
            self.send(sender, Facts(_facts()))
