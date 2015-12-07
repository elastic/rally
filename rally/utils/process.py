import shlex
import logging
import subprocess
import signal
import os

logger = logging.getLogger("rally.process")


def run_subprocess(command_line):
  command_line_args = shlex.split(command_line)
  logger.debug("Invoking subprocess '%s'" % command_line)
  try:
    command_line_process = subprocess.Popen(
      command_line_args,
      stdout=subprocess.PIPE,
      stderr=subprocess.STDOUT,
    )
    has_output = True
    while has_output:
      line = command_line_process.stdout.readline()
      if line:
        logger.info(line)
      else:
        has_output = False
  except OSError as exception:
    logger.warn("Exception occurred when running '%s': '%s'" % (command_line, str(exception)))
    return False
  else:
    logger.debug("Subprocess '%s' finished" % command_line)

  return True


def kill_java():
  # even better than using ps would be jps (for this we need a properly set up JDK on the path)
  for line in subprocess.Popen(['ps', '-A'], stdout=subprocess.PIPE).communicate()[0].splitlines():
    line = line.decode('utf-8')
    # TODO dm: This is quite brutal - can we change it to just killing running Elasticsearch instances? -> Check with Mike on the intention.
    #if 'java' in line and 'com.intellij' not in line and 'org.jetbrains.idea' not in line:
    if 'java' in line and 'elasticsearch' in line:
      pid = int(line.split(None, 1)[0])
      os.kill(pid, signal.SIGKILL)
