import shlex
import logging
import subprocess

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
