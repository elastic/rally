import os
import errno
import subprocess
import signal
#from zipfile import ZipFile as zip


def ensure_dir(directory):
  # avoid a race condition by trying to create the checkout directory
  try:
    os.makedirs(directory)
  except OSError as exception:
    if exception.errno != errno.EEXIST:
      raise


def unzip(zip_name, target_directory):
  # Actually this would be much better if it just would preserve file permissions...
  #z = zip(zip_name)
  #for f in z.namelist():
  #  z.extract(f, path=target_directory)
  #TODO dm: Causes problems the second time - no cwd? Check this
#  if os.system('unzip %s -d %s > unzip.log 2>&1' % (zip_name, target_directory)):
#    raise RuntimeError("Could not unzip %s to %s" % (zip_name, target_directory))
#  l = os.listdir('.')
#  # we leave it just when something is wrong...
#  l.remove('unzip.log')
  if os.system('unzip %s -d %s 2>&1' % (zip_name, target_directory)):
    raise RuntimeError("Could not unzip %s to %s" % (zip_name, target_directory))


# TODO dm: Consider creating a ps.py
def kill_java():
  for line in subprocess.Popen(['ps', '-A'], stdout=subprocess.PIPE).communicate()[0].splitlines():
    line = line.decode('utf-8')
    if 'java' in line and 'com.intellij' not in line and 'org.jetbrains.idea' not in line:
      pid = int(line.split(None, 1)[0])
      print('kill -9 %s: %s' % (pid, line.strip()))
      os.kill(pid, signal.SIGKILL)
