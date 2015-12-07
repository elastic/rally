import os
import errno
import glob
import subprocess
import signal
import bz2
# from zipfile import ZipFile as zip

import rally.utils.process


def ensure_dir(directory):
  # avoid a race condition by trying to create the checkout directory
  try:
    os.makedirs(directory)
  except OSError as exception:
    if exception.errno != errno.EEXIST:
      raise


def unzip(zip_name, target_directory):
  filename, extension = os.path.splitext(zip_name)
  if extension == ".zip":
    # Actually this would be much better if it just would preserve file permissions...
    # z = zip(zip_name)
    # for f in z.namelist():
    #  z.extract(f, path=target_directory)
    if not rally.utils.process.run_subprocess("unzip %s -d %s" % (zip_name, target_directory)):
      raise RuntimeError("Could not unzip %s to %s" % (zip_name, target_directory))
  elif extension == ".bz2":
    # We rather avoid external tools as much as possible to simplify Rally's setup, hence we use the library functions
    target_file = os.path.join(target_directory, filename)
    with open(target_file, 'wb') as extracted, bz2.BZ2File(zip_name, 'rb') as file:
      for data in iter(lambda: file.read(100 * 1024), b''):
        extracted.write(data)
  else:
    raise RuntimeError("Unsupported file extension '%s'. Cannot unzip '%s'" % (extension, zip_name))


def guess_install_location(binary_name, fallback=None):
  try:
    lines = subprocess.Popen(['which', binary_name], stdout=subprocess.PIPE).communicate()[0].splitlines()
    return lines[0].decode('utf-8')
  except BaseException:
    # could not determine location
    return fallback


def guess_java_home(major_version=8, fallback=None):
  try:
    return os.environ['JAVA_HOME']
  except KeyError:
    pass
  # obviously JAVA_HOME is not set, we try a bit harder for our developers on a Mac
  results = glob.glob("/Library/Java/JavaVirtualMachines/jdk1.%s*.jdk" % major_version)
  # don't do magic guesses if there are multiple versions and have the user specify one
  if results and len(results) == 1:
    return results[0] + "/Contents/Home"
  else:
    return fallback


# TODO dm: Consider creating a ps.py
# TODO dm: This is quite brutal - can we change it to just killing running Elasticsearch instances? -> Check with Mike on the intention.
def kill_java():
  for line in subprocess.Popen(['ps', '-A'], stdout=subprocess.PIPE).communicate()[0].splitlines():
    line = line.decode('utf-8')
    if 'java' in line and 'com.intellij' not in line and 'org.jetbrains.idea' not in line:
      pid = int(line.split(None, 1)[0])
      os.kill(pid, signal.SIGKILL)
