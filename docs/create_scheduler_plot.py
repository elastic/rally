# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys
# we don't include matplotlib in setup.py as it's just a development dependency
try:
    import matplotlib
    import matplotlib.pyplot as plt
except ImportError:
    print("Please install matplotlib")
    exit(1)

from esrally.driver import scheduler


def main():
    def n(data, sched):
        curr = data[-1] if len(data) > 0 else 0
        data.append(sched.next(curr))

    tnd = [0]
    tnp = [0]
    yn = [0]
    sd = scheduler.DeterministicScheduler({"target-interval": 1})
    sp = scheduler.PoissonScheduler({"target-interval": 1})
    # generate data for a little bit longer time frame...
    for i in range(0, 20):
        n(tnd, sd)
        n(tnp, sp)
        yn.append(0)

    fig, axs = plt.subplots(2, 1, sharex=True)
    fig.subplots_adjust(left=0.08, right=0.98, hspace=0.5)

    axes = plt.gca()
    # ... but only show the first ten seconds which makes for a nicer picture
    axes.set_xlim([0, 10])

    ax = axs[0]
    ax.plot(tnd, yn, "o-")
    ax.set_title("Deterministic schedule")

    ax = axs[1]
    ax.set_title("Poisson schedule")
    ax.plot(tnp, yn, "o-")

    plt.xlabel('time [s]')

    if len(sys.argv) == 2:
        output_file_path = sys.argv[1]
        print("Saving output to [%s]" % output_file_path)
        plt.savefig(output_file_path, bbox_inches='tight')
    plt.show()


if __name__ == '__main__':
    main()
