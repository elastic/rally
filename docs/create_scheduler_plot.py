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
