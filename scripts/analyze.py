#
# Simple helper script to create graphs based on multiple race.json files (it's a summary of the results of a single race which is
# stored in ~/.rally/benchmarks/races/RACE_TS/). There is no specific integration into Rally and it is also not installed with Rally.
#
# It requires matplotlib (install with pip3 install matplotlib).
#
#
# Usage: python3 analyze.py /path1/to/race.json /path2/to/race.json.
#
# Output: A bunch of .png files in the current directory. Each graph shows one data series per race and we use the chosen car as label. If
#         you want to change that you need to choose a different key in `#data_series_name()`.
#


import json
import sys

import matplotlib.pyplot as plt


def present(a_plot, name):
    a_plot.savefig("%s.png" % name, bbox_inches='tight')
    # plt.show()  # alternatively only show it
    # explicitly close to free resources
    a_plot.close()


def decode_percentile_key(k):
    return float(k.replace("_", "."))


def data_series_name(d):
    return d["car"]


def include(series):
    return True


def plot_service_time(raw_data):
    service_time_per_op = {}

    for d in raw_data:
        data_series = data_series_name(d)
        for op_metrics in d["results"]["op_metrics"]:
            operation = op_metrics["operation"]
            service_time_metrics = op_metrics["service_time"]
            if operation not in service_time_per_op:
                service_time_per_op[operation] = []
            service_time_per_op[operation].append({
                "data_series": data_series,
                "percentiles": [decode_percentile_key(p) for p in service_time_metrics.keys()],
                "percentile_values": list(service_time_metrics.values()),
            })

    for op, results in service_time_per_op.items():
        plt.rcdefaults()
        fig, ax = plt.subplots()

        legend_handles = []
        legend_labels = []

        for candidate in results:
            label = candidate["data_series"]
            series = ax.plot(candidate["percentiles"], candidate["percentile_values"], marker='.', label=label)
            legend_handles.append(series[0])
            legend_labels.append(label)

        # makes matters even worse because it compresses higher values.
        # ax.set_xscale('log')

        ax.set_ylabel("Service Time [ms]")
        ax.set_xlabel("Percentile")
        ax.set_title("Service Time of %s" % op)

        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        ax.legend(legend_handles, legend_labels, loc='center left', bbox_to_anchor=(1, 0.5))

        present(plt, "service_time_%s" % op)


def plot_throughput(raw_data):
    throughput_per_op = {}
    unit = ""

    for d in raw_data:
        data_series = data_series_name(d)
        for op_metrics in d["results"]["op_metrics"]:
            operation = op_metrics["operation"]
            throughput_metrics = op_metrics["throughput"]
            if operation not in throughput_per_op:
                throughput_per_op[operation] = []
            throughput_per_op[operation].append({
                "data_series": data_series,
                "max": throughput_metrics["max"],
                "median": throughput_metrics["median"],
                "min": throughput_metrics["min"]
            })
            # should all be the same
            unit = throughput_metrics["unit"]

    for op, results in throughput_per_op.items():
        plt.rcdefaults()
        fig, ax = plt.subplots()
        x_tick_labels = []
        throughput = []
        min_throughput = []
        max_throughput = []
        width = 0.35

        for candidate in results:
            x_tick_labels.append(candidate["data_series"])
            min_throughput.append(candidate["median"] - candidate["min"])
            throughput.append(candidate["median"])
            max_throughput.append(candidate["max"] - candidate["median"])

        indices = range(len(throughput))

        ax.bar(indices, throughput, width, yerr=[min_throughput, max_throughput])
        ax.set_xticks(indices)
        ax.set_xticklabels(x_tick_labels)
        ax.set_ylabel("Throughput [%s]" % unit)
        ax.set_title("Throughput of %s" % op)

        present(plt, "throughput_%s" % op)


def plot_gc_times(raw_data):
    plt.rcdefaults()
    fig, ax = plt.subplots()

    x_tick_labels = []
    old_gc_times = []
    young_gc_times = []
    width = 0.35

    for d in raw_data:
        data_series = data_series_name(d)

        x_tick_labels.append(data_series)

        old_gc_times.append(d["results"]["old_gc_time"])
        young_gc_times.append(d["results"]["young_gc_time"])

    indices = range(len(old_gc_times))

    old_bar = ax.bar(indices, old_gc_times, width)
    ax.set_xticks([x + width / 2 for x in indices])
    ax.set_xticklabels(x_tick_labels)
    ax.set_ylabel("Total Duration [ms]")
    ax.set_title("GC Times")

    indices = [x + width for x in indices]
    young_bar = ax.bar(indices, young_gc_times, width)

    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])

    ax.legend([old_bar[0], young_bar[0]], ["Old GC", "Young GC"], loc='center left', bbox_to_anchor=(1, 0.5))

    present(plt, "gc_times")


def plot(raw_data):
    plot_gc_times(raw_data)
    plot_throughput(raw_data)
    plot_service_time(raw_data)


def main():
    series = []

    for f in sys.argv[1:]:
        a_series = json.load(open(f, "rt"))
        if include(a_series):
            series.append(a_series)
    plot(series)


if __name__ == '__main__':
    main()
