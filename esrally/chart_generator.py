# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import glob
import json
import logging
import uuid

from esrally import config, exceptions, track
from esrally.utils import console, io

color_scheme_rgba = [
    # #00BFB3
    "rgba(0,191,179,1)",
    # #00A9E0
    "rgba(0,169,224,1)",
    # #F04E98
    "rgba(240,78,152,1)",
    # #FFCD00
    "rgba(255,205,0,1)",
    # #0076A8
    "rgba(0,118,168,1)",
    # #93C90E
    "rgba(147,201,14,1)",
    # #646464
    "rgba(100,100,100,1)",
]


def index_label(race_config):
    if race_config.label:
        return race_config.label

    label = "%s-%s" % (race_config.challenge, race_config.car)
    if race_config.plugins:
        label += "-%s" % race_config.plugins.replace(":", "-").replace(",", "+")
    if race_config.node_count > 1:
        label += " (%d nodes)" % race_config.node_count
    return label


class TimeSeriesCharts:
    @staticmethod
    def format_title(environment, track_name, flavor=None, es_license=None, suffix=None):
        title = [environment, str(track_name)]
        if suffix:
            title.append(suffix)

        return "-".join(title)

    @staticmethod
    def filter_string(environment, race_config):
        if race_config.name:
            return f'environment:"{environment}" AND active:true AND user-tags.name:"{race_config.name}"'
        else:
            return (
                f'environment:"{environment}" AND active:true AND track:"{race_config.track}"'
                f' AND challenge:"{race_config.challenge}" AND car:"{race_config.car}" AND node-count:{race_config.node_count}'
            )

    @staticmethod
    def gc(title, environment, race_config):
        vis_state = {
            "title": title,
            "type": "metrics",
            "params": {
                "axis_formatter": "number",
                "axis_position": "left",
                "id": str(uuid.uuid4()),
                "index_pattern": "rally-results-*",
                "interval": "1d",
                "series": [
                    {
                        "axis_position": "left",
                        "chart_type": "line",
                        "color": "#68BC00",
                        "fill": "0",
                        "formatter": "number",
                        "id": str(uuid.uuid4()),
                        "line_width": "1",
                        "metrics": [{"id": str(uuid.uuid4()), "type": "avg", "field": "value.single"}],
                        "point_size": "3",
                        "seperate_axis": 1,
                        "split_mode": "filters",
                        "stacked": "none",
                        "filter": "",
                        "split_filters": [
                            {
                                "filter": "young_gc_time",
                                "label": "Young Gen GC time",
                                "color": "rgba(0,191,179,1)",
                                "id": str(uuid.uuid4()),
                            },
                            {
                                "filter": "old_gc_time",
                                "label": "Old Gen GC time",
                                "color": "rgba(254,209,10,1)",
                                "id": str(uuid.uuid4()),
                            },
                        ],
                        "label": "GC Times",
                        "value_template": "{{value}} ms",
                        "steps": 0,
                    }
                ],
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "time_field": "race-timestamp",
                "type": "timeseries",
                "filter": TimeSeriesCharts.filter_string(environment, race_config),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "rally-annotations",
                        "query_string": f'((NOT _exists_:track) OR track:"{race_config.track}") AND ((NOT _exists_:chart) OR chart:gc) '
                        f'AND ((NOT _exists_:chart-name) OR chart-name:"{title}") AND environment:"{environment}"',
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "race-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1,
                    }
                ],
                "axis_min": "0",
            },
            "aggs": [],
            "listeners": {},
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "gc",
                "version": 1,
                "kibanaSavedObjectMeta": {"searchSourceJSON": '{"query":"*","filter":[]}'},
            },
        }

    @staticmethod
    def merge_time(title, environment, race_config):
        vis_state = {
            "title": title,
            "type": "metrics",
            "params": {
                "axis_formatter": "number",
                "axis_position": "left",
                "id": str(uuid.uuid4()),
                "index_pattern": "rally-results-*",
                "interval": "1d",
                "series": [
                    {
                        "axis_position": "left",
                        "chart_type": "line",
                        "color": "#68BC00",
                        "fill": "0",
                        "formatter": "number",
                        "id": str(uuid.uuid4()),
                        "line_width": "1",
                        "metrics": [{"id": str(uuid.uuid4()), "type": "avg", "field": "value.single"}],
                        "point_size": "3",
                        "seperate_axis": 1,
                        "split_mode": "filters",
                        "stacked": "none",
                        "filter": "",
                        "split_filters": [
                            {
                                "filter": "merge_time",
                                "label": "Cumulative merge time",
                                "color": "rgba(0,191,179,1)",
                                "id": str(uuid.uuid4()),
                            },
                            {
                                "filter": "merge_throttle_time",
                                "label": "Cumulative merge throttle time",
                                "color": "rgba(254,209,10,1)",
                                "id": str(uuid.uuid4()),
                            },
                        ],
                        "label": "Merge Times",
                        "value_template": "{{value}} ms",
                        "steps": 0,
                    }
                ],
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "time_field": "race-timestamp",
                "type": "timeseries",
                "filter": TimeSeriesCharts.filter_string(environment, race_config),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "rally-annotations",
                        "query_string": f'((NOT _exists_:track) OR track:"{race_config.track}") '
                        f"AND ((NOT _exists_:chart) OR chart:merge_times) "
                        f'AND ((NOT _exists_:chart-name) OR chart-name:"{title}") AND environment:"{environment}"',
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "race-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1,
                    }
                ],
                "axis_min": "0",
            },
            "aggs": [],
            "listeners": {},
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "merge_times",
                "version": 1,
                "kibanaSavedObjectMeta": {"searchSourceJSON": '{"query":"*","filter":[]}'},
            },
        }

    @staticmethod
    def merge_count(title, environment, race_config):
        vis_state = {
            "title": title,
            "type": "metrics",
            "params": {
                "axis_formatter": "number",
                "axis_position": "left",
                "id": str(uuid.uuid4()),
                "index_pattern": "rally-results-*",
                "interval": "1d",
                "series": [
                    {
                        "axis_position": "left",
                        "chart_type": "line",
                        "color": "#68BC00",
                        "fill": "0",
                        "formatter": "number",
                        "id": str(uuid.uuid4()),
                        "line_width": "1",
                        "metrics": [{"id": str(uuid.uuid4()), "type": "avg", "field": "value.single"}],
                        "point_size": "3",
                        "seperate_axis": 1,
                        "split_mode": "filters",
                        "stacked": "none",
                        "filter": "",
                        "split_filters": [
                            {
                                "filter": "merge_count",
                                "label": "Cumulative merge count",
                                "color": "rgba(0,191,179,1)",
                                "id": str(uuid.uuid4()),
                            }
                        ],
                        "label": "Merge Count",
                        "value_template": "{{value}}",
                        "steps": 0,
                    }
                ],
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "time_field": "race-timestamp",
                "type": "timeseries",
                "filter": TimeSeriesCharts.filter_string(environment, race_config),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "rally-annotations",
                        "query_string": f'((NOT _exists_:track) OR track:"{race_config.track}") '
                        f"AND ((NOT _exists_:chart) OR chart:merge_count) "
                        f'AND ((NOT _exists_:chart-name) OR chart-name:"{title}") AND environment:"{environment}"',
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "race-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1,
                    }
                ],
                "axis_min": "0",
            },
            "aggs": [],
            "listeners": {},
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "merge_count",
                "version": 1,
                "kibanaSavedObjectMeta": {"searchSourceJSON": '{"query":"*","filter":[]}'},
            },
        }

    @staticmethod
    def ml_processing_time(title, environment, race_config):
        vis_state = {
            "title": title,
            "type": "metrics",
            "params": {
                "axis_formatter": "number",
                "axis_position": "left",
                "id": str(uuid.uuid4()),
                "index_pattern": "rally-results-*",
                "interval": "1d",
                "series": [
                    {
                        "axis_position": "left",
                        "chart_type": "line",
                        "color": "#68BC00",
                        "fill": "0",
                        "formatter": "number",
                        "id": str(uuid.uuid4()),
                        "line_width": "1",
                        "metrics": [{"id": str(uuid.uuid4()), "type": "avg", "field": "value.mean"}],
                        "point_size": "3",
                        "seperate_axis": 1,
                        "split_mode": "filters",
                        "stacked": "none",
                        "filter": "",
                        "split_filters": [
                            {
                                "filter": "ml_processing_time",
                                "label": "Mean ML processing time",
                                "color": color_scheme_rgba[1],
                                "id": str(uuid.uuid4()),
                            }
                        ],
                        "label": "ML Mean Time",
                        "value_template": "{{value}}",
                        "steps": 0,
                    },
                    {
                        "axis_position": "left",
                        "chart_type": "line",
                        "color": "#68BC00",
                        "fill": "0",
                        "formatter": "number",
                        "id": str(uuid.uuid4()),
                        "line_width": "1",
                        "metrics": [{"id": str(uuid.uuid4()), "type": "avg", "field": "value.median"}],
                        "point_size": "3",
                        "seperate_axis": 1,
                        "split_mode": "filters",
                        "stacked": "none",
                        "filter": "",
                        "split_filters": [
                            {
                                "filter": "ml_processing_time",
                                "label": "Median ML processing time",
                                "color": color_scheme_rgba[0],
                                "id": str(uuid.uuid4()),
                            }
                        ],
                        "label": "ML Median Time",
                        "value_template": "{{value}}",
                        "steps": 0,
                    },
                ],
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "time_field": "race-timestamp",
                "type": "timeseries",
                "filter": TimeSeriesCharts.filter_string(environment, race_config),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "rally-annotations",
                        "query_string": f'((NOT _exists_:track) OR track:"{race_config.track}") '
                        f"AND ((NOT _exists_:chart) OR chart:ml_processing_time) "
                        f'AND ((NOT _exists_:chart-name) OR chart-name:"{title}") AND environment:"{environment}"',
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "race-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1,
                    }
                ],
                "axis_min": "0",
            },
            "aggs": [],
            "listeners": {},
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "ml_processing_time",
                "version": 1,
                "kibanaSavedObjectMeta": {"searchSourceJSON": '{"query":"*","filter":[]}'},
            },
        }

    @staticmethod
    def io(title, environment, race_config):
        vis_state = {
            "title": title,
            "type": "metrics",
            "params": {
                "axis_formatter": "number",
                "axis_position": "left",
                "id": str(uuid.uuid4()),
                "index_pattern": "rally-results-*",
                "interval": "1d",
                "series": [
                    {
                        "axis_position": "left",
                        "chart_type": "line",
                        "color": "#68BC00",
                        "fill": "0",
                        "formatter": "bytes",
                        "id": str(uuid.uuid4()),
                        "line_width": "1",
                        "metrics": [{"id": str(uuid.uuid4()), "type": "sum", "field": "value.single"}],
                        "point_size": "3",
                        "seperate_axis": 1,
                        "split_mode": "filters",
                        "stacked": "none",
                        "filter": "",
                        "split_filters": [
                            {
                                "filter": "name:index_size",
                                "label": "Index Size",
                                "color": "rgba(0,191,179,1)",
                                "id": str(uuid.uuid4()),
                            },
                            {
                                "filter": "name:bytes_written",
                                "label": "Written",
                                "color": "rgba(254,209,10,1)",
                                "id": str(uuid.uuid4()),
                            },
                        ],
                        "label": "Disk IO",
                        "value_template": "{{value}}",
                        "steps": 0,
                    }
                ],
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "time_field": "race-timestamp",
                "type": "timeseries",
                "filter": TimeSeriesCharts.filter_string(environment, race_config),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "rally-annotations",
                        "query_string": f'((NOT _exists_:track) OR track:"{race_config.track}") AND ((NOT _exists_:chart) OR chart:io) '
                        f'AND ((NOT _exists_:chart-name) OR chart-name:"{title}") AND environment:"{environment}"',
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "race-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1,
                    }
                ],
                "axis_min": "0",
            },
            "aggs": [],
            "listeners": {},
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "io",
                "version": 1,
                "kibanaSavedObjectMeta": {"searchSourceJSON": '{"query":"*","filter":[]}'},
            },
        }

    @staticmethod
    def disk_usage(title, environment, race_config):
        env_filter = TimeSeriesCharts.filter_string(environment, race_config)
        annotations_query = f'((NOT _exists_:track) OR track:"{race_config.track}") AND ((NOT _exists_:chart) OR chart:disk_usage)'
        vis_state = {
            "title": title,
            "type": "metrics",
            "params": {
                "axis_formatter": "number",
                "axis_position": "left",
                "id": str(uuid.uuid4()),
                "index_pattern": "rally-results-*",
                "interval": "1d",
                "series": [
                    {
                        "axis_position": "left",
                        "chart_type": "bar",
                        "color": "#68BC00",
                        "fill": "0.6",
                        "formatter": "bytes",
                        "id": str(uuid.uuid4()),
                        "line_width": "1",
                        "metrics": [{"id": str(uuid.uuid4()), "type": "sum", "field": "value.single"}],
                        "point_size": 1,
                        "seperate_axis": 1,
                        "split_mode": "terms",
                        "split_color_mode": "rainbow",
                        "stacked": "stacked",
                        "filter": "",
                        "terms_size": "1000",
                        "terms_order_by": "_key",
                        "terms_direction": "asc",
                        "terms_field": "field",
                        "label": "Disk Usage",
                        "value_template": "{{value}}",
                        "steps": 0,
                    }
                ],
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "time_field": "race-timestamp",
                "type": "timeseries",
                "filter": f"name:disk_usage_total {env_filter}",
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "rally-annotations",
                        "query_string": annotations_query,
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "race-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1,
                    }
                ],
                "axis_min": "0",
            },
            "aggs": [],
            "listeners": {},
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "per field disk usage",
                "version": 1,
                "kibanaSavedObjectMeta": {"searchSourceJSON": '{"query":"*","filter":[]}'},
            },
        }

    @staticmethod
    def query(environment, race_config, q, iterations):
        metric = "latency"
        title = TimeSeriesCharts.format_title(
            environment, race_config.track, es_license=race_config.es_license, suffix="%s-%s-%s" % (race_config.label, q, metric)
        )

        vis_state = {
            "title": title,
            "type": "metrics",
            "params": {
                "id": str(uuid.uuid4()),
                "type": "timeseries",
                "series": [
                    {
                        "id": str(uuid.uuid4()),
                        "color": color_scheme_rgba[0],
                        "split_mode": "everything",
                        "label": "50th percentile",
                        "metrics": [{"id": str(uuid.uuid4()), "type": "avg", "field": "value.50_0"}],
                        "seperate_axis": 0,
                        "axis_position": "right",
                        "formatter": "number",
                        "chart_type": "line",
                        "line_width": 1,
                        "point_size": 1,
                        "fill": "0.6",
                        "stacked": "none",
                        "split_color_mode": "gradient",
                        "series_drop_last_bucket": 0,
                        "value_template": "{{value}} ms",
                    },
                    {
                        "id": str(uuid.uuid4()),
                        "color": color_scheme_rgba[1],
                        "split_mode": "everything",
                        "label": "90th percentile",
                        "metrics": [{"id": str(uuid.uuid4()), "type": "avg", "field": "value.90_0"}],
                        "seperate_axis": 0,
                        "axis_position": "right",
                        "formatter": "number",
                        "chart_type": "line",
                        "line_width": 1,
                        "point_size": 1,
                        "fill": "0.4",
                        "stacked": "none",
                        "split_color_mode": "gradient",
                        "series_drop_last_bucket": 0,
                        "value_template": "{{value}} ms",
                    },
                    {
                        "id": str(uuid.uuid4()),
                        "color": color_scheme_rgba[2],
                        "split_mode": "everything",
                        "label": "99th percentile",
                        "metrics": [{"id": str(uuid.uuid4()), "type": "avg", "field": "value.99_0"}],
                        "seperate_axis": 0,
                        "axis_position": "right",
                        "formatter": "number",
                        "chart_type": "line",
                        "line_width": 1,
                        "point_size": 1,
                        "fill": "0.2",
                        "stacked": "none",
                        "split_color_mode": "gradient",
                        "series_drop_last_bucket": 0,
                        "value_template": "{{value}} ms",
                    },
                    {
                        "id": str(uuid.uuid4()),
                        "color": color_scheme_rgba[3],
                        "split_mode": "everything",
                        "label": "100th percentile",
                        "metrics": [{"id": str(uuid.uuid4()), "type": "avg", "field": "value.100_0"}],
                        "seperate_axis": 0,
                        "axis_position": "right",
                        "formatter": "number",
                        "chart_type": "line",
                        "line_width": 1,
                        "point_size": 1,
                        "fill": "0.1",
                        "stacked": "none",
                        "split_color_mode": "gradient",
                        "series_drop_last_bucket": 0,
                        "value_template": "{{value}} ms",
                    },
                ],
                "time_field": "race-timestamp",
                "index_pattern": "rally-results-*",
                "interval": "1d",
                "axis_position": "left",
                "axis_formatter": "number",
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "background_color_rules": [{"id": str(uuid.uuid4())}],
                "filter": 'task:"%s" AND name:"%s" AND %s' % (q, metric, TimeSeriesCharts.filter_string(environment, race_config)),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "rally-annotations",
                        "query_string": f'((NOT _exists_:track) OR track:"{race_config.track}") '
                        f"AND ((NOT _exists_:chart) OR chart:query) "
                        f'AND ((NOT _exists_:chart-name) OR chart-name:"{title}") AND environment:"{environment}"',
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "race-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1,
                    }
                ],
            },
            "aggs": [],
            "listeners": {},
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "query",
                "version": 1,
                "kibanaSavedObjectMeta": {"searchSourceJSON": '{"query":"*","filter":[]}'},
            },
        }

    @staticmethod
    def index(environment, race_configs, title):
        filters = []
        # any race_config will do - they all belong to the same track
        t = race_configs[0].track
        for idx, race_config in enumerate(race_configs):
            label = index_label(race_config)
            for bulk_task in race_config.bulk_tasks:
                filters.append(
                    {
                        "filter": 'task:"%s" AND %s' % (bulk_task, TimeSeriesCharts.filter_string(environment, race_config)),
                        "label": label,
                        "color": color_scheme_rgba[idx % len(color_scheme_rgba)],
                        "id": str(uuid.uuid4()),
                    }
                )

        vis_state = {
            "title": title,
            "type": "metrics",
            "params": {
                "axis_formatter": "number",
                "axis_position": "left",
                "id": str(uuid.uuid4()),
                "index_pattern": "rally-results-*",
                "interval": "1d",
                "series": [
                    {
                        "axis_position": "left",
                        "chart_type": "line",
                        "color": "#68BC00",
                        "fill": "0",
                        "formatter": "number",
                        "id": str(uuid.uuid4()),
                        "line_width": "1",
                        "metrics": [{"id": str(uuid.uuid4()), "type": "avg", "field": "value.median"}],
                        "point_size": "3",
                        "seperate_axis": 1,
                        "split_mode": "filters",
                        "stacked": "none",
                        "filter": 'environment:"%s" AND track:"%s"' % (environment, t),
                        "split_filters": filters,
                        "label": "Indexing Throughput",
                        "value_template": "{{value}} docs/s",
                        "steps": 0,
                    }
                ],
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "time_field": "race-timestamp",
                "type": "timeseries",
                "filter": 'environment:"%s" AND track:"%s" AND name:"throughput" AND active:true' % (environment, t),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "rally-annotations",
                        "query_string": f'((NOT _exists_:track) OR track:"{t}") '
                        f"AND ((NOT _exists_:chart) OR chart:indexing) "
                        f'AND ((NOT _exists_:chart-name) OR chart-name:"{title}") AND environment:"{environment}"',
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "race-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1,
                    }
                ],
                "axis_min": "0",
            },
            "aggs": [],
            "listeners": {},
        }
        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "index",
                "version": 1,
                "kibanaSavedObjectMeta": {"searchSourceJSON": '{"query":"*","filter":[]}'},
            },
        }

    @staticmethod
    def ingest(environment, race_config, title):
        track = race_config.track
        flavor = race_config.name
        vis_state = {
            "title": title,
            "type": "metrics",
            "params": {
                "axis_formatter": "number",
                "axis_position": "left",
                "id": str(uuid.uuid4()),
                "index_pattern": "rally-metrics-*",
                "interval": "1d",
                "series": [
                    {
                        "axis_position": "left",
                        "chart_type": "line",
                        "color": "#68BC00",
                        "fill": "0",
                        "formatter": "number",
                        "id": str(uuid.uuid4()),
                        "line_width": "1",
                        "metrics": [{"id": str(uuid.uuid4()), "type": "sum", "field": "value"}],
                        "point_size": "3",
                        "separate_axis": 0,
                        "split_color_mode": "rainbow",
                        "split_mode": "terms",
                        "terms_field": "meta.type",
                        "stacked": "none",
                        "filter": 'environment:"%s" AND track:"%s" AND meta.tag_name:"%s" AND name:"ingest_pipeline_processor_time"'
                        % (environment, track, flavor),
                        "label": "Ingest Processor Time (ms)",
                        "steps": 0,
                    }
                ],
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "time_field": "race-timestamp",
                "type": "timeseries",
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "rally-annotations",
                        "query_string": f'((NOT _exists_:track) OR track:"{track}") '
                        f"AND ((NOT _exists_:chart) OR chart:ingest) "
                        f'AND ((NOT _exists_:chart-name) OR chart-name:"{title}") AND environment:"{environment}"',
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "race-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1,
                    }
                ],
                "axis_min": "0",
            },
            "aggs": [],
            "listeners": {},
        }
        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "index",
                "version": 1,
                "kibanaSavedObjectMeta": {"searchSourceJSON": '{"query":"*","filter":[]}'},
            },
        }

    @staticmethod
    def revisions_table(title, environment, race_config):
        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(
                    {
                        "title": title,
                        "type": "table",
                        "params": {
                            "perPage": 15,
                            "sort": {"columnIndex": 0, "direction": "desc"},
                        },
                        "aggs": [
                            {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
                            {
                                "id": "2",
                                "enabled": True,
                                "type": "date_histogram",
                                "params": {
                                    "field": "race-timestamp",
                                    "interval": "d",
                                    "customLabel": "day",
                                },
                                "schema": "bucket",
                            },
                            {
                                "id": "3",
                                "enabled": True,
                                "type": "terms",
                                "params": {
                                    "field": "cluster.revision",
                                    "size": 100,
                                    "customLabel": "revision",
                                },
                                "schema": "bucket",
                            },
                            {
                                "id": "4",
                                "enabled": True,
                                "type": "terms",
                                "params": {
                                    "field": "cluster.distribution-version",
                                    "size": 100,
                                    "customLabel": "version",
                                },
                                "schema": "bucket",
                            },
                        ],
                        "listeners": {},
                    }
                ),
                "uiStateJSON": "{}",
                "description": "revisions",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps(
                        {
                            "index": "rally-races-*",
                            "query": {
                                "query_string": {
                                    "query": f'environment:"{environment}"'
                                    f' AND track:"{race_config.track}"'
                                    f' AND challenge:"{race_config.challenge}"'
                                    f' AND car:"{race_config.car}"',
                                    "analyze_wildcard": True,
                                }
                            },
                            "filter": [],
                        }
                    )
                },
            },
        }


class RaceConfigTrack:
    def __init__(self, cfg, repository, name=None):
        self.repository = repository
        self.cached_track = self.load_track(cfg, name=name)

    def load_track(self, cfg, name=None, params=None, excluded_tasks=None):
        if not params:
            params = {}
        # required in case a previous track using a different repository has specified the revision
        if cfg.opts("track", "repository.name", mandatory=False) != self.repository:
            cfg.add(config.Scope.applicationOverride, "track", "repository.revision", None)
        # hack to make this work with multiple tracks (Rally core is usually not meant to be used this way)
        if name:
            cfg.add(config.Scope.applicationOverride, "track", "repository.name", self.repository)
            cfg.add(config.Scope.applicationOverride, "track", "track.name", name)
        # another hack to ensure any track-params in the race config are used by Rally's track loader
        cfg.add(config.Scope.applicationOverride, "track", "params", params)
        if excluded_tasks:
            cfg.add(config.Scope.application, "track", "exclude.tasks", excluded_tasks)
        return track.load_track(cfg)

    def get_track(self, cfg, name=None, params=None, excluded_tasks=None):
        if params or excluded_tasks:
            return self.load_track(cfg, name, params, excluded_tasks)
        # if no params specified, return the initially cached, (non-parametrized) track
        return self.cached_track


def generate_index_ops(race_configs, environment, logger):
    idx_race_configs = list(filter(lambda c: "indexing" in c.charts, race_configs))
    for race_conf in idx_race_configs:
        logger.debug(
            "Gen index visualization for race config with name:[%s] / label:[%s] / flavor: [%s] / license: [%s]",
            race_conf.name,
            race_conf.label,
            race_conf.flavor,
            race_conf.es_license,
        )
    charts = []

    if idx_race_configs:
        title = TimeSeriesCharts.format_title(
            environment, race_configs[0].track, flavor=race_configs[0].flavor, suffix="indexing-throughput"
        )
        charts = [TimeSeriesCharts.index(environment, idx_race_configs, title)]
    return charts


def generate_ingest(race_configs, environment):
    structures = []

    for race_config in race_configs:
        if "ingest" in race_config.charts:
            title = f"{race_config.name}-ingest-time"
            structures.append(TimeSeriesCharts.ingest(environment, race_config, title))
    return structures


def generate_queries(race_configs, environment):
    # output JSON structures
    structures = []

    for race_config in race_configs:
        if "query" in race_config.charts:
            for q in race_config.throttled_tasks:
                structures.append(TimeSeriesCharts.query(environment, race_config, q.name, q.params.get("iterations", 100)))
    return structures


def generate_io(race_configs, environment):
    # output JSON structures
    structures = []
    for race_config in race_configs:
        if "io" in race_config.charts:
            title = TimeSeriesCharts.format_title(
                environment, race_config.track, es_license=race_config.es_license, suffix="%s-io" % race_config.label
            )
            structures.append(TimeSeriesCharts.io(title, environment, race_config))

    return structures


def generate_disk_usage(race_configs, environment):
    # output JSON structures
    structures = []
    for race_config in race_configs:
        if "disk_usage" in race_config.charts:
            title = TimeSeriesCharts.format_title(
                environment, race_config.track, es_license=race_config.es_license, suffix="%s-disk-usage" % race_config.label
            )
            structures.append(TimeSeriesCharts.disk_usage(title, environment, race_config))

    return structures


def generate_gc(race_configs, environment):
    structures = []
    for race_config in race_configs:
        if "gc" in race_config.charts:
            title = TimeSeriesCharts.format_title(
                environment, race_config.track, es_license=race_config.es_license, suffix="%s-gc" % race_config.label
            )
            structures.append(TimeSeriesCharts.gc(title, environment, race_config))

    return structures


def generate_merge_time(race_configs, environment):
    structures = []
    for race_config in race_configs:
        if "merge_times" in race_config.charts:
            title = TimeSeriesCharts.format_title(
                environment, race_config.track, es_license=race_config.es_license, suffix=f"{race_config.label}-merge-times"
            )
            chart = TimeSeriesCharts.merge_time(title, environment, race_config)
            if chart is not None:
                structures.append(chart)

    return structures


def generate_ml_processing_time(race_configs, environment):
    structures = []
    for race_config in race_configs:
        if "ml_processing_time" in race_config.charts:
            title = TimeSeriesCharts.format_title(
                environment, race_config.track, es_license=race_config.es_license, suffix=f"{race_config.label}-ml-processing-time"
            )
            chart = TimeSeriesCharts.ml_processing_time(title, environment, race_config)
            if chart is not None:
                structures.append(chart)

    return structures


def generate_merge_count(race_configs, environment):
    structures = []
    for race_config in race_configs:
        if "merge_count" in race_config.charts:
            title = TimeSeriesCharts.format_title(
                environment, race_config.track, es_license=race_config.es_license, suffix=f"{race_config.label}-merge-count"
            )
            chart = TimeSeriesCharts.merge_count(title, environment, race_config)
            if chart is not None:
                structures.append(chart)

    return structures


def generate_revisions(race_configs, environment):
    structures = []
    for race_config in race_configs:
        title = TimeSeriesCharts.format_title(
            environment, race_config.track, es_license=race_config.es_license, suffix=f"{race_config.label}-revisions"
        )
        chart = TimeSeriesCharts.revisions_table(title, environment, race_config)
        if chart is not None:
            structures.append(chart)

    return structures


def generate_dashboard(environment, track, charts, flavor=None):
    panels = []

    width = 24
    height = 32

    row = 0
    col = 0

    for idx, chart in enumerate(charts):
        panelIndex = idx + 1
        # make index charts wider
        if chart["attributes"]["description"] == "index":
            chart_width = 2 * width
            # force one panel per row
            next_col = 0
        else:
            chart_width = width
            # two rows per panel
            next_col = (col + 1) % 2

        panel = {
            "id": chart["id"],
            "panelIndex": panelIndex,
            "gridData": {"x": (col * chart_width), "y": (row * height), "w": chart_width, "h": height, "i": str(panelIndex)},
            "type": "visualization",
            "version": "7.10.2",
        }
        panels.append(panel)
        col = next_col
        if col == 0:
            row += 1

    return {
        "id": str(uuid.uuid4()),
        "type": "dashboard",
        "attributes": {
            "title": TimeSeriesCharts.format_title(environment, track.name, flavor=flavor),
            "hits": 0,
            "description": "",
            "panelsJSON": json.dumps(panels),
            "optionsJSON": '{"darkTheme":false}',
            "uiStateJSON": "{}",
            "version": 1,
            "timeRestore": False,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(
                    {
                        "filter": [{"query": {"query_string": {"analyze_wildcard": True, "query": "*"}}}],
                        "highlightAll": True,
                        "version": True,
                    }
                )
            },
        },
    }


class RaceConfig:
    def __init__(self, track, cfg=None, flavor=None, es_license=None, challenge=None, car=None, node_count=None, charts=None):
        self.track = track
        if cfg:
            self.configuration = cfg
            self.configuration["flavor"] = flavor
            self.configuration["es_license"] = es_license
        else:
            self.configuration = {"charts": charts, "challenge": challenge, "car": car, "node-count": node_count}

    @property
    def name(self):
        return self.configuration.get("name")

    @property
    def flavor(self):
        return self.configuration.get("flavor")

    @property
    def es_license(self):
        return self.configuration.get("es_license")

    @property
    def label(self):
        return self.configuration.get("label")

    @property
    def charts(self):
        return self.configuration["charts"]

    @property
    def node_count(self):
        return self.configuration.get("node-count", 1)

    @property
    def challenge(self):
        return self.configuration["challenge"]

    @property
    def car(self):
        return self.configuration["car"]

    @property
    def plugins(self):
        return self.configuration.get("plugins", "")

    @property
    def bulk_tasks(self):
        task_names = []
        for task in self.track.find_challenge_or_default(self.challenge).schedule:
            for sub_task in task:
                # We are looking for type bulk operations to add to indexing throughput chart.
                # For the observability track, the index operation is of type raw-bulk, instead of type bulk.
                # Doing a lenient match to allow for that.
                if track.OperationType.Bulk.to_hyphenated_string() in sub_task.operation.type:
                    if track.OperationType.Bulk.to_hyphenated_string() != sub_task.operation.type:
                        console.info(
                            f"Found [{sub_task.name}] of type [{sub_task.operation.type}] in "
                            f"[{self.challenge}], adding it to indexing dashboard.\n",
                            flush=True,
                        )
                    task_names.append(sub_task.name)
        return task_names

    @property
    def throttled_tasks(self):
        task_names = []
        for task in self.track.find_challenge_or_default(self.challenge).schedule:
            for sub_task in task:
                # We are assuming here that each task with a target throughput or target interval is interesting for latency charts.
                #
                # As a temporary workaround we're also treating operations of type "eql" as throttled tasks (requiring a latency
                # or service time chart) although they are (at the moment) not throttled. These tasks originate from the EQL track
                # available at https://github.com/elastic/rally-tracks/tree/master/eql.
                #
                # We should refactor the chart generator to make this classification logic more flexible so the user can specify
                # which tasks / or types of operations should be used.
                if (
                    sub_task.operation.type
                    in ["search", "composite", "eql", "paginated-search", "scroll-search", "raw-request", "composite-agg"]
                    or "target-throughput" in sub_task.params
                    or "target-interval" in sub_task.params
                ):
                    task_names.append(sub_task)
        return task_names


def load_race_configs(cfg, chart_spec_path=None):
    def add_configs(race_configs_per_lic, flavor_name="oss", lic="oss", track_name=None):
        configs_per_lic = []
        for race_config in race_configs_per_lic:
            excluded_tasks = None
            if "exclude-tasks" in race_config:
                excluded_tasks = race_config.get("exclude-tasks").split(",")
            configs_per_lic.append(
                RaceConfig(
                    track=race_config_track.get_track(
                        cfg, name=track_name, params=race_config.get("track-params", {}), excluded_tasks=excluded_tasks
                    ),
                    cfg=race_config,
                    flavor=flavor_name,
                    es_license=lic,
                )
            )
        return configs_per_lic

    def add_race_configs(license_configs, flavor_name, track_name):
        for lic_config in license_configs:
            race_configs_per_track.extend(add_configs(lic_config["configurations"], flavor_name, lic_config["name"], track_name))

    race_configs = {"oss": [], "default": []}
    chart_specs = glob.glob(io.normalize_path(chart_spec_path))
    if not chart_specs:
        raise exceptions.NotFound(f"Chart spec path [{chart_spec_path}] not found.")
    for _track_file in chart_specs:
        with open(_track_file, mode="rt", encoding="utf-8") as f:
            for item in json.load(f):
                _track_repository = item.get("track-repository", "default")
                race_config_track = RaceConfigTrack(cfg, _track_repository, name=item["track"])
                for flavor in item["flavors"]:
                    race_configs_per_track = []
                    _flavor_name = flavor["name"]
                    _track_name = item["track"]
                    add_race_configs(flavor["licenses"], _flavor_name, _track_name)

                    if race_configs_per_track:
                        race_configs[_flavor_name].append(race_configs_per_track)
    return race_configs


def gen_charts_per_track_configs(race_configs, env, flavor=None, logger=None):
    charts = (
        generate_index_ops(race_configs, env, logger)
        + generate_ingest(race_configs, env)
        + generate_io(race_configs, env)
        + generate_disk_usage(race_configs, env)
        + generate_gc(race_configs, env)
        + generate_merge_time(race_configs, env)
        + generate_merge_count(race_configs, env)
        + generate_ml_processing_time(race_configs, env)
        + generate_queries(race_configs, env)
        + generate_revisions(race_configs, env)
    )

    dashboard = generate_dashboard(env, race_configs[0].track, charts, flavor)

    return charts, dashboard


def gen_charts_from_track_combinations(race_configs, env, logger):
    structures = []
    for flavor, race_configs_per_flavor in race_configs.items():
        for race_configs_per_track in race_configs_per_flavor:
            logger.debug("Generating charts for race_configs with name:[%s]/flavor:[%s]", race_configs_per_track[0].name, flavor)
            charts, dashboard = gen_charts_per_track_configs(race_configs_per_track, env, flavor, logger)

            structures.extend(charts)
            structures.append(dashboard)

    return structures


def generate(cfg):
    logger = logging.getLogger(__name__)

    chart_spec_path = cfg.opts("generator", "chart.spec.path")

    console.info("Loading track data...", flush=True)
    race_configs = load_race_configs(cfg, chart_spec_path)
    env = cfg.opts("system", "env.name")

    console.info("Generating charts...", flush=True)
    structures = gen_charts_from_track_combinations(race_configs, env, logger)

    output_path = cfg.opts("generator", "output.path")
    if output_path:
        with open(io.normalize_path(output_path), mode="wt", encoding="utf-8") as f:
            for record in structures:
                print(json.dumps(record), file=f)
    else:
        for record in structures:
            print(json.dumps(record))
