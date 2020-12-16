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

import glob
import json
import logging
import uuid

from esrally import track, config, exceptions
from esrally.utils import io, console

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


class BarCharts:
    UI_STATE_JSON = json.dumps(
        {
            "vis": {
                "colors": dict(
                    zip(["bare-oss", "bare-basic", "bare-trial-security", "docker-oss", "ear-oss"], color_scheme_rgba))
            }
        })

    @staticmethod
    # flavor's unused but we need the same signature used by the corresponding method in TimeSeriesCharts
    def format_title(environment, track_name, flavor=None, es_license=None, suffix=None):
        title = "{}-{}".format(environment, track_name)

        if suffix:
            title += "-{}".format(suffix)

        return title

    @staticmethod
    def filter_string(environment, race_config):
        if race_config.name:
            return 'environment:"{}" AND active:true AND user-tags.name:"{}"'.format(
                environment,
                race_config.name)
        else:
            return 'environment:"{}" AND active:true AND track:"{}" AND challenge:"{}" AND car:"{}" AND node-count:{}'.format(
                environment,
                race_config.track,
                race_config.challenge,
                race_config.car,
                race_config.node_count)

    @staticmethod
    def gc(title, environment, race_config):
        vis_state = {
            "title": title,
            "type": "histogram",
            "params": {
                "addLegend": True,
                "addTimeMarker": False,
                "addTooltip": True,
                "categoryAxes": [
                    {
                        "id": "CategoryAxis-1",
                        "labels": {
                            "show": True,
                            "truncate": 100
                        },
                        "position": "bottom",
                        "scale": {
                            "type": "linear"
                        },
                        "show": True,
                        "style": {},
                        "title": {
                            "text": "filters"
                        },
                        "type": "category"
                    }
                ],
                "defaultYExtents": False,
                "drawLinesBetweenPoints": True,
                "grid": {
                    "categoryLines": False,
                    "style": {
                        "color": "#eee"
                    }
                },
                "interpolate": "linear",
                "legendPosition": "right",
                "radiusRatio": 9,
                "scale": "linear",
                "seriesParams": [
                    {
                        "data": {
                            "id": "1",
                            "label": "Total GC Duration [ms]"
                        },
                        "drawLinesBetweenPoints": True,
                        "mode": "normal",
                        "show": "True",
                        "showCircles": True,
                        "type": "histogram",
                        "valueAxis": "ValueAxis-1"
                    }
                ],
                "setYExtents": False,
                "showCircles": True,
                "times": [],
                "valueAxes": [
                    {
                        "id": "ValueAxis-1",
                        "labels": {
                            "filter": False,
                            "rotate": 0,
                            "show": True,
                            "truncate": 100
                        },
                        "name": "LeftAxis-1",
                        "position": "left",
                        "scale": {
                            "mode": "normal",
                            "type": "linear"
                        },
                        "show": True,
                        "style": {},
                        "title": {
                            "text": "Total GC Duration [ms]"
                        },
                        "type": "value"
                    }
                ]
            },
            "aggs": [
                {
                    "id": "1",
                    "enabled": True,
                    "type": "median",
                    "schema": "metric",
                    "params": {
                        "field": "value.single",
                        "percents": [
                            50
                        ],
                        "customLabel": "Total GC Duration [ms]"
                    }
                },
                {
                    "id": "2",
                    "enabled": True,
                    "type": "filters",
                    "schema": "segment",
                    "params": {
                        "filters": [
                            {
                                "input": {
                                    "query": {
                                        "query_string": {
                                            "query": "name:young_gc_time",
                                            "analyze_wildcard": True
                                        }
                                    }
                                },
                                "label": "Young GC"
                            },
                            {
                                "input": {
                                    "query": {
                                        "query_string": {
                                            "query": "name:old_gc_time",
                                            "analyze_wildcard": True
                                        }
                                    }
                                },
                                "label": "Old GC"
                            }
                        ]
                    }
                },
                {
                    "id": "3",
                    "enabled": True,
                    "type": "terms",
                    "schema": "split",
                    "params": {
                        "field": "distribution-version",
                        "size": 10,
                        "order": "asc",
                        "orderBy": "_term",
                        "row": False
                    }
                },
                {
                    "id": "4",
                    "enabled": True,
                    "type": "terms",
                    "schema": "group",
                    "params": {
                        "field": "user-tags.setup",
                        "size": 5,
                        "order": "desc",
                        "orderBy": "_term"
                    }
                }
            ],
            "listeners": {}
        }

        search_source = {
            "index": "rally-results-*",
            "query": {
                "query_string": {
                    "query": BarCharts.filter_string(environment, race_config),
                    "analyze_wildcard": True
                }
            },
            "filter": []
        }

        return {
            "_id": str(uuid.uuid4()),
            "_type": "visualization",
            "_source": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": BarCharts.UI_STATE_JSON,
                "description": "gc",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps(search_source)
                }
            }
        }

    @staticmethod
    def io(title, environment, race_config):
        vis_state = {
            "title": title,
            "type": "histogram",
            "params": {
                "addLegend": True,
                "addTimeMarker": False,
                "addTooltip": True,
                "categoryAxes": [
                    {
                        "id": "CategoryAxis-1",
                        "labels": {
                            "show": True,
                            "truncate": 100
                        },
                        "position": "bottom",
                        "scale": {
                            "type": "linear"
                        },
                        "show": True,
                        "style": {},
                        "title": {
                            "text": "filters"
                        },
                        "type": "category"
                    }
                ],
                "defaultYExtents": False,
                "drawLinesBetweenPoints": True,
                "grid": {
                    "categoryLines": False,
                    "style": {
                        "color": "#eee"
                    }
                },
                "interpolate": "linear",
                "legendPosition": "right",
                "radiusRatio": 9,
                "scale": "linear",
                "seriesParams": [
                    {
                        "data": {
                            "id": "1",
                            "label": "[Bytes]"
                        },
                        "drawLinesBetweenPoints": True,
                        "mode": "normal",
                        "show": "True",
                        "showCircles": True,
                        "type": "histogram",
                        "valueAxis": "ValueAxis-1"
                    }
                ],
                "setYExtents": False,
                "showCircles": True,
                "times": [],
                "valueAxes": [
                    {
                        "id": "ValueAxis-1",
                        "labels": {
                            "filter": False,
                            "rotate": 0,
                            "show": True,
                            "truncate": 100
                        },
                        "name": "LeftAxis-1",
                        "position": "left",
                        "scale": {
                            "mode": "normal",
                            "type": "linear"
                        },
                        "show": True,
                        "style": {},
                        "title": {
                            "text": "[Bytes]"
                        },
                        "type": "value"
                    }
                ]
            },
            "aggs": [
                {
                    "id": "1",
                    "enabled": True,
                    "type": "sum",
                    "schema": "metric",
                    "params": {
                        "field": "value.single",
                        "customLabel": "[Bytes]"
                    }
                },
                {
                    "id": "2",
                    "enabled": True,
                    "type": "filters",
                    "schema": "segment",
                    "params": {
                        "filters": [
                            {
                                "input": {
                                    "query": {
                                        "query_string": {
                                            "analyze_wildcard": True,
                                            "query": "name:index_size"
                                        }
                                    }
                                },
                                "label": "Index size"
                            },
                            {
                                "input": {
                                    "query": {
                                        "query_string": {
                                            "analyze_wildcard": True,
                                            "query": "name:bytes_written"
                                        }
                                    }
                                },
                                "label": "Bytes written"
                            }
                        ]
                    }
                },
                {
                    "id": "3",
                    "enabled": True,
                    "type": "terms",
                    "schema": "split",
                    "params": {
                        "field": "distribution-version",
                        "size": 10,
                        "order": "asc",
                        "orderBy": "_term",
                        "row": False
                    }
                },
                {
                    "id": "4",
                    "enabled": True,
                    "type": "terms",
                    "schema": "group",
                    "params": {
                        "field": "user-tags.setup",
                        "size": 5,
                        "order": "desc",
                        "orderBy": "_term"
                    }
                }
            ],
            "listeners": {}
        }

        search_source = {
            "index": "rally-results-*",
            "query": {
                "query_string": {
                    "query": BarCharts.filter_string(environment, race_config),
                    "analyze_wildcard": True
                }
            },
            "filter": []
        }

        return {
            "_id": str(uuid.uuid4()),
            "_type": "visualization",
            "_source": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": BarCharts.UI_STATE_JSON,
                "description": "io",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps(search_source)
                }
            }
        }

    @staticmethod
    def segment_memory(title, environment, race_config):
        # don't generate segment memory charts for releases
        return None

    @staticmethod
    def query(environment, race_config, q):
        metric = "service_time"
        title = BarCharts.format_title(environment, race_config.track, suffix="%s-%s-p99-%s" % (race_config.label, q, metric))
        label = "Query Service Time [ms]"

        vis_state = {
            "title": title,
            "type": "histogram",
            "params": {
                "addLegend": True,
                "addTimeMarker": False,
                "addTooltip": True,
                "categoryAxes": [
                    {
                        "id": "CategoryAxis-1",
                        "labels": {
                            "show": True,
                            "truncate": 100
                        },
                        "position": "bottom",
                        "scale": {
                            "type": "linear"
                        },
                        "show": True,
                        "style": {},
                        "title": {
                            "text": "distribution-version: Ascending"
                        },
                        "type": "category"
                    }
                ],
                "defaultYExtents": False,
                "drawLinesBetweenPoints": True,
                "grid": {
                    "categoryLines": False,
                    "style": {
                        "color": "#eee"
                    }
                },
                "interpolate": "linear",
                "legendPosition": "right",
                "radiusRatio": 9,
                "scale": "linear",
                "seriesParams": [
                    {
                        "data": {
                            "id": "1",
                            "label": label
                        },
                        "drawLinesBetweenPoints": True,
                        "mode": "normal",
                        "show": "True",
                        "showCircles": True,
                        "type": "histogram",
                        "valueAxis": "ValueAxis-1"
                    }
                ],
                "setYExtents": False,
                "showCircles": True,
                "times": [],
                "valueAxes": [
                    {
                        "id": "ValueAxis-1",
                        "labels": {
                            "filter": False,
                            "rotate": 0,
                            "show": True,
                            "truncate": 100
                        },
                        "name": "LeftAxis-1",
                        "position": "left",
                        "scale": {
                            "mode": "normal",
                            "type": "linear"
                        },
                        "show": True,
                        "style": {},
                        "title": {
                            "text": label
                        },
                        "type": "value"
                    }
                ]
            },
            "aggs": [
                {
                    "id": "1",
                    "enabled": True,
                    "type": "median",
                    "schema": "metric",
                    "params": {
                        "field": "value.99_0",
                        "percents": [
                            50
                        ],
                        "customLabel": label
                    }
                },
                {
                    "id": "2",
                    "enabled": True,
                    "type": "terms",
                    "schema": "segment",
                    "params": {
                        "field": "distribution-version",
                        "size": 10,
                        "order": "asc",
                        "orderBy": "_term"
                    }
                },
                {
                    "id": "3",
                    "enabled": True,
                    "type": "terms",
                    "schema": "group",
                    "params": {
                        "field": "user-tags.setup",
                        "size": 10,
                        "order": "desc",
                        "orderBy": "_term"
                    }
                }
            ],
            "listeners": {}
        }

        search_source = {
            "index": "rally-results-*",
            "query": {
                "query_string": {
                    "query": "name:\"%s\" AND task:\"%s\" AND %s" % (metric, q, BarCharts.filter_string(environment, race_config)),
                    "analyze_wildcard": True
                }
            },
            "filter": []
        }

        return {
            "_id": str(uuid.uuid4()),
            "_type": "visualization",
            "_source": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": BarCharts.UI_STATE_JSON,
                "description": "query",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps(search_source)
                }
            }
        }

    @staticmethod
    def index(environment, race_configs, title):
        filters = []
        for race_config in race_configs:
            label = index_label(race_config)
            # the assumption is that we only have one bulk task
            for bulk_task in race_config.bulk_tasks:
                filters.append({
                    "input": {
                        "query": {
                            "query_string": {
                                "analyze_wildcard": True,
                                "query": "task:\"%s\" AND %s" % (bulk_task, BarCharts.filter_string(environment, race_config))
                            }
                        }
                    },
                    "label": label
                })

        vis_state = {
            "aggs": [
                {
                    "enabled": True,
                    "id": "1",
                    "params": {
                        "customLabel": "Median Indexing Throughput [docs/s]",
                        "field": "value.median",
                        "percents": [
                            50
                        ]
                    },
                    "schema": "metric",
                    "type": "median"
                },
                {
                    "enabled": True,
                    "id": "2",
                    "params": {
                        "field": "distribution-version",
                        "order": "asc",
                        "orderBy": "_term",
                        "size": 10
                    },
                    "schema": "segment",
                    "type": "terms"
                },
                {
                    "enabled": True,
                    "id": "3",
                    "params": {
                        "field": "user-tags.setup",
                        "order": "desc",
                        "orderBy": "_term",
                        "size": 10
                    },
                    "schema": "group",
                    "type": "terms"
                },
                {
                    "enabled": True,
                    "id": "4",
                    "params": {
                        "filters": filters,
                        "row": True
                    },
                    "schema": "split",
                    "type": "filters"
                }
            ],
            "listeners": {},
            "params": {
                "addLegend": True,
                "addTimeMarker": False,
                "addTooltip": True,
                "categoryAxes": [
                    {
                        "id": "CategoryAxis-1",
                        "labels": {
                            "show": True,
                            "truncate": 100
                        },
                        "position": "bottom",
                        "scale": {
                            "type": "linear"
                        },
                        "show": True,
                        "style": {},
                        "title": {
                            "text": "distribution-version: Ascending"
                        },
                        "type": "category"
                    }
                ],
                "defaultYExtents": False,
                "drawLinesBetweenPoints": True,
                "grid": {
                    "categoryLines": False,
                    "style": {
                        "color": "#eee"
                    }
                },
                "interpolate": "linear",
                "legendPosition": "right",
                "radiusRatio": 9,
                "scale": "linear",
                "seriesParams": [
                    {
                        "data": {
                            "id": "1",
                            "label": "Median Indexing Throughput [docs/s]"
                        },
                        "drawLinesBetweenPoints": True,
                        "mode": "normal",
                        "show": "True",
                        "showCircles": True,
                        "type": "histogram",
                        "valueAxis": "ValueAxis-1"
                    }
                ],
                "setYExtents": False,
                "showCircles": True,
                "times": [],
                "valueAxes": [
                    {
                        "id": "ValueAxis-1",
                        "labels": {
                            "filter": False,
                            "rotate": 0,
                            "show": True,
                            "truncate": 100
                        },
                        "name": "LeftAxis-1",
                        "position": "left",
                        "scale": {
                            "mode": "normal",
                            "type": "linear"
                        },
                        "show": True,
                        "style": {},
                        "title": {
                            "text": "Median Indexing Throughput [docs/s]"
                        },
                        "type": "value"
                    }
                ]
            },
            "title": title,
            "type": "histogram"
        }

        search_source = {
            "index": "rally-results-*",
            "query": {
                "query_string": {
                    "analyze_wildcard": True,
                    "query": "environment:\"%s\" AND active:true AND name:\"throughput\"" % environment
                }
            },
            "filter": []
        }

        return {
            "_id": str(uuid.uuid4()),
            "_type": "visualization",
            "_source": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": BarCharts.UI_STATE_JSON,
                "description": "index",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps(search_source)
                }
            }
        }


class TimeSeriesCharts:
    @staticmethod
    def format_title(environment, track_name, flavor=None, es_license=None, suffix=None):
        if flavor:
            title = [environment, flavor, str(track_name)]
        elif es_license:
            title = [environment, es_license, str(track_name)]
        elif flavor and es_license:
            raise exceptions.RallyAssertionError(
                "Specify either flavor [{}] or license [{}] but not both".format(flavor, es_license))
        else:
            title = [environment, str(track_name)]
        if suffix:
            title.append(suffix)

        return "-".join(title)

    @staticmethod
    def filter_string(environment, race_config):
        nightly_extra_filter = ""
        if race_config.es_license:
            # Time series charts need to support different licenses and produce customized titles.
            nightly_extra_filter = ' AND user-tags.license:"{}"'.format(race_config.es_license)
        if race_config.name:
            return 'environment:"{}" AND active:true AND user-tags.name:"{}"{}'.format(
                environment,
                race_config.name,
                nightly_extra_filter)
        else:
            return 'environment:"{}" AND active:true AND track:"{}" AND challenge:"{}" AND car:"{}" AND node-count:{}'.format(
                environment,
                race_config.track,
                race_config.challenge,
                race_config.car,
                race_config.node_count)

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
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "avg",
                                "field": "value.single"
                            }
                        ],
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
                                "id": str(uuid.uuid4())
                            },
                            {
                                "filter": "old_gc_time",
                                "label": "Old Gen GC time",
                                "color": "rgba(254,209,10,1)",
                                "id": str(uuid.uuid4())
                            }
                        ],
                        "label": "GC Times",
                        "value_template": "{{value}} ms",
                        "steps": 0,
                        "axis_min": "0"
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
                        "query_string": "((NOT _exists_:track) OR track:\"%s\") AND ((NOT _exists_:chart) OR chart:gc) "
                                        "AND environment:\"%s\"" % (race_config.track, environment),
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "race-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1
                    }
                ]
            },
            "aggs": [],
            "listeners": {}
        }

        return {
            "_id": str(uuid.uuid4()),
            "_type": "visualization",
            "_source": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "gc",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"query\":{\"query_string\":{\"query\":\"*\"}},\"filter\":[]}"
                }
            }
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
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "sum",
                                "field": "value.single"
                            }
                        ],
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
                                "id": str(uuid.uuid4())
                            },
                            {
                                "filter": "name:bytes_written",
                                "label": "Written",
                                "color": "rgba(254,209,10,1)",
                                "id": str(uuid.uuid4())
                            }
                        ],
                        "label": "Disk IO",
                        "value_template": "{{value}}",
                        "steps": 0,
                        "axis_min": "0"
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
                        "query_string": "((NOT _exists_:track) OR track:\"%s\") AND ((NOT _exists_:chart) OR chart:io) "
                                        "AND environment:\"%s\"" % (race_config.track, environment),
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "race-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1
                    }
                ]
            },
            "aggs": [],
            "listeners": {}
        }

        return {
            "_id": str(uuid.uuid4()),
            "_type": "visualization",
            "_source": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "io",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"query\":{\"query_string\":{\"query\":\"*\"}},\"filter\":[]}"
                }
            }
        }

    @staticmethod
    def segment_memory(title, environment, race_config):
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
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "avg",
                                "field": "value.single"
                            }
                        ],
                        "point_size": "3",
                        "seperate_axis": 1,
                        "split_mode": "filters",
                        "stacked": "none",
                        "filter": "environment:{} AND track:{}".format(environment, race_config.track),
                        "split_filters": [
                            {
                                "filter": "memory_segments",
                                "label": "Segments",
                                "color": color_scheme_rgba[0],
                                "id": str(uuid.uuid4())
                            },
                            {
                                "filter": "memory_doc_values",
                                "label": "Doc Values",
                                "color": color_scheme_rgba[1],
                                "id": str(uuid.uuid4())
                            },
                            {
                                "filter": "memory_terms",
                                "label": "Terms",
                                "color": color_scheme_rgba[2],
                                "id": str(uuid.uuid4())
                            },
                            {
                                "filter": "memory_norms",
                                "label": "Norms",
                                "color": color_scheme_rgba[3],
                                "id": str(uuid.uuid4())
                            },
                            {
                                "filter": "memory_points",
                                "label": "Points",
                                "color": color_scheme_rgba[4],
                                "id": str(uuid.uuid4())
                            },
                            {
                                "filter": "memory_stored_fields",
                                "label": "Stored Fields",
                                "color": color_scheme_rgba[5],
                                "id": str(uuid.uuid4())
                            }
                        ],
                        "label": "Segment Memory",
                        "value_template": "{{value}}",
                        "steps": 0,
                        "axis_min": "0"
                    }
                ],
                "show_legend": 1,
                "time_field": "race-timestamp",
                "type": "timeseries",
                "filter": TimeSeriesCharts.filter_string(environment, race_config),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "rally-annotations",
                        "query_string": "((NOT _exists_:track) OR track:\"%s\") AND ((NOT _exists_:chart) OR chart:segment_memory) "
                                        "AND environment:\"%s\"" % (race_config.track, environment),
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "race-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1
                    }
                ],
                "show_grid": 1,
                "drop_last_bucket": 0
            },
            "aggs": []
        }

        return {
            "_id": str(uuid.uuid4()),
            "_type": "visualization",
            "_source": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "segment_memory",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"query\":{\"query\":{\"query_string\":{\"query\":\"*\"}},\"language\":\"lucene\"},\"filter\":[]}"
                }
            }
        }

    @staticmethod
    def query(environment, race_config, q):
        metric = "latency"
        title = TimeSeriesCharts.format_title(environment, race_config.track, es_license=race_config.es_license,
                                              suffix="%s-%s-%s" % (race_config.label, q, metric))

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
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "avg",
                                "field": "value.50_0"
                            }
                        ],
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
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "avg",
                                "field": "value.90_0"
                            }
                        ],
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
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "avg",
                                "field": "value.99_0"
                            }
                        ],
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
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "avg",
                                "field": "value.100_0"
                            }
                        ],
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
                    }
                ],
                "time_field": "race-timestamp",
                "index_pattern": "rally-results-*",
                "interval": "1d",
                "axis_position": "left",
                "axis_formatter": "number",
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "background_color_rules": [
                    {
                        "id": str(uuid.uuid4())
                    }
                ],
                "filter": "task:\"%s\" AND name:\"%s\" AND %s" % (q, metric, TimeSeriesCharts.filter_string(
                    environment, race_config)),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "rally-annotations",
                        "query_string": "((NOT _exists_:track) OR track:\"%s\") AND ((NOT _exists_:chart) OR chart:query) "
                                        "AND environment:\"%s\"" % (race_config.track, environment),
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "race-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1
                    }
                ]
            },
            "aggs": [],
            "listeners": {}
        }

        return {
            "_id": str(uuid.uuid4()),
            "_type": "visualization",
            "_source": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "query",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"query\":{\"query_string\":{\"query\":\"*\"}},\"filter\":[]}"
                }
            }
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
                        "filter": "task:\"%s\" AND %s" % (bulk_task, TimeSeriesCharts.filter_string(environment, race_config)),
                        "label": label,
                        "color": color_scheme_rgba[idx % len(color_scheme_rgba)],
                        "id": str(uuid.uuid4())
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
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "avg",
                                "field": "value.median"
                            }
                        ],
                        "point_size": "3",
                        "seperate_axis": 1,
                        "split_mode": "filters",
                        "stacked": "none",
                        "filter": "environment:\"%s\" AND track:\"%s\"" % (environment, t),
                        "split_filters": filters,
                        "label": "Indexing Throughput",
                        "value_template": "{{value}} docs/s",
                        "steps": 0,
                        "axis_min": "0"
                    }
                ],
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "time_field": "race-timestamp",
                "type": "timeseries",
                "filter": "environment:\"%s\" AND track:\"%s\" AND name:\"throughput\" AND active:true" % (environment, t),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "rally-annotations",
                        "query_string": "((NOT _exists_:track) OR track:\"%s\") AND ((NOT _exists_:chart) OR chart:indexing) "
                                        "AND environment:\"%s\"" % (t, environment),
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "race-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1
                    }
                ]
            },
            "aggs": [],
            "listeners": {}
        }
        return {
            "_id": str(uuid.uuid4()),
            "_type": "visualization",
            "_source": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "index",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"query\":{\"query_string\":{\"query\":\"*\"}},\"filter\":[]}"
                }
            }
        }


class RaceConfigTrack:
    def __init__(self, cfg, repository, name=None):
        self.repository = repository
        self.cached_track = self.load_track(cfg, name=name)

    def load_track(self, cfg, name=None, params=None):
        if not params:
            params = {}
        # required in case a previous track using a different repository has specified the revision
        if cfg.opts("track", "repository.name") != self.repository:
            cfg.add(config.Scope.applicationOverride, "track", "repository.revision", None)
        # hack to make this work with multiple tracks (Rally core is usually not meant to be used this way)
        if name:
            cfg.add(config.Scope.applicationOverride, "track", "repository.name", self.repository)
            cfg.add(config.Scope.applicationOverride, "track", "track.name", name)
        # another hack to ensure any track-params in the race config are used by Rally's track loader
        cfg.add(config.Scope.applicationOverride, "track", "params", params)
        return track.load_track(cfg)

    def get_track(self, cfg, name=None, params=None):
        if params:
            return self.load_track(cfg, name, params)
        # if no params specified, return the initially cached, (non-parametrized) track
        return self.cached_track


def generate_index_ops(chart_type, race_configs, environment, logger):
    idx_race_configs = list(filter(lambda c: "indexing" in c.charts, race_configs))
    for race_conf in idx_race_configs:
        logger.debug("Gen index visualization for race config with name:[%s] / label:[%s] / flavor: [%s] / license: [%s]",
                     race_conf.name, race_conf.label, race_conf.flavor, race_conf.es_license)
    charts = []

    if idx_race_configs:
        title = chart_type.format_title(environment, race_configs[0].track, flavor=race_configs[0].flavor, suffix="indexing-throughput")
        charts = [chart_type.index(environment, idx_race_configs, title)]
    return charts


def generate_queries(chart_type, race_configs, environment):
    # output JSON structures
    structures = []

    for race_config in race_configs:
        if "query" in race_config.charts:
            for q in race_config.throttled_tasks:
                structures.append(chart_type.query(environment, race_config, q))
    return structures


def generate_io(chart_type, race_configs, environment):
    # output JSON structures
    structures = []
    for race_config in race_configs:
        if "io" in race_config.charts:
            title = chart_type.format_title(environment, race_config.track, es_license=race_config.es_license,
                                            suffix="%s-io" % race_config.label)
            structures.append(chart_type.io(title, environment, race_config))

    return structures


def generate_gc(chart_type, race_configs, environment):
    structures = []
    for race_config in race_configs:
        if "gc" in race_config.charts:
            title = chart_type.format_title(environment, race_config.track, es_license=race_config.es_license,
                                            suffix="%s-gc" % race_config.label)
            structures.append(chart_type.gc(title, environment, race_config))

    return structures


def generate_segment_memory(chart_type, race_configs, environment):
    structures = []
    for race_config in race_configs:
        if "segment_memory" in race_config.charts:
            title = chart_type.format_title(environment, race_config.track, es_license=race_config.es_license,
                                            suffix="%s-segment-memory" % race_config.label)
            chart = chart_type.segment_memory(title, environment, race_config)
            if chart:
                structures.append(chart)
    return structures


def generate_dashboard(chart_type, environment, track, charts, flavor=None):
    panels = []

    width = 24
    height = 32

    row = 0
    col = 0

    for idx, chart in enumerate(charts):
        panelIndex = idx + 1
        # make index charts wider
        if chart["_source"]["description"] == "index":
            chart_width = 2 * width
            # force one panel per row
            next_col = 0
        else:
            chart_width = width
            # two rows per panel
            next_col = (col + 1) % 2

        panel = {
            "id": chart["_id"],
            "panelIndex": panelIndex,
            "gridData": {
                "x": (col * chart_width),
                "y": (row * height),
                "w": chart_width,
                "h": height,
                "i": "{}".format(panelIndex)
            },
            "type": "visualization",
            "version": "6.3.2"
        }
        panels.append(panel)
        col = next_col
        if col == 0:
            row += 1

    return {
        "_id": str(uuid.uuid4()),
        "_type": "dashboard",
        "_source": {
            "title": chart_type.format_title(environment, track.name, flavor=flavor),
            "hits": 0,
            "description": "",
            "panelsJSON": json.dumps(panels),
            "optionsJSON": "{\"darkTheme\":false}",
            "uiStateJSON": "{}",
            "version": 1,
            "timeRestore": False,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(
                    {
                        "filter": [
                            {
                                "query": {
                                    "query_string": {
                                        "analyze_wildcard": True,
                                        "query": "*"
                                    }
                                }
                            }
                        ],
                        "highlightAll": True,
                        "version": True
                    }
                )
            }
        }
    }


class RaceConfig:
    def __init__(self, track, cfg=None, flavor=None, es_license=None, challenge=None, car=None, node_count=None, charts=None):
        self.track = track
        self.excluded_tasks = []
        if cfg.get("exclude-tasks") is not None:
            self.excluded_tasks =  cfg.get("exclude-tasks").split(",")
        if cfg:
            self.configuration = cfg
            self.configuration["flavor"] = flavor
            self.configuration["es_license"] = es_license
        else:
            self.configuration = {
                "charts": charts,
                "challenge": challenge,
                "car": car,
                "node-count": node_count
            }

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
                if sub_task.operation.type == track.OperationType.Bulk.to_hyphenated_string():
                    if sub_task.name not in self.excluded_tasks:
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
                # which tasks / or types of operations should be used for which chart types.
                if "target-throughput" in sub_task.params or "target-interval" in sub_task.params or sub_task.operation.type == "eql":
                    if sub_task.name not in self.excluded_tasks:
                        task_names.append(sub_task.name)
        return task_names


def load_race_configs(cfg, chart_type, chart_spec_path=None):
    def add_configs(race_configs_per_lic, flavor_name="oss", lic="oss", track_name=None):
        configs_per_lic = []
        for race_config in race_configs_per_lic:
            configs_per_lic.append(
                RaceConfig(track=race_config_track.get_track(cfg, name=track_name,
                                                             params=race_config.get("track-params", {})),
                           cfg=race_config,
                           flavor=flavor_name,
                           es_license=lic)
            )
        return configs_per_lic

    def add_race_configs(license_configs, flavor_name, track_name):
        if chart_type == BarCharts:
            # Only one license config, "basic", is present in bar charts
            _lic_conf = [license_config["configurations"] for license_config in license_configs if license_config["name"] == "basic"]
            if _lic_conf:
                race_configs_per_track.extend(add_configs(_lic_conf[0], track_name=track_name))
        else:
            for lic_config in license_configs:
                race_configs_per_track.extend(add_configs(lic_config["configurations"],
                                                          flavor_name,
                                                          lic_config["name"],
                                                          track_name))

    if chart_spec_path:
        race_configs = {"oss": [], "default": []}
        if chart_type == BarCharts:
            race_configs = []

        for _track_file in glob.glob(io.normalize_path(chart_spec_path)):
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
                            if chart_type == BarCharts:
                                race_configs.append(race_configs_per_track)
                            else:
                                race_configs[_flavor_name].append(race_configs_per_track)
    else:
        car_names = cfg.opts("mechanic", "car.names")
        if len(car_names) > 1:
            raise exceptions.SystemSetupError("Chart generator supports only a single car but got %s" % car_names)
        else:
            car_name = car_names[0]
        race_configs = [
            [
                RaceConfig(track=track.load_track(cfg),
                           challenge=cfg.opts("track", "challenge.name"),
                           car=car_name,
                           node_count=cfg.opts("generator", "node.count"),
                           charts=["indexing", "query", "gc", "io"])
             ]
        ]
    return race_configs


def gen_charts_per_track_configs(race_configs, chart_type, env, flavor=None, logger=None):
    charts = generate_index_ops(chart_type, race_configs, env, logger) + \
             generate_io(chart_type, race_configs, env) + \
             generate_gc(chart_type, race_configs, env) + \
             generate_segment_memory(chart_type, race_configs, env) + \
             generate_queries(chart_type, race_configs, env)

    dashboard = generate_dashboard(chart_type, env, race_configs[0].track, charts, flavor)

    return charts, dashboard


def gen_charts_per_track(race_configs, chart_type, env, flavor=None, logger=None):
    structures = []
    for race_configs_per_track in race_configs:
        charts, dashboard = gen_charts_per_track_configs(race_configs_per_track, chart_type, env, flavor, logger)
        structures.extend(charts)
        structures.append(dashboard)

    return structures


def gen_charts_from_track_combinations(race_configs, chart_type, env, logger):
    structures = []
    for flavor, race_configs_per_flavor in race_configs.items():
        for race_configs_per_track in race_configs_per_flavor:
            logger.debug("Generating charts for race_configs with name:[%s]/flavor:[%s]",
                         race_configs_per_track[0].name, flavor)
            charts, dashboard = gen_charts_per_track_configs(race_configs_per_track, chart_type, env, flavor, logger)

            structures.extend(charts)
            structures.append(dashboard)

    return structures


def generate(cfg):
    logger = logging.getLogger(__name__)

    chart_spec_path = cfg.opts("generator", "chart.spec.path", mandatory=False)
    if cfg.opts("generator", "chart.type") == "time-series":
        chart_type = TimeSeriesCharts
    else:
        chart_type = BarCharts

    console.info("Loading track data...", flush=True)
    race_configs = load_race_configs(cfg, chart_type, chart_spec_path)
    env = cfg.opts("system", "env.name")

    structures = []
    console.info("Generating charts...", flush=True)

    if chart_spec_path:
        if chart_type == BarCharts:
            # bar charts are flavor agnostic and split results based on a separate `user.setup` field
            structures = gen_charts_per_track(race_configs, chart_type, env, logger=logger)
        elif chart_type == TimeSeriesCharts:
            structures = gen_charts_from_track_combinations(race_configs, chart_type, env, logger)
    else:
        # Process a normal track
        structures = gen_charts_per_track(race_configs, chart_type, env, logger=logger)

    output_path = cfg.opts("generator", "output.path")
    if output_path:
        with open(io.normalize_path(output_path), mode="wt", encoding="utf-8") as f:
            print(json.dumps(structures, indent=4), file=f)
    else:
        print(json.dumps(structures, indent=4))
