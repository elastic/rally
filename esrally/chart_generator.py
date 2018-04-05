import uuid
import json

from esrally import track, config, exceptions
from esrally.utils import io, console

color_scheme_rgba = [
    "rgba(0,191,179,1)",
    "rgba(0,169,224,1)",
    "rgba(240,78,152,1)",
    "rgba(255,205,0,1)",
    "rgba(0,118,168,1)",
    "rgba(147,201,14,1)",
    "rgba(100,100,100,1)",
]


def index_label(combination_label, challenge, car, plugins, node_count):
    if combination_label:
        return combination_label

    label = "%s-%s" % (challenge, car)
    if plugins:
        label += "-%s" % plugins.replace(":", "-").replace(",", "+")
    if node_count > 1:
        label += " (%d nodes)" % node_count
    return label


def format_title(environment, track_name, suffix=None):
    if suffix:
        return "%s-%s-%s" % (environment, track_name, suffix)
    else:
        return "%s-%s" % (environment, track_name)


class BarCharts:
    UI_STATE_JSON = json.dumps({})
    # UI_STATE_JSON = json.dumps({"vis": {"colors": {"bare": "#00BFB3", "docker": "#00A9E0", "ear": "#F04E98", "x-pack": "#FFCD00"}}})

    @staticmethod
    def gc(title, environment, track, combination_name, challenge, car, node_count):
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
                        "field": "user-tags.env",
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
                    "query": filter_string(environment, combination_name, track, challenge, car, node_count),
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
                "description": "",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps(search_source)
                }
            }
        }

    @staticmethod
    def io(title, environment, track, combination_name, challenge, car, node_count):
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
                    "type": "median",
                    "schema": "metric",
                    "params": {
                        "field": "value.single",
                        "percents": [
                            50
                        ],
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
                        "field": "user-tags.env",
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
                    "query": filter_string(environment, combination_name, track, challenge, car, node_count),
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
                "description": "",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps(search_source)
                }
            }
        }

    @staticmethod
    def query(environment, track, combination_name, challenge, car, node_count, q):
        metric = "latency"
        title = format_title(environment, track, "%s-p99-%s" % (q, metric))
        label = "Query Latency [ms]"

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
                        "field": "user-tags.env",
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
                    "query": "name:\"%s\" AND task:\"%s\" AND %s" %
                             (metric, q, filter_string(environment, combination_name, track, challenge, car, node_count)),
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
                "description": "",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps(search_source)
                }
            }
        }

    @staticmethod
    def index(environment, track, cci, title):
        filters = []
        for idx, item in enumerate(cci):
            combination_name, combination_label, challenge, car, plugins, node_count, index_task = item
            label = index_label(combination_label, challenge, car, plugins, node_count)
            filters.append({
                "input": {
                    "query": {
                        "query_string": {
                            "analyze_wildcard": True,
                            "query": "task:\"%s\" AND %s"
                                     % (index_task, filter_string(environment, combination_name, track, challenge, car, node_count))
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
                        "field": "user-tags.env",
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
                "description": "",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps(search_source)
                }
            }
        }


class TimeSeriesCharts:
    @staticmethod
    def gc(title, environment, track, combination_name, challenge, car, node_count):
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
                "time_field": "trial-timestamp",
                "type": "timeseries",
                "filter": filter_string(environment, combination_name, track, challenge, car, node_count),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "rally-annotations",
                        "query_string": "((NOT _exists_:track) OR track:\"%s\") AND ((NOT _exists_:chart) OR chart:gc) "
                                        "AND environment:\"%s\"" % (track, environment),
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "trial-timestamp",
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
                "description": "",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"query\":{\"query_string\":{\"query\":\"*\"}},\"filter\":[]}"
                }
            }
        }

    @staticmethod
    def io(title, environment, track, combination_name, challenge, car, node_count):
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
                                "label": "Bytes Written",
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
                "time_field": "trial-timestamp",
                "type": "timeseries",
                "filter": filter_string(environment, combination_name, track, challenge, car, node_count),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "rally-annotations",
                        "query_string": "((NOT _exists_:track) OR track:\"%s\") AND ((NOT _exists_:chart) OR chart:io) "
                                        "AND environment:\"%s\"" % (track, environment),
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "trial-timestamp",
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
                "description": "",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"query\":{\"query_string\":{\"query\":\"*\"}},\"filter\":[]}"
                }
            }
        }

    @staticmethod
    def query(environment, track, combination_name, challenge, car, node_count, q):
        metric = "latency"
        title = format_title(environment, track, "%s-%s" % (q, metric))

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
                        "series_drop_last_bucket": 0
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
                        "series_drop_last_bucket": 0
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
                        "series_drop_last_bucket": 0
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
                        "series_drop_last_bucket": 0
                    }
                ],
                "time_field": "trial-timestamp",
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
                "filter": "task:\"%s\" AND name:\"%s\" AND %s" %
                          (q, metric, filter_string(environment, combination_name, track, challenge, car, node_count)),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "rally-annotations",
                        "query_string": "((NOT _exists_:track) OR track:\"%s\") AND ((NOT _exists_:chart) OR chart:query) "
                                        "AND environment:\"%s\"" % (track, environment),
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "trial-timestamp",
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
                "description": "",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"query\":{\"query_string\":{\"query\":\"*\"}},\"filter\":[]}"
                }
            }
        }

    @staticmethod
    def index(environment, track, cci, title):
        filters = []
        for idx, item in enumerate(cci):
            combination_name, combination_label, challenge, car, plugins, node_count, index_task = item
            label = index_label(combination_label, challenge, car, plugins, node_count)
            filters.append(
                {
                    "filter": "task:\"%s\" AND %s"
                              % (index_task, filter_string(environment, combination_name, track, challenge, car, node_count)),
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
                        "filter": "environment:\"%s\" AND track:\"%s\"" % (environment, track),
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
                "time_field": "trial-timestamp",
                "type": "timeseries",
                "filter": "environment:\"%s\" AND track:\"%s\" AND name:\"throughput\" AND active:true" % (environment, track),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "rally-annotations",
                        "query_string": "((NOT _exists_:track) OR track:\"%s\") AND ((NOT _exists_:chart) OR chart:indexing) "
                                        "AND environment:\"%s\"" % (track, environment),
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "trial-timestamp",
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
                "description": "",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"query\":{\"query_string\":{\"query\":\"*\"}},\"filter\":[]}"
                }
            }
        }


def load_track(cfg, name=None):
    # hack to make this work with multiple tracks (Rally core is usually not meant to be used this way)
    if name:
        cfg.add(config.Scope.applicationOverride, "track", "track.name", name)
    return track.load_track(cfg)


def generate_index_ops(chart_type, race_config, environment):
    def tracks_for_index():
        t = race_config["track"]
        cci = []
        for combination in race_config["combinations"]:
            combination_name = combination.get("name")
            combination_label = combination.get("label")
            challenge = combination["challenge"]
            car = combination["car"]
            node_count = int(combination.get("node-count", 1))
            plugins = combination.get("plugins")
            for task in t.find_challenge_or_default(challenge).schedule:
                for sub_task in task:
                    if sub_task.operation.type == track.OperationType.Bulk.name:
                        cci.append((combination_name, combination_label, challenge, car, plugins, node_count, sub_task.name))
        return t.name, cci

    t, cci = tracks_for_index()
    title = format_title(environment, t, "indexing-throughput")
    return [chart_type.index(environment, t, cci, title)]


def default_tracks(race_config):
    defaults = []
    for combination in race_config["combinations"]:
        t = race_config["track"]
        combination_name = combination.get("name")
        challenge = combination["challenge"]
        car = combination["car"]
        node_count = int(combination.get("node-count", 1))
        # only generate some charts for the default combination (we might want to make this configurable)
        default_combination = combination.get("default-combination", False)
        ops = []
        if default_combination:
            for task in t.find_challenge(challenge).schedule:
                for sub_task in task:
                    # We are assuming here that each task with a target throughput or target interval is interesting for latency charts.
                    if "target-throughput" in sub_task.params or "target-interval" in sub_task.params:
                        ops.append(sub_task.name)
            defaults.append((t.name, combination_name, challenge, car, node_count, ops))

    return defaults


def filter_string(environment, combination_name, t, challenge, car, node_count):
    if combination_name:
        return "environment:\"%s\" AND active:true AND user-tags.name:\"%s\"" % (environment, combination_name)
    else:
        return "environment:\"%s\" AND active:true AND track:\"%s\" AND challenge:\"%s\" AND car:\"%s\" AND node-count:%d" \
               % (environment, t, challenge, car, node_count)


def generate_queries(chart_type, race_config, environment):
    # output JSON structures
    structures = []
    for track, combination_name, challenge, car, node_count, queries in default_tracks(race_config):
        for q in queries:
            structures.append(chart_type.query(environment, track, combination_name, challenge, car, node_count, q))
    return structures


def generate_io(chart_type, race_config, environment):
    # output JSON structures
    structures = []
    for track, combination_name, challenge, car, node_count, queries in default_tracks(race_config):
        title = format_title(environment, track, "io")
        structures.append(chart_type.io(title, environment, track, combination_name, challenge, car, node_count))

    return structures


def generate_gc(chart_type, race_config, environment):
    structures = []
    for track, combination_name, challenge, car, node_count, queries in default_tracks(race_config):
        title = format_title(environment, track, "gc")
        structures.append(chart_type.gc(title, environment, track, combination_name, challenge, car, node_count))

    return structures


def generate_dashboard(environment, track, charts):
    panels = []

    width = 6
    height = 6

    row = 0
    col = 0

    for idx, chart in enumerate(charts):
        panel = {
            "id": chart["_id"],
            "panelIndex": idx,
            "row": (row * height) + 1,
            "col": (col * width) + 1,
            "size_x": width,
            "size_y": height,
            "type": "visualization"
        }
        panels.append(panel)
        # two rows per panel
        col = (col + 1) % 2
        if col == 0:
            row += 1

    return {
        "_id": str(uuid.uuid4()),
        "_type": "dashboard",
        "_source": {
            "title": format_title(environment, track.name),
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


def load_race_configs(cfg):
    chart_spec_path = cfg.opts("generator", "chart.spec.path", mandatory=False)
    if chart_spec_path:
        import json
        race_configs = []
        with open(io.normalize_path(chart_spec_path), mode="rt", encoding="utf-8") as f:
            for item in json.load(f):
                # load track based on its name and replace it
                item["track"] = load_track(cfg, item["track"])
                race_configs.append(item)
    else:
        t = load_track(cfg)

        car_names = cfg.opts("mechanic", "car.names")
        if len(car_names) > 1:
            raise exceptions.SystemSetupError("Chart generator supports only a single car but got %s" % car_names)
        else:
            car_name = car_names[0]

        race_configs = [
            {
                "track": t,
                "combinations": [
                    {
                        "challenge": cfg.opts("track", "challenge.name"),
                        "car": car_name,
                        "node-count": cfg.opts("generator", "node.count")
                    }
                ]
            }
        ]
    return race_configs


def generate(cfg):
    if cfg.opts("generator", "chart.type") == "time-series":
        chart_type = TimeSeriesCharts
    else:
        chart_type = BarCharts

    console.info("Loading track data...", flush=True)
    race_configs = load_race_configs(cfg)
    env = cfg.opts("system", "env.name")

    structures = []
    console.info("Generating charts...", flush=True)
    for race_config in race_configs:

        charts = generate_index_ops(chart_type, race_config, env) + \
                 generate_io(chart_type, race_config, env) + \
                 generate_gc(chart_type, race_config, env) + \
                 generate_queries(chart_type, race_config, env)

        dashboard = generate_dashboard(env, race_config["track"], charts)

        structures.extend(charts)
        structures.append(dashboard)

    output_path = cfg.opts("generator", "output.path")
    if output_path:
        with open(io.normalize_path(output_path), mode="wt", encoding="utf-8") as f:
            print(json.dumps(structures, indent=4), file=f)
    else:
        print(json.dumps(structures, indent=4))
