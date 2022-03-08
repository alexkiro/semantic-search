import json

import requests


def make_query(size=10):
    query = {
        "bool": {
            "must": [{"match_all": {}}],
            "filter": [
                {
                    "constant_score": {
                        "filter": {
                            "bool": {
                                "should": [
                                    {
                                        "bool": {
                                            "must_not": {"exists": {"field": "expires"}}
                                        }
                                    },
                                    {
                                        "range": {
                                            "expires": {"gte": "2022-02-22T12:58:09Z"}
                                        }
                                    },
                                ]
                            }
                        }
                    }
                },
                {
                    "bool": {
                        "should": [{"range": {"readingTime": {}}}],
                        "minimum_should_match": 1,
                    }
                },
                {
                    "bool": {
                        "should": [{"range": {"issued.date": {}}}],
                        "minimum_should_match": 1,
                    }
                },
                {
                    "bool": {
                        "should": [{"term": {"language": "en"}}],
                        "minimum_should_match": 1,
                    }
                },
                {"term": {"hasWorkflowState": "published"}},
                {
                    "constant_score": {
                        "filter": {
                            "range": {"issued.date": {"lte": "2022-02-22T13:03:04Z"}}
                        }
                    }
                },
            ],
        }
    }
    runtime_mappings = {
        "op_cluster": {
            "type": "keyword",
            "script": {
                "source": 'emit("_all_"); def clusters_settings = [["name": "News", "values": ["News","Article"]],["name": "Publications", "values": ["Report","Indicator","Briefing","Topic page","Country fact sheet"]],["name": "Maps and charts", "values": ["Figure (chart/map)","Chart (interactive)","Infographic","Dashboard","Map (interactive)"]],["name": "Data", "values": ["External data reference","Data set"]],["name": "Others", "values": ["Webpage","Organisation","FAQ","Video","Contract opportunity","Glossary term","Collection","File","Adaptation option","Guidance","Research and knowledge project","Information portal","Tool","Case study"]]]; def vals = doc[\'objectProvides\']; def clusters = [\'All\']; for (val in vals) { for (cs in clusters_settings) { if (cs.values.contains(val)) { emit(cs.name) } } }'
            },
        }
    }
    body = {
        "query": {
            "function_score": {
                "query": query,
                "functions": [
                    {"exp": {"issued.date": {"offset": "30d", "scale": "1800d"}}}
                ],
                "score_mode": "sum",
            }
        },
        "aggs": {},
        "size": size,
        "runtime_mappings": runtime_mappings,
        "params": {},
        "track_total_hits": True,
    }
    resp = requests.post(
        "http://search.ai-lab-aws.eea.europa.eu/_es/globalsearch/_search", json=body
    )
    resp.raise_for_status()
    return resp.json()


results = make_query(1000)
with open("data1000.jsonl", "w") as f:
    for result in results["hits"]["hits"]:
        f.write(json.dumps(result["_source"]))
        f.write("\n")
