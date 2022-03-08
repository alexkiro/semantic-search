import time

from init_es import *


def main():
    query_type = input("Select query type? [match/semantic] ")
    text_query = input("Input search query: ")
    # Reasonable assumption that the query is less than 512 words
    start = time.time()
    query_vector = list(embed_text(text_query))[0]
    print("embed time %.2fs" % (time.time() - start))

    script = """
        (cosineSimilarity(params.query_vector, 'embeddings.vector') + 1.0)
    """
    match_query = {
        "query_string": {
            "query": text_query,
            "fields": [
                "fulltext",
            ],
        },
    }
    semantic_score = {
        "nested": {
            "path": "embeddings",
            "score_mode": "max",
            "query": {
                "function_score": {
                    "script_score": {
                        "script": {
                            "source": script,
                            "params": {"query_vector": query_vector},
                        }
                    }
                }
            },
        }
    }

    query = {
        "bool": {
            "must": [match_query if query_type == "match" else semantic_score],
            # "must": [
            #     match_query,
            #     semantic_score,
            # ],
            "filter": [
                {
                    "terms": {
                        "topic": ["Climate change adaptation"],
                    },
                },
            ],
            "should": [],
        }
    }

    body = {
        "query": {
            "function_score": {
                "query": query,
                "functions": [
                    # {"exp": {"issued.date": {"offset": "30d", "scale": "1800d"}}}
                ],
                "score_mode": "sum",
            }
        },
    }

    resp = requests.post(f"{es_url}/{index}/_search", json=body)

    if not resp.ok:
        pprint(resp.json())
    resp.raise_for_status()

    results = resp.json()
    # pprint(results)
    print("full round-trip time %.2fs" % (time.time() - start))

    doc = results["hits"]["hits"][0]
    embeddings = doc["_source"].pop("embeddings")
    pprint(doc)

    resp = requests.get(f"{es_url}/{index}/_explain/{doc['_id']}", json=body)
    pprint(resp.json())
    resp.raise_for_status()


if __name__ == "__main__":
    while True:
        main()
