import json
import math
from pprint import pprint

import requests
import spacy
import tensorflow_hub
from tensorflow.python.ops.numpy_ops import np_config

np_config.enable_numpy_behavior()

index = "test-index-2"
es_url = "http://localhost:9200"
model = "https://tfhub.dev/google/universal-sentence-encoder-large/5"
embed = tensorflow_hub.load(model)
max_words = 512


def embed_text(text):
    for vector in embed([text]):
        yield vector.tolist()


def maybe_split_sentence(sent):
    if len(sent) <= max_words:
        yield sent
        return

    middle = math.floor(len(sent) / 2)
    yield from maybe_split_sentence(sent[:middle])
    yield from maybe_split_sentence(sent[middle:])


def get_embeddings(fulltext, lang="en"):
    nlp = spacy.blank(lang)
    nlp.add_pipe("sentencizer")

    # TODO: Normalize to UNIX EOL?
    for paragraph in fulltext.split("\n\n"):
        parsed = nlp(paragraph)

        words = 0
        chunk = []

        for sentence in parsed.sents:
            for part in maybe_split_sentence(sentence):
                if words + len(part) > max_words:
                    yield from embed_text(" ".join(chunk))
                    words = 0
                    chunk = []

                words += len(part)
                chunk.append(part.text)

        if chunk:
            yield from embed_text(" ".join(chunk))


def get_sentence_embeddings(fulltext, lang="en"):
    nlp = spacy.blank(lang)
    nlp.add_pipe("sentencizer")

    # TODO: Normalize to UNIX EOL?
    for paragraph in fulltext.split("\n\n"):
        parsed = nlp(paragraph)

        for sentence in parsed.sents:
            for part in maybe_split_sentence(sentence):
                yield from embed_text(part.text)


def main():
    try:
        resp = requests.delete(f"{es_url}/{index}")
        pprint(resp.json())
        resp.raise_for_status()
    except Exception as e:
        print(e)

    with open("index-settings.json") as f:
        data = json.load(f)

    resp = requests.put(f"{es_url}/{index}", json=data)
    pprint(resp.json())
    resp.raise_for_status()

    bulk_data = []

    for _id, line in enumerate(open("data1000.jsonl"), start=1):
        doc = json.loads(line)
        bulk_data.append({"index": {"_index": index, "_id": _id}})
        bulk_data.append(
            {
                "id": doc["id"],
                "fulltext": doc["fulltext"],
                "topic": doc["topic"],
                "creator": doc["creator"],
                "embeddings": [
                    {"vector": vector for vector in get_embeddings(doc["fulltext"])}
                ],
                "sentence_embeddings": [
                    {
                        "vector": vector
                        for vector in get_sentence_embeddings(doc["fulltext"])
                    }
                ],
            }
        )

    data = "\n".join(json.dumps(item) for item in bulk_data) + "\n"

    resp = requests.post(
        f"{es_url}/_bulk",
        data=data,
        headers={"content-type": "application/json"},
    )
    pprint(resp.json())
    resp.raise_for_status()

    resp = requests.post(f"{es_url}/_refresh")
    pprint(resp.json())
    resp.raise_for_status()

    print("")
    print(requests.get(f"{es_url}/_cat/indices?v").text)


if __name__ == "__main__":
    main()
