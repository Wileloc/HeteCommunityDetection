import time
from elasticsearch import Elasticsearch
import json
import copy


es = Elasticsearch(
    hosts="192.168.1.153:9200",
    http_auth=('elastic', 'GKzy123456')
)
search_index = "ms_affiliation_relation"
author_index = "ms_author"
paper_index = "ms_paper"
basic_scroll_query = {
    "size": 10000
}
author_query = {
    "size": 10000,
    "sort": {
        "rank": {
            "order": "asc"
        }
    }
}
relation_query = {
    "query": {
        "bool": {
            "must": [
            ]
        }
    }
}


def get_top_authors():
    es_response = es.search(index=author_index, scroll="5m", body=author_query, request_timeout=30)
    f_out = open('author.json', 'w')
    count = 0
    while len(es_response["hits"]["hits"]) > 0:
        try:
            sid = es_response["_scroll_id"]
            hits = es_response["hits"]["hits"]
            count += 1
            print("processed 10000")
            for author in hits:
                source = author["_source"]
                if source["lastKnownAffiliationId"]:
                    f_out.writelines(json.dumps({"authorId": source["authorId"], "paperCount": source["paperCount"],
                                                 "affiliationId": source["lastKnownAffiliationId"]}) + '\n')
            if count >= 34:
                break
            es_response = es.scroll(scroll_id=sid, scroll="5m", request_timeout=30)
        except Exception as e:
            print(e)
    f_out.close()


def scan_paper():
    es_response = es.search(index=paper_index, scroll="5m", body=basic_scroll_query, request_timeout=30)
    f_out = open('paper.json', 'w', encoding='utf-8')
    count = 0
    while len(es_response["hits"]["hits"]) > 0:
        try:
            sid = es_response["_scroll_id"]
            hits = es_response["hits"]["hits"]
            print("processed 10000")
            for paper in hits:
                source = paper["_source"]
                if source["year"] and int(source["year"]) >= 2017:
                    f_out.writelines(json.dumps({"paperId": source["paperId"]}) + '\n')
                    count += 1
                    if count % 100000 == 0:
                        print(f"get {count} papers")
            es_response = es.scroll(scroll_id=sid, scroll="5m", request_timeout=30)
        except Exception as e:
            print(sid)
            print(e)
    f_out.close()


def load_json_file(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            yield json.loads(line)


def get_author_by_paper_affiliation_pair(papers, affiliation_id):
    author_dict = {}
    print("total papers: ", len(papers))
    for p in papers:
        search_query = copy.deepcopy(relation_query)
        search_query["query"]["bool"]["must"].append({"term": {"paperId": p}})
        search_query["query"]["bool"]["must"].append({"term": {"affiliationId": affiliation_id}})
        respond = es.search(index=search_index, body=search_query, request_timeout=60)
        if respond["hits"]["hits"]:
            for relation in respond["hits"]["hits"]:
                source = relation["_source"]
                if source["authorId"] not in author_dict:
                    author_dict[source["authorId"]] = 0
                author_dict[source["authorId"]] += 1
    return [author for author in author_dict if author_dict[author] >= 2]


def get_community_by_author():
    authors = load_json_file('author.json')
    f_out = open('pairs.json', 'w')
    start = time.clock()
    for idx, a in enumerate(authors):
        search_query = copy.deepcopy(relation_query)
        search_query["query"]["bool"]["must"].append({"term": {"authorId": a["authorId"]}})
        search_query["query"]["bool"]["must"].append({"term": {"affiliationId": a["affiliationId"]}})
        search_query["size"] = a["paperCount"]
        respond = es.search(index=search_index, body=search_query, request_timeout=60)
        if respond["hits"]["hits"]:
            papers = set()
            for relation in respond["hits"]["hits"]:
                source = relation["_source"]
                if int(source["year"]) >= 2017:
                    papers.add(source["paperId"])
            related_author = get_author_by_paper_affiliation_pair(papers, a["affiliationId"])
            f_out.writelines('\t'.join(related_author) + '\n')
        if (idx + 1) % 100 == 0:
            current = time.clock()
            print(f"processed 100, cost {current - start} s")
            start = current
    f_out.close()


if __name__ == '__main__':
    # scan_paper()
    # get_top_authors()
    get_community_by_author()
