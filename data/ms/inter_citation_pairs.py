from elasticsearch import Elasticsearch
import copy
import json


es = Elasticsearch(
    hosts="172.17.62.62:9200"
)
paper_index = "cs_paper_v2"
reference_index = "cs_paper_reference"
paper_query = {
    "size": 10000,
    "_source": ["paperId", "authors.authorId"]
}
reference_query = {
    "query": {
        "bool": {
            "must": [
            ]
        }
    },
    "size": 10000,
    "_source": ["refPaperId"]
}


def load_paper_dict():
    es_response = es.search(index=paper_index, body=paper_query, scroll="5m", request_timeout=30)
    paper_dict = {}
    # count = 0
    while len(es_response["hits"]["hits"]) > 0:
        try:
            sid = es_response["_scroll_id"]
            hits = es_response["hits"]["hits"]
            # count += 1
            print("processed 10000")
            for paper in hits:
                source = paper["_source"]
                if "authors" in source and source["authors"]:
                    paper_dict[source["paperId"]] = source["authors"]
            es_response = es.scroll(scroll_id=sid, scroll="5m", request_timeout=30)
            break
        except Exception as e:
            print(e)
    return paper_dict


def find_inter_citation(paper_dict):
    citation_dict = {}
    for paper in paper_dict:
        search_query = copy.deepcopy(reference_query)
        search_query["query"]["bool"]["must"].append({"term": {"paperId": paper}})
        es_response = es.search(index=reference_index, body=search_query, request_timeout=30)
        if es_response["hits"]["hits"]:
            hits = es_response["hits"]["hits"]
            ref_authors = {}
            for ref_paper in hits:
                ref_paper_id = ref_paper["_source"]["refPaperId"]
                if ref_paper_id not in paper_dict:
                    continue
                for author in paper_dict[ref_paper_id]:
                    author_id = author["authorId"]
                    if author_id not in ref_authors:
                        ref_authors[author_id] = 0
                    ref_authors[author_id] += 1
            if ref_authors:
                for author in paper_dict[paper]:
                    author_id = author["authorId"]
                    if author_id not in citation_dict:
                        citation_dict[author_id] = ref_authors
                    else:
                        for key in ref_authors:
                            if key not in citation_dict[author_id]:
                                citation_dict[author_id][key] = ref_authors[key]
                            else:
                                citation_dict[author_id][key] += ref_authors[key]

    citation_tuple = {}
    inter_citation_author_pairs = set()
    for src_author in citation_dict:
        for dst_author in citation_dict[src_author]:
            citation_tuple[(src_author, dst_author)] = citation_dict[src_author][dst_author]
            if (dst_author, src_author) in citation_tuple \
                    and (src_author, dst_author) not in inter_citation_author_pairs \
                    and (dst_author, src_author) not in inter_citation_author_pairs:
                inter_citation_author_pairs.add((src_author, dst_author))
    return inter_citation_author_pairs


if __name__ == '__main__':
    p_dict = load_paper_dict()
    with open('/mnt1/home/wzm/p_dict.json', 'w') as f:
        f.writelines(json.dumps(p_dict, ensure_ascii=False))
    # result = find_inter_citation(p_dict)
    # with open('/mnt1/home/wzm/result.txt', 'w') as f:
    #     for i in result:
    #         f.writelines(i[0] + i[1] + '\n')
