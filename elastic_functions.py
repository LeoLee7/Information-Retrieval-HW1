
from elasticsearch import Elasticsearch
import logging

def connect_elasticsearch():
    _es = Elasticsearch("http://localhost:9200")
    if _es.ping():
        print('Connected to elasticsearch')
        logging.info(_es.ping())
    else:
        print('Error!, unable to connect to elastic')

    return _es

def document_structure():
	es_doc_structure = {
		"settings": {
			"number_of_shards": 2,
		    "number_of_replicas": 2,
			"analysis": {
			    "filter": {
			        "stop_filter": {
			            "type": "stop",
			            "stopwords": ["_english_"]
			        },
			        "stemmer_filter": {
			            "type": "stemmer",
			            "name": "minimal_english"
			        }
			    },
			    "analyzer": {
			        "my_analyzer": {
			            "type": "custom",
			            "tokenizer": "standard",
			            "filter": ["lowercase", "stop_filter", "stemmer_filter"],
			            "char_filter": ["html_strip"]
			        },
			        # "wp_raw_lowercase_analyzer": {
			        #     "type": "custom",
			        #     "tokenizer": "keyword",
			        #     "filter": ["lowercase"]
			        # }
			    }
			}
		},
		"mappings": {
			"dynamic": "strict",
	        "properties": {
	            "sequenceID": {"type": "integer"},
	            "MedID": {"type": "integer"},
	            "MeSH": {"type": "text"},
	            "Title": {
	            	"type": "text",
	            	"analyzer": "my_analyzer",
	            	"term_vector": "with_positions_offsets_payloads",
	            },
	            "PublicationType": {
	            	"type": "text",
	            	"analyzer": "my_analyzer",
	            	"term_vector": "with_positions_offsets_payloads",
	            },
	            "Abstract": {
	            	"type": "text",
	            	"analyzer": "my_analyzer",
	            	"term_vector": "with_positions_offsets_payloads",
	            },
	            "Author": {"type": "text"},
	            "Source": {"type": "text"}
	        }
	    }
	}


def tf_idf_query(qstring):
	query = {
		"from" : 0,
		"size" : 50,
	    "query": {
	    	"query_string" : {
	    		"query" : qstring.replace('/',''),
	    		"fields": ["Title", "PublicationType", "Abstract"]
	    	}
     	}
	}
	return query

def tf_query(qstring):
	bquery = {
        "from": 0,
        "size": 50,
        "query": {
            "function_score": {
                "query": {
                    "match": {
                        "Abstract": {
                            "query": qstring.replace('/', '')
                        }
                    }
                },
                "functions": [
                    {
                        "script_score": {
                            "script": {
                                "source": "int freq = 0; for (t in doc['Abstract'].value.split()) { if (t == term) { freq++; } } return freq;",
                                "params": {
                                    "term": qstring
                                }
                            }
                        }
                    }
                ],
                "boost_mode": "replace"
            }
        }
    }
	return bquery

def bool_query(title, abstract):


	bquery = {
		"from" : 0,
		"size" : 50,
		"query": {
		   "bool": {
		   		"should": { 
		            "match": { 
	             		"Title": {
	             			"query": title.replace('/',''),
	             		}
	             	}
	            },
	            "should":{
	            	"match":{
	            		"Abstract": abstract
	            	}
	            }
		    }
		}
	}
	return bquery


def rf_query(title,es):
		# Define the initial query
		title = title.replace('/','')
		initial_query = {
		    "query": {
		        "query_string": {
		            "query": title.replace('/', ''),
		            "fields": ["Title", "PublicationType", "Abstract"]
		        }
		    },
		    "size": 50
		}
		
		# Execute the initial search
		res = es.search(index='medical_records', body=initial_query)
		
		# Get the IDs of the top N documents
		N = 3
		doc_ids = [hit['_id'] for hit in res['hits']['hits'][:N]]
		
		# Define the pseudo-relevance feedback query
		feedback_query = {
			"from" : 0,
			"size" : 50,
		    "query": {
		        "bool": {
		            "should": [
		                {
		                    "query_string": {
		                        "query": title.replace('/', ''),
		                        "fields": ["Title", "PublicationType", "Abstract"]
		                    }
		                },
		                {
		                    "more_like_this": {
		                        "fields": ["Title", "PublicationType", "Abstract"],
		                        "like": doc_ids,
		                        "min_term_freq": 1,
		                        "max_query_terms": 20
		                    }
		                }
		            ]
		        }
		    },
		    "size": 50
			}
		return feedback_query




def get_rank_eval_query(qid, query_string, ratings, ids):
	
	req = [{
		"id": qid,                                  
		      "request": {                                              
		          "query": { 
		          		"multi_match": { 
		          			"query": query_string, 
		          			"fields": ["Title","Abstract","PublicationType" ]
		          	} 
		        }
		    }, 
		      "ratings": [
		      	{"_index": "medical_records", "_id":ids[i], "rating": ratings[i]} for i in range(len(ratings))
			]
	}]
	metric = {
		"mean_reciprocal_rank": {
                "k": 50,
                "relevant_rating_threshold": 0
        }
	}
	return req,  metric

def fuzzy_query(query_string,es):

	# define initial fuzzy query with parameters
	fquery = {
	    "query": {
	        "multi_match": {
	            "query": query_string,
	            "fields": ["Title", "Abstract", "PublicationType"],
	            "fuzziness": "AUTO"
	        }
	    }
	}
	
	# execute initial search
	res = es.search(index="medical_records", body=fquery)
	
	# get list of relevant document IDs from initial search results
	relevant_ids = [hit['_id'] for hit in res['hits']['hits']]
	
	# define threshold for number of relevant documents
	threshold = 10
	
	# define multi-pass fuzzy query with parameters
	mpfquery = {
	    "query": {
	        "multi_match": {
	            "query": query_string,
	            "fields": ["Title", "Abstract", "PublicationType"],
	            "fuzziness": 0
	        }
	    }
	}
	
	# execute multi-pass fuzzy search until we have enough relevant documents
	while len(relevant_ids) < threshold:
	    # execute fuzzy search with current fuzziness parameter
		res = es.search(index="my_index", body=mpfquery)
		# get list of relevant document IDs from fuzzy search results
		new_relevant_ids = [hit['_id'] for hit in res['hits']['hits']]
		# remove duplicates from list of relevant document IDs
		new_relevant_ids = list(set(new_relevant_ids) - set(relevant_ids))
		# add new relevant document IDs to list
		relevant_ids += new_relevant_ids
		# if no new relevant documents were found, stop searching
		if len(new_relevant_ids) == 0:
			break
		# increase fuzziness parameter for next search
		mpfquery["query"]["multi_match"]["fuzziness"] += 1
	
	# define pseudo-relevant feedback query with parameters
	prf_query = {
	    "query": {
	        "bool": {
	            "should": [
	                {"terms": {"_id": relevant_ids}},
	                {"more_like_this": {
	                    "fields": ["Title", "Abstract", "PublicationType"],
	                    "like": relevant_ids,
	                    "min_term_freq": 1,
	                    "min_doc_freq": 1
	                }}
	            ],
	            "minimum_should_match": 1
	        }
	    }
	}

	return fquery