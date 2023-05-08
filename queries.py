from datetime import datetime
from elasticsearch import Elasticsearch
import logging, json, time
from constants import file_identifiers, query_constants as qc, doc_constants as dc
from elastic_functions import connect_elasticsearch, tf_idf_query, bool_query,rf_query,fuzzy_query, tf_query,get_rank_eval_query


with open(qc.rels_file) as f:
	relslines = f.readlines()

def get_single_query(lines,curr, file_length):
	single_query = {}
	while curr < file_length:
		line = lines[curr].strip('\n')
		if line == qc.qstart:
			curr+=1
		elif line == qc.qend:
			break
		else:
			line_list = line.strip('<')
			temp = ''.join(line_list)
			line_list2 = temp.split('>')
			
			if line_list2[0] == qc.num:
				key_value = line_list2[1].split(':')
				single_query[qc.num]=key_value[1]

			elif line_list2[0] == qc.title:
				single_query[qc.title]=line_list2[1]
			
			elif line_list2[0]== qc.description:
				curr+=1
				single_query[qc.description]=lines[curr].strip('\n')
			curr+=1
	return single_query, curr+1

def write_results_to_file(algorithm, query_id, results, output_file):
 	#	R101 Q0 83653 1 0.430352 tfidf-Both 

	with open(output_file, 'a+') as f:
		for i in range(len(results)):
			# if algorithm == qc.rank_eval:
			# 	doc_id = results[i]["hit"]["_id"]
			# 	score = str(results[i]["hit"]["_score"])
			# else:
			doc_id = results[i]["_id"]
			score = str(results[i]["_score"])
			
			file_string = f"{query_id} Q0 {doc_id} {str(i+1)} {score} {algorithm}\n"
			f.write(file_string)
	logging.info("Writing results to file")

def parse_rels_file(qid):
	ratings= []
	ids = []
	for line in relslines:
		term = line.strip('\n').split('\t')
		if term[0] == qid:
			ids.append(term[2])
			ratings.append(int(term[3]))

	return ratings,ids

   
def rank_search (es, query):
	
	ratings,ids = parse_rels_file(query["num"])
	req, metric = get_rank_eval_query(query["num"], query[qc.title], ratings[:20], ids[:20])
	
	result  = es.rank_eval(index=dc.index_name, requests=req, metric =metric)
	
	hits = result["details"][query["num"]]["hits"]
	
	write_results_to_file(qc.rank_eval, query["num"], hits, qc.rank_log_file)


def search_query(es, query, algorithm):

	if algorithm == qc.tf_idf:
		qstring= query[qc.title]+" "+query[qc.description]
		body_query = tf_idf_query(qstring)
		output_file = qc.tfidf_log_file

	elif algorithm == qc.bquery:
		body_query = bool_query(query[qc.title], query[qc.description])
		output_file = qc.bool_log_file

	elif algorithm == qc.relevance_feedback:

		body_query = rf_query(query[qc.title],es)
		output_file = qc.relevance_feedback_log_file

	elif algorithm == qc.tf:
		qstring= query[qc.title]
		body_query = tf_query(qstring)
		output_file = qc.tf_log_file

	elif algorithm == qc.fuzzy:
		qstring= query[qc.title]+" "+query[qc.description]
		body_query = fuzzy_query(qstring,es)
		output_file = qc.fuzzy_log_file
	
	try:
		result = es.search(index=dc.index_name, body=body_query, track_scores= True)
		write_results_to_file(algorithm, query["num"], result["hits"]["hits"], output_file)

	except Exception as ex:
		print('Error in querying data', str(ex))
		logging.info(str(ex))

if __name__ == "__main__":

	logging.basicConfig(filename='searchLogs.log', filemode='w+', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
	es = connect_elasticsearch()
	
	start_time = time.time()
	with open(qc.qfile_name) as f:
		lines = f.readlines()
		curr, file_length = 0, len(lines)
		while curr < file_length:
			query, curr = get_single_query(lines, curr, file_length)
			print('searching query with id', query["num"])
			
			search_query(es, query, qc.relevance_feedback)
			# search_query(es, query, qc.tf_idf)
			# search_query(es, query, qc.bquery)
			# search_query(es,query, qc.fuzzy)
			# rank_search(es,query)
			curr+=1
			
		
	print('execution time is ', time.time()-start_time)