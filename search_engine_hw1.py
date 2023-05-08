from datetime import datetime
from elasticsearch import Elasticsearch
import logging, json, time
from constants import file_identifiers, query_constants as qc, doc_constants as dc
from elastic_functions import (
    connect_elasticsearch, tf_idf_query, bool_query, rf_query,
    wildcard_query, tf_query, get_rank_eval_query, document_structure,
)

def extract_document(lines, current_line, num_lines):
    """
    Extracts a single document from the list of lines in the file.
    :param lines: List of lines in the file.
    :param current_line: The index of the current line being processed.
    :param num_lines: Total number of lines in the file.
    :return: A dictionary containing the extracted document and the index of the current line after extraction.
    """
    document = {}
    while current_line < num_lines:
        line = lines[current_line].strip('\n')
        identifier = line[:2]
        if identifier in file_identifiers.keys():
            current_identifier = file_identifiers[identifier]
            if identifier == '.I':
                if current_identifier in document:
                    return document, current_line
                document[current_identifier] = line[2:]
            else:
                current_line += 1
                document[current_identifier] = lines[current_line].strip('\n')
        current_line += 1
    return document, current_line


def create_index(es, index_name, mapping):
    """
    Create an ES index with the provided mapping schema.
    :param index_name: Name of the index.
    :param mapping: Mapping schema for the index.
    """
    logging.info(f"Creating index {index_name} with the following schema:{json.dumps(mapping, indent=2)}")
    es.indices.create(index=index_name, ignore=400, body=mapping)

def store_record(es, index_name, document):
    try:
        outcome = es.index(index=index_name, body=document, id=document[dc.doc_id])
    except Exception as e:
        print('Error in indexing document')
        print(str(e))
        logging.info(str(e))



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

			elif line_list2[0] == 'title':
				single_query['title']=line_list2[1]
			
			elif line_list2[0]== 'desc':
				curr+=1
				single_query['desc']=lines[curr].strip('\n')
			curr+=1
	return single_query, curr+1

def write_results_to_file(algorithm, query_id, results, output_file):
	
	with open(output_file, 'a+') as f:
		for i in range(len(results)):
			doc_id = results[i]["_id"]
			score = str(results[i]["_score"])
			
			file_string = f"{query_id} Q0 {doc_id} {str(i+1)} {score} {algorithm}\n"
			f.write(file_string)
	logging.info("Writing results to file")


def search_query(es, query, algorithm):

	if algorithm == 'tf_idf':
		qstring= query['title']+" "+query['desc']
		body_query = tf_idf_query(qstring)


	elif algorithm == 'bquery':
		body_query = bool_query(query['title'], query['desc'])


	elif algorithm == 'relevance_feedback':
		body_query = rf_query(query['title'],es)


	elif algorithm == 'tf':
		qstring= query['title']
		body_query = tf_query(qstring)


	elif algorithm == 'wildcard':
		qstring= query['title']+" "+query['desc']
		body_query = wildcard_query(qstring,es)

	
	try:
		result = es.search(index=dc.index_name, body=body_query, track_scores= True)
		output_file = 'results/{}_log.txt'.format(algorithm)
		write_results_to_file(algorithm, query["num"], result["hits"]["hits"], output_file)

	except Exception as ex:
		print('Error in querying data', str(ex))
		logging.info(str(ex))




if __name__ == "__main__":


    # Configure logging
    logging.basicConfig(
        filename='searchLogs.log',
        filemode='w+',
        format='%(name)s - %(levelname)s - %(message)s',
        level=logging.DEBUG
    )
    
    # Connect to Elasticsearch
    es = connect_elasticsearch()
    
    # Index documents
    start_time = time.time()
    with open(dc.dfile_name) as f:
        lines = f.readlines()
        current_line, num_lines = 0, len(lines)
        while current_line < num_lines:
            document, current_line = extract_document(lines, current_line, num_lines)
            store_record(es, dc.index_name, document)

    
    print('Total time:', time.time()-start_time)
    logging.info("execution time is %s", str(time.time()-start_time))
    
    logging.basicConfig(filename='searchLogs.log', filemode='w+', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    es = connect_elasticsearch()
    
    start_time = time.time()
    with open(qc.qfile_name) as f:
        lines = f.readlines()
        curr, file_length = 0, len(lines)
        while curr < file_length:
            query, curr = get_single_query(lines, curr, file_length)
            print('searching query with id', query["num"])

            search_query(es, query, 'relevance_feedback')
            search_query(es, query, 'tf_idf')
            search_query(es, query, 'bquery')
            search_query(es,query, 'wildcard')
            search_query(es,query, 'tf')

            curr+=1
    
    
    print('execution time is ', time.time()-start_time)
