import rdflib
import os
from rdflib.plugins.stores import sparqlstore
import simpleconfigparser
import pprint
import csv

# method to read in files and call one by one
def query_iterator(graph):
    results = []

    # portable for *NIX/Windows
    if os.name == "nt":
        path = os.getcwd() + "\\queries\\"
    elif os.name == "posix":
        path = os.getcwd() + "/queries/"

    for query_file in os.listdir(path):
        if(query_file.endswith("rq")):
            print("Running " + query_file)
            f = open(path + query_file, 'r')
            results.append(graph.query(f.read()))
    return results

if __name__ == "__main__":
    cp = simpleconfigparser.simpleconfigparser()
    cp.read("config/exgest_config.ini")

    store = sparqlstore.SPARQLUpdateStore()
    store.open((cp.settings.endpoint, cp.settings.endpoint))

    g = rdflib.Graph(store)

    results = query_iterator(g)

    result_hash = {}

    field_names = None

    #collect all the data into one data structure
    for result in results:
        for row in result:

            #set the field_names, if it hasn't already
            if field_names is None:
                field_names = row.labels.keys()

            key = row[cp.settings.result_key]
            if key in result_hash:
                pass
            else:
                result_hash[key] = {}

            for column in row.labels:
                #create set if it doesn't exist
                if column in result_hash[key]:
                    pass
                else:
                    result_hash[key][column] = set()

                #add data to set
                if(row[column] is not None and type(row[column]) == rdflib.term.Literal):
                    result_hash[key][column].add(row[column].value.strip())
                elif(row[column] is not None and type(row[column]) == rdflib.term.URIRef):
                    result_hash[key][column].add(str(row[column]))

    pprint.pprint(field_names)

    for i in result_hash:
        for j in result_hash[i]:
            result_hash[i][j] = ", ".join(result_hash[i][j])

    #write out all the data into a csv file
    with open(cp.settings.output_file, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names, delimiter='\t')
        writer.writeheader()
        for key in result_hash:
            writer.writerow(result_hash[key])


