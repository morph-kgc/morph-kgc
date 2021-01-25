import os
import sys
import csv
import mysql.connector
import psycopg2
from configparser import ConfigParser, ExtendedInterpolation
from rdflib import Graph, RDF, Namespace, compare, Literal, URIRef


def test_all():
    q1 = """SELECT ?database_uri WHERE { 
        ?database_uri rdf:type <http://purl.org/NET/rdb2rdf-test#DataBase>. 
      } ORDER BY ?database_uri"""
    for r in manifest_graph.query(q1):
        database_uri = r.database_uri
        d_identifier = manifest_graph.value(subject=database_uri, predicate=DCELEMENTS.identifier, object=None)
        d_title = manifest_graph.value(subject=database_uri, predicate=DCELEMENTS.title, object=None)
        database = manifest_graph.value(subject=database_uri, predicate=RDB2RDFTEST.sqlScriptFile, object=None)
        print("**************************************************************************")
        print("Using the database: " + d_identifier + " (" + d_title + ")")
        database_load(database)
        q2 = """SELECT ?test_uri WHERE { 
                ?test_uri <http://purl.org/NET/rdb2rdf-test#database> ?database_uri. 
              } ORDER BY ?test_uri"""
        for r2 in manifest_graph.query(q2, initBindings={'?database_uri': URIRef(database_uri)}):
            test_uri = r2.test_uri
            t_identifier = manifest_graph.value(subject=test_uri, predicate=DCELEMENTS.identifier, object=None)
            t_title = manifest_graph.value(subject=test_uri, predicate=DCELEMENTS.title, object=None)
            purpose = manifest_graph.value(subject=test_uri, predicate=TESTDEC.purpose, object=None)
            expected_output = bool(
                manifest_graph.value(subject=test_uri, predicate=RDB2RDFTEST.hasExpectedOutput, object=None))
            r2rml = manifest_graph.value(subject=test_uri, predicate=RDB2RDFTEST.mappingDocument, object=None)
            print("-----------------------------------------------------------------")
            print("Testing R2RML test-case: " + t_identifier + " (" + t_title + ")")
            print("Purpose of this test is: " + purpose)
            run_test(t_identifier, r2rml, test_uri, expected_output)



def test_one(identifier):
    test_uri = manifest_graph.value(subject=None, predicate=DCELEMENTS.identifier, object=Literal(identifier))
    t_title = manifest_graph.value(subject=test_uri, predicate=DCELEMENTS.title, object=None)
    purpose = manifest_graph.value(subject=test_uri, predicate=TESTDEC.purpose, object=None)
    expected_output = bool(manifest_graph.value(subject=test_uri, predicate=RDB2RDFTEST.hasExpectedOutput, object=None))
    r2rml = manifest_graph.value(subject=test_uri, predicate=RDB2RDFTEST.mappingDocument, object=None)
    database_uri = manifest_graph.value(subject=test_uri, predicate=RDB2RDFTEST.database, object=None)
    database = manifest_graph.value(subject=database_uri, predicate=RDB2RDFTEST.sqlScriptFile, object=None)
    print("Testing R2RML test-case: " + identifier + " (" + t_title + ")")
    print("Purpose of this test is: " + purpose)
    database_load(database)
    run_test(identifier, r2rml, test_uri, expected_output)


def database_up():
    if database_system == "mysql":
        os.system("docker-compose -f databases/docker-compose-mysql.yml stop")
        os.system("docker-compose -f databases/docker-compose-mysql.yml rm --force")
        os.system("docker-compose -f databases/docker-compose-mysql.yml up -d && sleep 30")
    elif database_system == "postgresql":
        os.system("docker-compose -f databases/docker-compose-postgresql.yml stop")
        os.system("docker-compose -f databases/docker-compose-postgresql.yml rm --force")
        os.system("docker-compose -f databases/docker-compose-postgresql.yml up -d && sleep 30")


def database_down():
    if database_system == "mysql":
        os.system("docker-compose -f databases/docker-compose-mysql.yml stop")
        os.system("docker-compose -f databases/docker-compose-mysql.yml rm --force")
    elif database_system == "postgresql":
        os.system("docker-compose -f databases/docker-compose-postgresql.yml stop")
        os.system("docker-compose -f databases/docker-compose-postgresql.yml rm --force")


def database_load(database_script):
    print("Loading in " + config["properties"]["database_system"] + " system the file:" + database_script)

    if database_system == "mysql":
        cnx = mysql.connector.connect(user='r2rml', password='r2rml', host='127.0.0.1', database='r2rml')
        cursor = cnx.cursor()
        for statement in open('databases/' + database_script):
            cursor.execute(statement)
        cnx.commit()
        cursor.close()
        cnx.close()

    elif database_system == "postgresql":
        cnx = psycopg2.connect("dbname='r2rml' user='r2rml' host='localhost' password='r2rml'")
        cursor = cnx.cursor()
        if database_script == Literal("d016.sql"):
            database_script = "d016-postgresql.sql"
        for statement in open('databases/' + database_script):
            cursor.execute(statement)
        cnx.commit()
        cursor.close()
        cnx.close()


def run_test(t_identifier, mapping, test_uri, expected_output):
    os.system("cp " + t_identifier + "/" + mapping + " r2rml.ttl")
    expected_output_graph = Graph()
    if os.path.isfile(config["properties"]["output_results"]):
        os.system("rm "+config["properties"]["output_results"])

    if expected_output:
        output = manifest_graph.value(subject=test_uri, predicate=RDB2RDFTEST.output, object=None)
        expected_output_graph.parse("./" + t_identifier + "/" + output, format="nquads")

    os.system(config["properties"]["engine_command"] + " > " + t_identifier + "/engine_output-"+database_system+".log")

    # if there is output file
    if os.path.isfile(config["properties"]["output_results"]):
        os.system("cp " + config["properties"]["output_results"] + " " + t_identifier + "/engine_output-"+database_system+".ttl")
        # and expected output is true
        if expected_output:
            output_graph = Graph()
            iso_expected = compare.to_isomorphic(expected_output_graph)
            # trying to parse the output (e.g., not valid RDF)
            try:
                output_graph.parse(config["properties"]["output_results"],
                                   format=config["properties"]["output_format"])
                iso_output = compare.to_isomorphic(output_graph)
                # and graphs are equal
                if iso_expected == iso_output:
                    result = passed
                # and graphs are distinct
                else:
                    result = failed
            # output is not valid RDF
            except:
                result = failed

        # and expected output is false
        else:
            result = failed
    # if there is not output file
    else:
        # and expected output is true
        if expected_output:
            result = failed
        # expected output is false
        else:
            result = passed

    results.append(
        [config["tester"]["tester_name"], config["engine"]["engine_name"], database_system, t_identifier, result])
    print(t_identifier + "," + result)


def generate_results():
    with open('results.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(results)

    print("Generating the RDF results using EARL vocabulary")
    os.system("java -jar rmlmapper.jar -m mapping.rml.ttl -o results-" + database_system + ".ttl -d")
    os.system("rm metadata.csv r2rml.ttl && mv results.csv results-" + database_system + ".csv")


def merge_results():
    if os.path.isfile("results-mysql.ttl") and os.path.isfile("results-postgresql.ttl"):
        final_results = Graph()
        final_results.parse("results-mysql.ttl", format="ntriples")
        final_results.parse("results-postgresql.ttl", format="ntriples")
        final_results.serialize("results.ttl", format="ntriples")

def get_database_url():
    if database_system == "mysql":
        return "https://www.mysql.com/"
    elif database_system == "postgresql":
        return "https://www.postgresql.org/"
    else:
        print("Database system declared in config file must be mysql or postgresql")
        sys.exit()


if __name__ == "__main__":
    config_file = str(sys.argv[1])
    if not os.path.isfile(config_file):
        print("The configuration file " + config_file + " does not exist.")
        print("Aborting...")
        sys.exit(1)

    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read(config_file)
    database_system = config["properties"]["database_system"]

    manifest_graph = Graph()
    manifest_graph.parse("./manifest.ttl", format='turtle')
    RDB2RDFTEST = Namespace("http://purl.org/NET/rdb2rdf-test#")
    TESTDEC = Namespace("http://www.w3.org/2006/03/test-description#")
    DCELEMENTS = Namespace("http://purl.org/dc/elements/1.1/")

    results = [["tester", "platform", "rdbms", "testid", "result"]]
    metadata = [
        ["tester_name", "tester_url", "tester_contact", "test_date", "engine_version", "engine_name", "engine_created",
         "engine_url", "database", "database_name"],
        [config["tester"]["tester_name"], config["tester"]["tester_url"], config["tester"]["tester_contact"],
         config["engine"]["test_date"],
         config["engine"]["engine_version"], config["engine"]["engine_name"], config["engine"]["engine_created"],
         config["engine"]["engine_url"], get_database_url(), database_system]]
    failed = "failed"
    passed = "passed"
    with open('metadata.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(metadata)

    print("Deployment docker container for " + database_system + "...")
    database_up()

    if config["properties"]["tests"] == "all":
        test_all()
        generate_results()
    else:
        test_one(config["properties"]["tests"])
        generate_results()

    database_down()
    merge_results()
