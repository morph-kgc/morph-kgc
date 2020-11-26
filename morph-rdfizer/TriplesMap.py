"""
Code in this module has been reused from SDM-RDFizer (https://github.com/SDM-TIB/SDM-RDFizer).
SDM-RDFizer has been developed by members of the Scientific Data Management Group at TIB.
Its development has been coordinated and supervised by Maria-Esther Vidal. The implementation has been done
by Enrique Iglesias and Guillermo Betancourt under the supervision of David Chaves-Fraga, Samaneh Jozashoori,
and Kemele Endris.

Parts of the code have been modified and/or extended by the Ontology Engineering Group from Universidad Politécnica
de Madrid (UPM).
"""

import re, rdflib
import pandas as pd


class TriplesMap:

    def __init__(self, triples_map_id, data_source, subject_map, predicate_object_maps_list, ref_form=None,
                 iterator=None, tablename=None, query=None):

        """
        Constructor of a TriplesMap object

        Parameters
        ----------
        triples_map_id : string
            URI containing the triples-map indentification
        data_source : string
            URI containing the path to the data source
        subject_map : SubjectMap object
            SubjectMap object containing the specifications of the subject
        predicate_object_maps_list : list of PredicateObjectMap objects
            List containing the PredicateObjectMap objects associated with the SubjectMap object
        ref_from : string
            URI containing the data source reference formulation

        """

        self.triples_map_id = triples_map_id
        self.triples_map_name = re.compile("((.*?))$").search(str(self.triples_map_id)).group(0)
        self.data_source = data_source[7:] if data_source[:7] == "file://" else data_source
        self.reference_formulation = ref_form
        if self.reference_formulation != "None":
            self.file_format = re.compile("(#[A-Za-z]+)$").search(str(self.reference_formulation)).group(0)[1:]
        else:
            self.file_format = None
        self.iterator = iterator
        self.tablename = tablename
        self.query = query

        if subject_map is not None:
            self.subject_map = subject_map
        else:
            print("Subject map cannot be empty")
            print("Aborting...")
            exit(1)

        self.predicate_object_maps_list = predicate_object_maps_list

    def __repr__(self):

        """
        Proper string representation for the TriplesMap objects

        Returns
        -------
        Returns a string containing a human-readable representation for the TriplesMap objects
        """

        value = "triples map id: {}\n".format(self.triples_map_name)
        value += "\tlogical source: {}\n".format(self.data_source)
        value += "\treference formulation: {}\n".format(self.reference_formulation)
        value += "\titerator: {}\n".format(self.iterator)
        value += "\tsubject map: {}\n".format(self.subject_map.value)

        for predicate_object_map in self.predicate_object_maps_list:
            value += "\t\tpredicate: {} - mapping type: {}\n".format(
                predicate_object_map.predicate_map.value, predicate_object_map.predicate_map.mapping_type)
            value += "\t\tobject: {} - mapping type: {} - datatype: {}\n\n".format(
                predicate_object_map.object_map.value, predicate_object_map.object_map.mapping_type,
                str(predicate_object_map.object_map.datatype))
            if predicate_object_map.object_map.mapping_type == "parent triples map":
                value += "\t\t\tjoin condition: - child: {} - parent: {} \n\n\n".format(
                    predicate_object_map.object_map.child, predicate_object_map.object_map.parent)

        return value + "\n"


class SubjectMap:

    def __init__(self, subject_value, condition, subject_mapping_type, rdf_class=None, term_type=None, graph=None):
        """
        Constructor of a SubjectMap object

        Parameters
        ----------
        subject_value : string
            URI containing the subject
        rdf_class : string (optional)
            URI containing the class of the subject

        """

        self.value = subject_value
        self.condition = condition
        self.rdf_class = rdf_class
        self.term_type = term_type
        self.subject_mapping_type = subject_mapping_type
        self.graph = graph


class PredicateObjectMap:

    def __init__(self, predicate_map, object_map, graph):
        """
        Constructor of a PredicateObjectMap object

        Parameters
        ----------
        predicate_map : PredicateMap object
            Object representing a predicate-map
        object_map : ObjectMap object
            Object representing a object-map

        """

        self.predicate_map = predicate_map
        self.object_map = object_map
        self.graph = graph


class PredicateMap:

    def __init__(self, predicate_mapping_type, predicate_value, predicate_condition):
        """
        Constructor of a PredicateMap object

        Parameters
        ----------
        predicate_mapping_type : string
            String containing the type of predicate-map ("constant", "constant shortcut",
            "template" or "reference")
        predicate_value : string
            URI containi

        """

        self.value = predicate_value
        self.mapping_type = predicate_mapping_type
        self.condition = predicate_condition


class ObjectMap:

    def __init__(self, object_mapping_type, object_value, object_datatype, object_child, object_parent, term, language):
        """
        Constructor of ObjectMap object

        Parameters
        ----------
        predicate_map : PredicateMap object
            Object representing a predicate-map
        object_map : ObjectMap object
            Object representing a object-map

        """

        self.value = object_value
        self.datatype = object_datatype if object_datatype != "None" else None
        self.mapping_type = object_mapping_type
        self.child = object_child if "None" not in object_child else None
        self.parent = object_parent if "None" not in object_parent else None
        self.term = term if term != "None" else None
        self.language = language if language != "None" else None


MAPPING_PARSING_QUERY = """
    prefix rr: <http://www.w3.org/ns/r2rml#> 
    prefix rml: <http://semweb.mmlab.be/ns/rml#> 
    prefix ql: <http://semweb.mmlab.be/ns/ql#> 
    prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> 

    SELECT DISTINCT 
        ?triples_map_id ?data_source ?ref_form ?iterator ?tablename ?query
        ?jdbcDSN ?jdbcDriver ?user ?password
        ?subject_template ?subject_reference ?subject_constant ?subject_rdf_class ?subject_termtype ?graph
        ?predicate_constant ?predicate_template ?predicate_reference ?predicate_constant_shortcut
        ?object_constant ?object_template ?object_reference ?object_termtype ?object_datatype ?object_language
        ?object_parent_triples_map ?join_condition ?child_value ?parent_value ?object_constant_shortcut
        ?predicate_object_graph
        
    WHERE {
        ?triples_map_id rml:logicalSource ?_source .
        OPTIONAL { ?_source rml:source ?data_source . }
        OPTIONAL { ?_source rml:referenceFormulation ?ref_form . }
        OPTIONAL { ?_source rml:iterator ?iterator . }
        OPTIONAL { ?_source rr:tableName ?tablename . }
        OPTIONAL { ?_source rml:query ?query . }
        OPTIONAL {
            ?_source a d2rq:Database ;
            d2rq:jdbcDSN ?jdbcDSN ;
            d2rq:jdbcDriver ?jdbcDriver ;
            d2rq:username ?user ;
            d2rq:password ?password .
        }

# Subject -------------------------------------------------------------------------
        ?triples_map_id rr:subjectMap ?_subject_map .
        OPTIONAL { ?_subject_map rr:template ?subject_template . }
        OPTIONAL { ?_subject_map rml:reference ?subject_reference . }
        OPTIONAL { ?_subject_map rr:constant ?subject_constant . }
        OPTIONAL { ?_subject_map rr:class ?subject_rdf_class . }
        OPTIONAL { ?_subject_map rr:termType ?subject_termtype . }
        OPTIONAL { ?_subject_map rr:graph ?graph . }
        OPTIONAL {
            ?_subject_map rr:graphMap ?_graph_structure .
            ?_graph_structure rr:constant ?graph .
        }
        OPTIONAL {
            ?_subject_map rr:graphMap ?_graph_structure .
            ?_graph_structure rr:template ?graph .
        }

# Predicate -----------------------------------------------------------------------
        OPTIONAL {
            ?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
            OPTIONAL {
                ?_predicate_object_map rr:predicateMap ?_predicate_map .
                ?_predicate_map rr:constant ?predicate_constant .
            }
            OPTIONAL {
                ?_predicate_object_map rr:predicateMap ?_predicate_map .
                ?_predicate_map rr:template ?predicate_template .
            }
            OPTIONAL {
                ?_predicate_object_map rr:predicateMap ?_predicate_map .
                ?_predicate_map rml:reference ?predicate_reference .
            }
            OPTIONAL { ?_predicate_object_map rr:predicate ?predicate_constant_shortcut . }

# Object --------------------------------------------------------------------------
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?_object_map .
                ?_object_map rr:constant ?object_constant .
                OPTIONAL { ?_object_map rr:termType ?object_termtype . }
                OPTIONAL { ?_object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?_object_map rr:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?_object_map .
                ?_object_map rr:template ?object_template .
                OPTIONAL { ?_object_map rr:termType ?object_termtype . }
                OPTIONAL { ?_object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?_object_map rr:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?_object_map .
                ?_object_map rml:reference ?object_reference .
                OPTIONAL { ?_object_map rr:termType ?object_termtype . }
                OPTIONAL { ?_object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?_object_map rr:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?_object_map .
                ?_object_map rr:parentTriplesMap ?object_parent_triples_map .
                OPTIONAL {
                    ?_object_map rr:joinCondition ?join_condition .
                    ?join_condition rr:child ?child_value;
                                 rr:parent ?parent_value.
                    OPTIONAL { ?_object_map rr:termType ?object_termtype . }
                }
            }
            OPTIONAL {
                ?_predicate_object_map rr:object ?object_constant_shortcut .
                OPTIONAL { ?_object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?_object_map rr:language ?object_language . }
            }
            OPTIONAL { ?_predicate_object_map rr:graph ?predicate_object_graph . }
            OPTIONAL {
                ?_predicate_object_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:constant ?predicate_object_graph .
            }
            OPTIONAL {
                ?_predicate_object_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:template ?predicate_object_graph .
            }
        }
    }
"""


def parse_rml_mapping_file(mapping_file):
    mapping_graph = rdflib.Graph()

    try:
        '''
            TO DO: guess mapping file format
        '''
        mapping_graph.load(mapping_file, format='n3')
    except Exception as n3_mapping_parse_exception:
        raise Exception(n3_mapping_parse_exception)

    mapping_query_results = mapping_graph.query(MAPPING_PARSING_QUERY)
    mappings_df = _transform_mappings_into_dataframe(mapping_query_results)

    triples_map_list = []

    for result_triples_map in mapping_query_results:
        triples_map_exists = False
        for triples_map in triples_map_list:
            triples_map_exists = triples_map_exists or (
                    str(triples_map.triples_map_id) == str(result_triples_map.triples_map_id))

'''

        if not triples_map_exists:
            if result_triples_map.subject_template is not None:
                if result_triples_map.rdf_class is None:
                    reference, condition = string_separetion(str(result_triples_map.subject_template))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template",
                                                [result_triples_map.rdf_class], result_triples_map.termtype,
                                                [result_triples_map.graph])
                else:
                    reference, condition = string_separetion(str(result_triples_map.subject_template))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template",
                                                [str(result_triples_map.rdf_class)], result_triples_map.termtype,
                                                [result_triples_map.graph])
            elif result_triples_map.subject_reference is not None:
                if result_triples_map.rdf_class is None:
                    reference, condition = string_separetion(str(result_triples_map.subject_reference))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference",
                                                [result_triples_map.rdf_class], result_triples_map.termtype,
                                                [result_triples_map.graph])
                else:
                    reference, condition = string_separetion(str(result_triples_map.subject_reference))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference",
                                                [str(result_triples_map.rdf_class)], result_triples_map.termtype,
                                                [result_triples_map.graph])
            elif result_triples_map.subject_constant is not None:
                if result_triples_map.rdf_class is None:
                    reference, condition = string_separetion(str(result_triples_map.subject_constant))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant",
                                                [result_triples_map.rdf_class], result_triples_map.termtype,
                                                [result_triples_map.graph])
                else:
                    reference, condition = string_separetion(str(result_triples_map.subject_constant))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant",
                                                [str(result_triples_map.rdf_class)], result_triples_map.termtype,
                                                [result_triples_map.graph])

            mapping_query_prepared = prepareQuery(mapping_query)

            mapping_query_prepared_results = mapping_graph.query(mapping_query_prepared, initBindings={
                'triples_map_id': result_triples_map.triples_map_id})

            join_predicate = {}
            predicate_object_maps_list = []
            predicate_object_graph = {}
            for result_predicate_object_map in mapping_query_prepared_results:
                join = True
                if result_predicate_object_map.predicate_constant is not None:
                    predicate_map = tm.PredicateMap("constant", str(result_predicate_object_map.predicate_constant), "")
                    predicate_object_graph[
                        str(result_predicate_object_map.predicate_constant)] = result_triples_map.predicate_object_graph
                elif result_predicate_object_map.predicate_constant_shortcut is not None:
                    predicate_map = tm.PredicateMap("constant shortcut",
                                                    str(result_predicate_object_map.predicate_constant_shortcut), "")
                    predicate_object_graph[str(
                        result_predicate_object_map.predicate_constant_shortcut)] = result_triples_map.predicate_object_graph
                elif result_predicate_object_map.predicate_template is not None:
                    template, condition = string_separetion(str(result_predicate_object_map.predicate_template))
                    predicate_map = tm.PredicateMap("template", template, condition)
                elif result_predicate_object_map.predicate_reference is not None:
                    reference, condition = string_separetion(str(result_predicate_object_map.predicate_reference))
                    predicate_map = tm.PredicateMap("reference", reference, condition)
                else:
                    predicate_map = tm.PredicateMap("None", "None", "None")

                if result_predicate_object_map.object_constant is not None:
                    object_map = tm.ObjectMap("constant", str(result_predicate_object_map.object_constant),
                                              str(result_predicate_object_map.object_datatype), "None", "None",
                                              result_predicate_object_map.term, result_predicate_object_map.language)
                elif result_predicate_object_map.object_template is not None:
                    object_map = tm.ObjectMap("template", str(result_predicate_object_map.object_template),
                                              str(result_predicate_object_map.object_datatype), "None", "None",
                                              result_predicate_object_map.term, result_predicate_object_map.language)
                elif result_predicate_object_map.object_reference is not None:
                    object_map = tm.ObjectMap("reference", str(result_predicate_object_map.object_reference),
                                              str(result_predicate_object_map.object_datatype), "None", "None",
                                              result_predicate_object_map.term, result_predicate_object_map.language)
                elif result_predicate_object_map.object_parent_triples_map is not None:
                    if predicate_map.value not in join_predicate:
                        join_predicate[predicate_map.value] = {"predicate": predicate_map,
                                                               "childs": [str(result_predicate_object_map.child_value)],
                                                               "parents": [
                                                                   str(result_predicate_object_map.parent_value)],
                                                               "triples_map": str(
                                                                   result_predicate_object_map.object_parent_triples_map)}
                    else:
                        join_predicate[predicate_map.value]["childs"].append(
                            str(result_predicate_object_map.child_value))
                        join_predicate[predicate_map.value]["parents"].append(
                            str(result_predicate_object_map.parent_value))
                    join = False
                elif result_predicate_object_map.object_constant_shortcut is not None:
                    object_map = tm.ObjectMap("constant shortcut",
                                              str(result_predicate_object_map.object_constant_shortcut),
                                              str(result_predicate_object_map.object_datatype), "None", "None",
                                              result_predicate_object_map.term, result_predicate_object_map.language)
                else:
                    object_map = tm.ObjectMap("None", "None", "None", "None", "None", "None", "None")
                if join:
                    predicate_object_maps_list += [
                        tm.PredicateObjectMap(predicate_map, object_map, predicate_object_graph)]
                join = True
            if join_predicate:
                for jp in join_predicate.keys():
                    object_map = tm.ObjectMap("parent triples map", join_predicate[jp]["triples_map"],
                                              str(result_predicate_object_map.object_datatype),
                                              join_predicate[jp]["childs"], join_predicate[jp]["parents"],
                                              result_predicate_object_map.term, result_predicate_object_map.language)
                    predicate_object_maps_list += [
                        tm.PredicateObjectMap(join_predicate[jp]["predicate"], object_map, predicate_object_graph)]

            current_triples_map = tm.TriplesMap(str(result_triples_map.triples_map_id),
                                                str(result_triples_map.data_source), subject_map,
                                                predicate_object_maps_list, ref_form=str(result_triples_map.ref_form),
                                                iterator=str(result_triples_map.iterator),
                                                tablename=str(result_triples_map.tablename),
                                                query=str(result_triples_map.query))
            triples_map_list += [current_triples_map]

        else:
            for triples_map in triples_map_list:
                if str(triples_map.triples_map_id) == str(result_triples_map.triples_map_id):
                    if result_triples_map.rdf_class not in triples_map.subject_map.rdf_class:
                        triples_map.subject_map.rdf_class.append(result_triples_map.rdf_class)
                    if result_triples_map.graph not in triples_map.subject_map.graph:
                        triples_map.graph.append(result_triples_map.graph)
    
    return triples_map_list
'''


def _transform_mappings_into_dataframe(mapping_query_results):
    '''
    Transforms the result from a SPARQL query in rdflib to a DataFrame.

    :param mapping_query_results:
    :return:
    '''

    mappings_df = pd.DataFrame(columns=[
        'triples_map_id', 'data_source', 'ref_form', 'iterator', 'tablename', 'query', 'jdbcDSN', 'jdbcDriver', 'user',
        'password', 'subject_template', 'subject_reference', 'subject_constant', 'subject_rdf_class',
        'subject_termtype', 'graph', 'predicate_constant', 'predicate_template', 'predicate_reference',
        'predicate_constant_shortcut', 'object_constant', 'object_template', 'object_reference', 'object_termtype',
        'object_datatype', 'object_language', 'object_parent_triples_map', 'join_condition', 'child_value',
        'parent_value', 'object_constant_shortcut', 'predicate_object_graph'
    ])

    for mapping_rule in mapping_query_results:
        _append_mapping_rule(mappings_df, mapping_rule)

    return mappings_df


def _append_mapping_rule(mappings_df, mapping_rule):
    i = len(mappings_df)

    mappings_df.at[i, 'triples_map_id'] = mapping_rule.triples_map_id
    mappings_df.at[i, 'data_source'] = mapping_rule.data_source
    mappings_df.at[i, 'ref_form'] = mapping_rule.ref_form
    mappings_df.at[i, 'iterator'] = mapping_rule.iterator
    mappings_df.at[i, 'tablename'] = mapping_rule.tablename
    mappings_df.at[i, 'query'] = mapping_rule.query
    mappings_df.at[i, 'jdbcDSN'] = mapping_rule.jdbcDSN
    mappings_df.at[i, 'jdbcDriver'] = mapping_rule.jdbcDriver
    mappings_df.at[i, 'user'] = mapping_rule.user
    mappings_df.at[i, 'password'] = mapping_rule.password
    mappings_df.at[i, 'subject_template'] = mapping_rule.subject_template
    mappings_df.at[i, 'subject_reference'] = mapping_rule.subject_reference
    mappings_df.at[i, 'subject_constant'] = mapping_rule.subject_constant
    mappings_df.at[i, 'subject_rdf_class'] = mapping_rule.subject_rdf_class
    mappings_df.at[i, 'subject_termtype'] = mapping_rule.subject_termtype
    mappings_df.at[i, 'graph'] = mapping_rule.graph
    mappings_df.at[i, 'predicate_constant'] = mapping_rule.predicate_constant
    mappings_df.at[i, 'predicate_template'] = mapping_rule.predicate_template
    mappings_df.at[i, 'predicate_reference'] = mapping_rule.predicate_reference
    mappings_df.at[i, 'predicate_constant_shortcut'] = mapping_rule.predicate_constant_shortcut
    mappings_df.at[i, 'object_constant'] = mapping_rule.object_constant
    mappings_df.at[i, 'object_template'] = mapping_rule.object_template
    mappings_df.at[i, 'object_reference'] = mapping_rule.object_reference
    mappings_df.at[i, 'object_termtype'] = mapping_rule.object_termtype
    mappings_df.at[i, 'object_datatype'] = mapping_rule.object_datatype
    mappings_df.at[i, 'object_language'] = mapping_rule.object_language
    mappings_df.at[i, 'object_parent_triples_map'] = mapping_rule.object_parent_triples_map
    mappings_df.at[i, 'join_condition'] = mapping_rule.join_condition
    mappings_df.at[i, 'child_value'] = mapping_rule.child_value
    mappings_df.at[i, 'parent_value'] = mapping_rule.parent_value
    mappings_df.at[i, 'object_constant_shortcut'] = mapping_rule.object_constant_shortcut
    mappings_df.at[i, 'predicate_object_graph'] = mapping_rule.predicate_object_graph
