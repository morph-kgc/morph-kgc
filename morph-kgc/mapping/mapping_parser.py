""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import rdflib
import logging
import sql_metadata
import pandas as pd
import numpy as np
import constants
import utils
import multiprocessing as mp

from mapping.mapping_constants import MAPPINGS_DATAFRAME_COLUMNS, MAPPING_PARSING_QUERY, JOIN_CONDITION_PARSING_QUERY
from mapping.mapping_partitioner import MappingPartitioner
from mapping.mapping_validator import MappingValidator
from data_source import relational_source


def _mapping_to_rml(mapping_graph, section_name):
    """
    Recognizes the mapping language of the rules in a graph. If it is R2RML, the mapping rules are converted to RML.
    """

    r2rml_query = 'SELECT ?s WHERE {?s <http://www.w3.org/ns/r2rml#logicalTable> ?o .} LIMIT 1 '
    # if the query result set is not empty then the mapping language is R2RML
    if len(mapping_graph.query(r2rml_query)) > 0:
        logging.debug("Data source `" + section_name + "` has R2RML rules, converting them to RML.")

        # replace R2RML predicates with the equivalent RML predicates
        mapping_graph = utils.replace_predicates_in_graph(mapping_graph, constants.R2RML_LOGICAL_TABLE,
                                                          constants.RML_LOGICAL_SOURCE)
        mapping_graph = utils.replace_predicates_in_graph(mapping_graph, constants.R2RML_SQL_QUERY,
                                                          constants.RML_QUERY)
        mapping_graph = utils.replace_predicates_in_graph(mapping_graph, constants.R2RML_COLUMN,
                                                          constants.RML_REFERENCE)

    return mapping_graph


def _get_join_object_maps_join_conditions(join_query_results):
    """
    Creates a dictionary with the results of the JOIN_CONDITION_PARSING_QUERY. The keys are the identifiers of the
    child triples maps of the join condition. The values of the dictionary are in turn other dictionaries with two
    items with keys, child_value and parent_value, representing a join condition.
    """

    join_conditions_dict = {}

    for join_condition in join_query_results:
        # add the child triples map identifier if it is not in the dictionary
        if join_condition.object_map not in join_conditions_dict:
            join_conditions_dict[join_condition.object_map] = {}

        # add the new join condition (note that several join conditions can apply in a join)
        join_conditions_dict[join_condition.object_map][str(join_condition.join_condition)] = \
            {'child_value': str(join_condition.child_value), 'parent_value': str(join_condition.parent_value)}

    return join_conditions_dict


def _validate_no_repeated_triples_maps(mapping_graph, source_name):
    """
    Checks that there are no repeated triples maps in the mapping rules of a source. This is important because
    if there are repeated triples maps (i.e. triples map with the same identifier), it is not possible to process
    parent triples maps correctly.
    """

    query = 'SELECT ?triples_map_id WHERE { ?triples_map_id <http://www.w3.org/ns/r2rml#subjectMap> ?_subject_map . }'

    # get the identifiers of all the triples maps in the graph
    triples_map_ids = [str(result.triples_map_id) for result in list(mapping_graph.query(query))]

    # get the identifiers that are repeated
    repeated_triples_map_ids = utils.get_repeated_elements_in_list(triples_map_ids)

    # if there are any repeated identifiers, then it will produce errors during materialization
    if len(repeated_triples_map_ids) > 0:
        raise Exception("The following triples maps in data source `" + source_name + "` are repeated: " +
                        str(repeated_triples_map_ids) + '.')


def _transform_mappings_into_dataframe(mapping_query_results, join_query_results, section_name):
    """
    Builds a Pandas DataFrame from the results obtained from MAPPING_PARSING_QUERY and
    JOIN_CONDITION_PARSING_QUERY for one source.
    """

    # graph mapping rules to DataFrame
    source_mappings_df = pd.DataFrame(mapping_query_results.bindings)
    source_mappings_df.columns = source_mappings_df.columns.map(str)

    # process mapping rules with joins
    # create dict with child triples maps in the keys and its join conditions in in the values
    join_conditions_dict = _get_join_object_maps_join_conditions(join_query_results)
    # map the dict with the join conditions to the mapping rules in the DataFrame
    source_mappings_df['join_conditions'] = source_mappings_df['object_map'].map(join_conditions_dict)
    # needed for later hashing the dataframe
    source_mappings_df['join_conditions'] = source_mappings_df['join_conditions'].where(
        pd.notna(source_mappings_df['join_conditions']), '')
    # convert the join condition dictionaries to string (can later be converted back to dict)
    source_mappings_df['join_conditions'] = source_mappings_df['join_conditions'].astype(str)
    # object_map column no longer needed, remove it
    source_mappings_df = source_mappings_df.drop('object_map', axis=1)

    # link the mapping rules to their data source name
    source_mappings_df['source_name'] = section_name

    return source_mappings_df


def _is_delimited_identifier(identifier):
    """
    Checks if an identifier is delimited or not.
    """

    if len(identifier) > 2:
        if identifier[0] == '"' and identifier[len(identifier) - 1] == '"':
            return True
    return False


def _get_undelimited_identifier(identifier):
    """
    Removes delimiters from the identifier if it is delimited.
    """

    if pd.notna(identifier):
        identifier = str(identifier)
        if _is_delimited_identifier(identifier):
            return identifier[1:-1]
    return identifier


def _get_valid_template_identifiers(template):
    """
    Removes delimiters from delimited identifiers in a template.
    """

    if pd.notna(template):
        return template.replace('{"', '{').replace('"}', '}')
    return template


class MappingParser:

    def __init__(self, config):
        self.mappings_df = pd.DataFrame(columns=MAPPINGS_DATAFRAME_COLUMNS)
        self.config = config

    def __str__(self):
        return str(self.mappings_df)

    def __repr__(self):
        return repr(self.mappings_df)

    def __len__(self):
        return len(self.mappings_df)

    def parse_mappings(self):
        self._get_from_r2_rml()
        self._normalize_mappings()
        # self._remove_self_joins_from_mappings()
        self._infer_datatypes()

        mapping_validator = MappingValidator(self.mappings_df, self.config)
        mapping_validator.validate_mappings()

        logging.info(str(len(self.mappings_df)) + ' mapping rules retrieved.')

        # generate mapping partitions
        mapping_partitioner = MappingPartitioner(self.mappings_df, self.config)
        self.mappings_df = mapping_partitioner.partition_mappings()

        # replace empty strings with NaN
        self.mappings_df = self.mappings_df.replace(r'^\s*$', np.nan, regex=True)

        return self.mappings_df

    def _get_from_r2_rml(self):
        # parse mapping files for each data source in the config file and add the parsed mappings rules to a
        # common DataFrame for all data sources

        data_source_sections = self.config.get_data_sources_sections()

        if self.config.is_multiprocessing_enabled() and self.config.has_multiple_data_sources():
            pool = mp.Pool(self.config.get_number_of_processes())
            mappings_dfs = pool.map(self._parse_data_source_mapping_files, data_source_sections)
            self.mappings_df = pd.concat([self.mappings_df, pd.concat(mappings_dfs)])
        else:
            for section_name in data_source_sections:
                data_source_mappings_df = self._parse_data_source_mapping_files(section_name)
                self.mappings_df = pd.concat([self.mappings_df, data_source_mappings_df])

        self.mappings_df = self.mappings_df.reset_index(drop=True)

    def _parse_data_source_mapping_files(self, section_name):
        """
        Creates a Pandas DataFrame with the mapping rules for a data source. It loads the mapping files in a rdflib
        graph and recognizes the mapping language used. Mapping files serialization is automatically guessed.
        It performs queries MAPPING_PARSING_QUERY and JOIN_CONDITION_PARSING_QUERY and process the results to build a
        DataFrame with the mapping rules. Also verifies that there are not repeated triples maps in the mappings.
        """

        mapping_graph = rdflib.Graph()

        mapping_file_paths = self.config.get_mappings_files(section_name)
        try:
            # load mapping rules to graph
            [mapping_graph.load(f, format=rdflib.util.guess_format(f)) for f in mapping_file_paths]
        except Exception as n3_mapping_parse_exception:
            raise Exception(n3_mapping_parse_exception)

        # before further processing, convert R2RML rules to RML, so that we can assume RML for parsing
        mapping_graph = _mapping_to_rml(mapping_graph, section_name)

        # parse the mappings with the parsing query
        mapping_query_results = mapping_graph.query(MAPPING_PARSING_QUERY)
        join_query_results = mapping_graph.query(JOIN_CONDITION_PARSING_QUERY)

        # check triples maps are not repeated, which would lead to errors (because of repeated triples maps identifiers)
        _validate_no_repeated_triples_maps(mapping_graph, section_name)

        # convert the SPARQL result set with the parsed mappings to DataFrame
        return _transform_mappings_into_dataframe(mapping_query_results, join_query_results, section_name)

    def _normalize_mappings(self):
        # start by removing duplicated triples
        self.mappings_df = self.mappings_df.drop_duplicates()
        # complete source type with reference formulation
        self._complete_source_types()
        # convert rr:class to new POMs
        self._rdf_class_to_pom()
        # normalizes graphs terms in the mappings
        self._process_pom_graphs()
        # if a term as no associated rr:termType, complete it as indicated in R2RML specification
        self._complete_termtypes()

        # ignore the delimited identifiers (this is not conformant with R2MRL specification)
        self._remove_delimiters_from_mappings()

        # remove mapping rules with no predicate or object (subject map is conserved because rdf class was added as POM)
        self.mappings_df = self.mappings_df.dropna(subset=['predicate_constant', 'predicate_template',
                                                           'predicate_reference', 'object_constant', 'object_template',
                                                           'object_reference'], how='all')

        # create a unique id for each mapping rule
        self.mappings_df.insert(0, 'id', self.mappings_df.reset_index(drop=True).index)

    def _remove_self_joins_from_mappings(self):
        if not self.config.remove_self_joins():
            return

        for i, mapping_rule in self.mappings_df.iterrows():
            if pd.notna(mapping_rule['object_parent_triples_map']):
                parent_triples_map_rule = utils.get_mapping_rule_from_triples_map_id(self.mappings_df, mapping_rule[
                    'object_parent_triples_map'])

                # str() is to be able to compare np.nan
                if str(mapping_rule['source_name']) == str(parent_triples_map_rule['source_name']) and \
                        str(mapping_rule['data_source']) == str(parent_triples_map_rule['data_source']) and \
                        str(mapping_rule['tablename']) == str(parent_triples_map_rule['tablename']) and \
                        str(mapping_rule['iterator']) == str(parent_triples_map_rule['iterator']) and \
                        str(mapping_rule['query']) == str(parent_triples_map_rule['query']):

                    remove_join = True

                    # check that all conditions in the join condition have the same references
                    join_conditions = eval(mapping_rule['join_conditions'])
                    for key, join_condition in join_conditions.items():
                        if join_condition['child_value'] != join_condition['parent_value']:
                            remove_join = False

                    if remove_join:
                        logging.debug('Removing join from mapping rule `' + str(mapping_rule['id']) + '`.')

                        self.mappings_df.at[i, 'object_parent_triples_map'] = ''
                        self.mappings_df.at[i, 'join_conditions'] = ''
                        self.mappings_df.at[i, 'object_constant'] = parent_triples_map_rule.at['subject_constant']
                        self.mappings_df.at[i, 'object_template'] = parent_triples_map_rule.at['subject_template']
                        self.mappings_df.at[i, 'object_reference'] = parent_triples_map_rule.at['subject_reference']
                        self.mappings_df.at[i, 'object_termtype'] = parent_triples_map_rule.at['subject_termtype']

    def _rdf_class_to_pom(self):
        """
        Transforms rr:class properties (subject_rdf_class column in the input DataFrame) into POMs. The new mapping
        rules corresponding to rr:class properties are added to the input DataFrame and subject_rdf_class column is
        removed.
        """

        # make a copy of the parsed mappings
        initial_mapping_df = self.mappings_df.copy()

        # iterate over the mapping rules
        for i, row in initial_mapping_df.iterrows():
            # if a mapping rules has rr:class, generate a new POM to generate triples for this graph
            if pd.notna(row['subject_rdf_class']):
                # get the position of the new POM in the DataFrame
                j = len(self.mappings_df)

                # build the new POM from the mapping rule
                self.mappings_df.at[j, 'source_name'] = row['source_name']
                self.mappings_df.at[j, 'triples_map_id'] = row['triples_map_id']
                self.mappings_df.at[j, 'tablename'] = row['tablename']
                self.mappings_df.at[j, 'query'] = row['query']
                self.mappings_df.at[j, 'subject_template'] = row['subject_template']
                self.mappings_df.at[j, 'subject_reference'] = row['subject_reference']
                self.mappings_df.at[j, 'subject_constant'] = row['subject_constant']
                self.mappings_df.at[j, 'graph_constant'] = row['graph_constant']
                self.mappings_df.at[j, 'graph_reference'] = row['graph_reference']
                self.mappings_df.at[j, 'graph_template'] = row['graph_template']
                self.mappings_df.at[j, 'subject_termtype'] = row['subject_termtype']
                self.mappings_df.at[j, 'predicate_constant'] = constants.RDF_TYPE
                self.mappings_df.at[j, 'object_constant'] = row['subject_rdf_class']
                self.mappings_df.at[j, 'object_termtype'] = constants.R2RML_IRI
                self.mappings_df.at[j, 'join_conditions'] = ''

        # subject_rdf_class column no longer needed, remove it
        self.mappings_df = self.mappings_df.drop('subject_rdf_class', axis=1)
        # ensure that we do not generate duplicated mapping rules
        self.mappings_df = self.mappings_df.drop_duplicates()

    def _process_pom_graphs(self):
        """
        Completes mapping rules in the input DataFrame with rr:defaultGraph if any graph term is provided for that
        mapping rule (as indicated in R2RML specification
        (https://www.w3.org/2001/sw/rdb2rdf/r2rml/#generated-triples)). Also simplifies the DataFrame unifying graph
        terms in one column (graph_constant, graph_template, graph_reference).
        """

        # use rr:defaultGraph for those mapping rules that do not have any graph term
        for i, mapping_rule in self.mappings_df.iterrows():
            # check the POM has no associated graph term
            if pd.isna(mapping_rule['graph_constant']) and pd.isna(mapping_rule['graph_reference']) and \
                    pd.isna(mapping_rule['graph_template']):
                if pd.isna(mapping_rule['predicate_object_graph_constant']) and \
                        pd.isna(mapping_rule['predicate_object_graph_reference']) and \
                        pd.isna(mapping_rule['predicate_object_graph_template']):
                    # no graph term for this POM, assign rr:defaultGraph
                    self.mappings_df.at[i, 'graph_constant'] = constants.R2RML_DEFAULT_GRAPH

        # instead of having two columns for graph terms (one for subject maps, i.e. graph_constant, and other for POMs,
        # i.e. predicate_object_graph_constant), keep only one for simplicity. In order
        # to have only one column, append POM graph terms as new mapping rules in the DataFrame.
        for i, mapping_rule in self.mappings_df.copy().iterrows():
            if pd.notna(mapping_rule['predicate_object_graph_constant']):
                # position of the new rule in the DataFrame
                j = len(self.mappings_df)
                # copy (duplicate) the mapping rule
                self.mappings_df.loc[j] = mapping_rule
                # update the graph term (with the POM graph term) of the new mapping rule
                self.mappings_df.at[j, 'graph_constant'] = mapping_rule['predicate_object_graph_constant']
            if pd.notna(mapping_rule['predicate_object_graph_template']):
                j = len(self.mappings_df)
                self.mappings_df.loc[j] = mapping_rule
                self.mappings_df.at[j, 'graph_template'] = mapping_rule['predicate_object_graph_template']
            if pd.notna(mapping_rule['predicate_object_graph_reference']):
                j = len(self.mappings_df)
                self.mappings_df.loc[j] = mapping_rule
                self.mappings_df.at[j, 'graph_reference'] = mapping_rule['predicate_object_graph_reference']

        # remove POM graph columns
        self.mappings_df = self.mappings_df.drop(
            columns=['predicate_object_graph_constant', 'predicate_object_graph_template',
                     'predicate_object_graph_reference'])
        # Drop where graph_constant, graph_template and graph_reference are null. This is because it can happen that
        # original mapping rules have graph term for subject maps but not for POM, and if this happens, the newly
        # appended mapping rule applies, but the the old one (without graph term in the subject map) must not be
        # considered.
        self.mappings_df = self.mappings_df.dropna(subset=['graph_constant', 'graph_template', 'graph_reference'],
                                                   how='all')
        # ensure that we do not introduce duplicate mapping rules
        self.mappings_df = self.mappings_df.drop_duplicates()

    def _complete_termtypes(self):
        """
        Completes term types of mapping rules that do not have rr:termType property as indicated in R2RML specification
        (https://www.w3.org/2001/sw/rdb2rdf/r2rml/#termtype).
        """

        for i, mapping_rule in self.mappings_df.iterrows():
            # if subject termtype is missing, then subject termtype is rr:IRI
            if pd.isna(mapping_rule['subject_termtype']):
                self.mappings_df.at[i, 'subject_termtype'] = constants.R2RML_IRI

            if pd.isna(mapping_rule['object_termtype']):
                # if object termtype is missing and there is a language tag or datatype or object term is a
                # reference, then termtype is rr:Literal
                if pd.notna(mapping_rule['object_language']) or pd.notna(mapping_rule['object_datatype']) or \
                        pd.notna(mapping_rule['object_reference']):
                    self.mappings_df.at[i, 'object_termtype'] = constants.R2RML_LITERAL

                else:
                    # if previous conditions (language tag, datatype or reference) do not hold, then termtype is rr:IRI
                    self.mappings_df.at[i, 'object_termtype'] = constants.R2RML_IRI

        # convert to str (instead of rdflib object) to avoid problems later
        self.mappings_df['subject_termtype'] = self.mappings_df['subject_termtype'].astype(str)
        self.mappings_df['object_termtype'] = self.mappings_df['object_termtype'].astype(str)

    def _complete_source_types(self):
        # we want to track the type of data source (RDB, CSV, EXCEL, JSON, etc) in the parsed mapping rules
        for i, mapping_rule in self.mappings_df.iterrows():
            if self.config.has_source_type(mapping_rule['source_name']):
                self.mappings_df.at[i, 'source_type'] = self.config.get_source_type(mapping_rule['source_name'])
            elif pd.notna(mapping_rule['ref_form']):
                self.mappings_df.at[i, 'source_type'] = str(mapping_rule['ref_form']).split('#')[1]
            else:
                logging.error('No source type could be retrieved for mapping rule `' + mapping_rule['id'] + '`.')

        # ref form is no longer needed, remove it
        self.mappings_df = self.mappings_df.drop('ref_form', axis=1)

    def _remove_delimiters_from_mappings(self):
        """
        Removes delimiters from all identifiers in the mapping rules in the input DataFrame.
        """

        for i, mapping_rule in self.mappings_df.iterrows():
            self.mappings_df.at[i, 'tablename'] = _get_undelimited_identifier(mapping_rule['tablename'])
            self.mappings_df.at[i, 'subject_template'] = _get_valid_template_identifiers(
                mapping_rule['subject_template'])
            self.mappings_df.at[i, 'subject_reference'] = _get_undelimited_identifier(
                mapping_rule['subject_reference'])
            self.mappings_df.at[i, 'graph_reference'] = _get_undelimited_identifier(
                mapping_rule['graph_reference'])
            self.mappings_df.at[i, 'graph_template'] = _get_valid_template_identifiers(
                mapping_rule['graph_template'])
            self.mappings_df.at[i, 'predicate_template'] = _get_valid_template_identifiers(
                mapping_rule['predicate_template'])
            self.mappings_df.at[i, 'predicate_reference'] = _get_undelimited_identifier(
                mapping_rule['predicate_reference'])
            self.mappings_df.at[i, 'object_template'] = _get_valid_template_identifiers(
                mapping_rule['object_template'])
            self.mappings_df.at[i, 'object_reference'] = _get_undelimited_identifier(
                mapping_rule['object_reference'])

            # if join_confition is not null and is not empty (so we can evaluate the dictZ)
            if pd.notna(mapping_rule['join_conditions']) and mapping_rule['join_conditions']:
                join_conditions = eval(mapping_rule['join_conditions'])
                for key, value in join_conditions.items():
                    join_conditions[key]['child_value'] = _get_undelimited_identifier(
                        join_conditions[key]['child_value'])
                    join_conditions[key]['parent_value'] = _get_undelimited_identifier(
                        join_conditions[key]['parent_value'])
                    self.mappings_df.at[i, 'join_conditions'] = str(join_conditions)

    def _infer_datatypes(self):
        """
        Get RDF datatypes for rules corresponding to relational data sources if they are not overridden in the mapping
        rules. The inferring of RDF datatypes is defined in R2RML specification
        (https://www.w3.org/2001/sw/rdb2rdf/r2rml/#natural-mapping).
        """

        # return if datatype inferring is not enabled in the config
        if not self.config.infer_sql_datatypes():
            return

        for i, mapping_rule in self.mappings_df.iterrows():
            # datatype inferring only applies to relational data sources
            if (mapping_rule['source_type'] in constants.VALID_RELATIONAL_SOURCE_TYPES) and (
                    # datatype inferring only applies to literals
                    mapping_rule['object_termtype'] == constants.R2RML_LITERAL) and (
                    # if the literal has a language tag or an overridden datatype, datatype inference does not apply
                    pd.isna(mapping_rule['object_datatype']) and pd.isna(mapping_rule['object_language'])):

                if pd.notna(mapping_rule['tablename']) and pd.notna(mapping_rule['object_reference']):
                    inferred_data_type = relational_source.get_column_datatype(
                        self.config, mapping_rule['source_name'], mapping_rule['tablename'],
                        mapping_rule['object_reference']
                    )

                    self.mappings_df.at[i, 'object_datatype'] = inferred_data_type
                    if inferred_data_type:
                        logging.debug("`" + inferred_data_type + "` datatype inferred for column `" +
                                      mapping_rule['object_reference'] + "` of table `" +
                                      mapping_rule['tablename'] + "` in data source `" +
                                      mapping_rule['source_name'] + "`.")

                elif pd.notna(mapping_rule['query']):
                    # if mapping rule has a query, get the table names in the query
                    table_names = sql_metadata.get_query_tables(mapping_rule['query'])
                    for table_name in table_names:
                        # for each table in the query get the datatype of the object reference in that table if an
                        # exception is thrown, then the reference is not a column in that table, and nothing is done
                        try:
                            data_type = relational_source.get_column_datatype(
                                self.config, mapping_rule['source_name'], table_name,
                                mapping_rule['object_reference']
                            )

                            self.mappings_df.at[i, 'object_datatype'] = data_type
                            if data_type:
                                logging.debug("`" + data_type + "` datatype inferred for reference `" +
                                              mapping_rule['object_reference'] + "` in query [" +
                                              mapping_rule['query'] + "] in data source `" +
                                              mapping_rule['source_name'] + "`.")

                            # already found it, end looping
                            break
                        except:
                            pass
