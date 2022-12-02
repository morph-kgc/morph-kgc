__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import sys

from ..constants import *
from ..utils import *
from ..mapping.mapping_constants import MAPPINGS_DATAFRAME_COLUMNS, MAPPING_PARSING_QUERY, JOIN_CONDITION_PARSING_QUERY
from ..mapping.mapping_partitioner import MappingPartitioner
from ..data_source.relational_database import get_rdb_reference_datatype


def retrieve_mappings(config):
    if config.is_read_parsed_mappings_file_provided():
        # retrieve parsed mapping from file and finish mapping processing
        mappings = pd.read_csv(config.get_parsed_mappings_read_path())
        logging.info(f'{len(mappings)} mappings rules loaded from file.')
    else:
        mappings_parser = MappingParser(config)

        start_time = time.time()
        mappings = mappings_parser.parse_mappings()
        logging.info(f'Mappings processed in {get_delta_time(start_time)} seconds.')

    if config.is_write_parsed_mappings_file_provided():
        mappings.sort_values(by=['id'], axis=0).to_csv(config.get_parsed_mappings_write_path(), index=False)
        logging.info('Parsed mapping rules saved to file.')
        sys.exit()

    return mappings


def _mapping_to_rml_star(mapping_graph):
    """
    Replaces R2RML rules in the graph with the corresponding RML rules.
    """

    # add namespaces
    mapping_graph.bind('rr', rdflib.term.URIRef(RML_NAMESPACE))
    mapping_graph.bind('rml', rdflib.term.URIRef(RML_NAMESPACE))
    mapping_graph.bind('ql', rdflib.term.URIRef(QL_NAMESPACE))

    # add reference formulation and sql version for RDB sources
    query = f'SELECT ?logical_source ?x WHERE {{ ?logical_source <{R2RML_TABLE_NAME}> ?x . }} '
    for logical_source, _ in mapping_graph.query(query):
        mapping_graph.add((logical_source, rdflib.term.URIRef(R2RML_SQL_VERSION), rdflib.term.URIRef(R2RML_SQL2008)))
    query = f'SELECT ?logical_source ?x WHERE {{ ?logical_source <{R2RML_SQL_QUERY}> ?x . }} '
    for logical_source, _ in mapping_graph.query(query):
        mapping_graph.add((logical_source, rdflib.term.URIRef(R2RML_SQL_VERSION), rdflib.term.URIRef(R2RML_SQL2008)))
        mapping_graph.add((logical_source, rdflib.term.URIRef(RML_REFERENCE_FORMULATION), rdflib.term.URIRef(QL_CSV)))

    # replace R2RML properties with the equivalent RML properties
    mapping_graph = replace_predicates_in_graph(mapping_graph, R2RML_LOGICAL_TABLE, RML_LOGICAL_SOURCE)
    mapping_graph = replace_predicates_in_graph(mapping_graph, R2RML_SQL_QUERY, RML_QUERY)
    mapping_graph = replace_predicates_in_graph(mapping_graph, R2RML_COLUMN, RML_REFERENCE)

    # remove R2RML classes
    mapping_graph.remove((None, rdflib.term.URIRef(R2RML_R2RML_VIEW_CLASS), None))
    mapping_graph.remove((None, rdflib.term.URIRef(R2RML_LOGICAL_TABLE_CLASS), None))

    # replace RML properties with the equivalent RML-star properties
    mapping_graph = replace_predicates_in_graph(mapping_graph, R2RML_SUBJECT_MAP, RML_STAR_SUBJECT_MAP)
    mapping_graph = replace_predicates_in_graph(mapping_graph, R2RML_SUBJECT_CONSTANT_SHORTCUT, RML_STAR_SUBJECT_CONSTANT_SHORTCUT)
    mapping_graph = replace_predicates_in_graph(mapping_graph, R2RML_OBJECT_MAP, RML_STAR_OBJECT_MAP)
    mapping_graph = replace_predicates_in_graph(mapping_graph, R2RML_OBJECT_CONSTANT_SHORTCUT, RML_STAR_OBJECT_CONSTANT_SHORTCUT)

    return mapping_graph


def _expand_constant_shortcut_properties(mapping_graph):
    """
    Expand constant shortcut properties rml:subject, rr:predicate, rml:object and rr:graph.
    See R2RML specification (https://www.w3.org/2001/sw/rdb2rdf/r2rml/#constant).
    """

    constant_properties = [RML_STAR_SUBJECT_MAP, R2RML_PREDICATE_MAP, RML_STAR_OBJECT_MAP, R2RML_GRAPH_MAP]
    constant_shortcuts = [RML_STAR_SUBJECT_CONSTANT_SHORTCUT, R2RML_PREDICATE_CONSTANT_SHORTCUT,
                          RML_STAR_OBJECT_CONSTANT_SHORTCUT, R2RML_GRAPH_CONSTANT_SHORTCUT]

    for constant_property, constant_shortcut in zip(constant_properties, constant_shortcuts):
        for s, o in mapping_graph.query(f'SELECT ?s ?o WHERE {{?s <{constant_shortcut}> ?o .}}'):
            blanknode = rdflib.BNode()
            mapping_graph.add((s, rdflib.term.URIRef(constant_property), blanknode))
            mapping_graph.add((blanknode, rdflib.term.URIRef(R2RML_CONSTANT), o))

        mapping_graph.remove((None, rdflib.term.URIRef(constant_shortcut), None))

    return mapping_graph


def _rdf_class_to_pom(mapping_graph):
    """
    Replace rr:class definitions by predicate object maps.
    """

    query = 'SELECT ?tm ?c WHERE { ' \
            f'?tm <{RML_STAR_SUBJECT_MAP}> ?sm . ' \
            f'?sm <{R2RML_CLASS}> ?c . }}'
    for tm, c in mapping_graph.query(query):
        blanknode = rdflib.BNode()
        mapping_graph.add((tm, rdflib.term.URIRef(R2RML_PREDICATE_OBJECT_MAP), blanknode))
        mapping_graph.add((blanknode, rdflib.term.URIRef(R2RML_PREDICATE_CONSTANT_SHORTCUT), rdflib.RDF.type))
        mapping_graph.add((blanknode, rdflib.term.URIRef(RML_STAR_OBJECT_CONSTANT_SHORTCUT), c))

    mapping_graph.remove((None, rdflib.term.URIRef(R2RML_CLASS), None))

    return mapping_graph


def _subject_graph_maps_to_pom(mapping_graph):
    """
    Move graph maps in subject maps to the predicate object maps of subject maps.
    """

    # add the graph maps in the subject maps to every predicate object map of the subject maps
    query = 'SELECT ?sm ?gm ?pom WHERE { ' \
            f'?tm <{RML_STAR_SUBJECT_MAP}> ?sm . ' \
            f'?sm <{R2RML_GRAPH_MAP}> ?gm . ' \
            f'?tm <{R2RML_PREDICATE_OBJECT_MAP}> ?pom . }}'
    for sm, gm, pom in mapping_graph.query(query):
        mapping_graph.add((pom, rdflib.term.URIRef(R2RML_GRAPH_MAP), gm))

    # remove the graph maps from the subject maps
    query = 'SELECT ?sm ?gm WHERE { ' \
            f'?tm <{RML_STAR_SUBJECT_MAP}> ?sm . ' \
            f'?sm <{R2RML_GRAPH_MAP}> ?gm . }}'
    for sm, gm in mapping_graph.query(query):
        mapping_graph.remove((sm, rdflib.term.URIRef(R2RML_GRAPH_MAP), gm))

    return mapping_graph


def _complete_pom_with_default_graph(mapping_graph):
    """
    Complete predicate object maps without graph maps with rr:defaultGraph.
    """

    query = 'SELECT DISTINCT ?tm ?pom WHERE { ' \
            f'?tm <{R2RML_PREDICATE_OBJECT_MAP}> ?pom . ' \
            f'OPTIONAL {{ ?pom <{R2RML_GRAPH_MAP}> ?gm . }} . ' \
            'FILTER ( !bound(?gm) ) }'
    for tm, pom in mapping_graph.query(query):
        blanknode = rdflib.BNode()
        mapping_graph.add((pom, rdflib.term.URIRef(R2RML_GRAPH_MAP), blanknode))
        mapping_graph.add((blanknode, rdflib.term.URIRef(R2RML_CONSTANT), rdflib.term.URIRef(R2RML_DEFAULT_GRAPH)))

    return mapping_graph


def _complete_termtypes(mapping_graph):
    """
    Completes term types of mapping rules that do not have rr:termType property as indicated in R2RML specification
    (https://www.w3.org/2001/sw/rdb2rdf/r2rml/#termtype).
    """

    # add missing RDF-star triples termtypes (in the subject and object maps)
    query = 'SELECT DISTINCT ?term_map ?quoted_triples_map WHERE { ' \
            f'?term_map <{RML_STAR_QUOTED_TRIPLES_MAP}> ?quoted_triples_map . ' \
            f'OPTIONAL {{ ?term_map <{R2RML_TERM_TYPE}> ?termtype . }} . ' \
            'FILTER ( !bound(?termtype) ) }'
    for term_map, _ in mapping_graph.query(query):
        mapping_graph.add((term_map, rdflib.term.URIRef(R2RML_TERM_TYPE), rdflib.term.URIRef(RML_STAR_RDF_STAR_TRIPLE)))

    # add missing blanknode termtypes in the constant-valued object maps
    query = 'SELECT DISTINCT ?term_map ?constant WHERE { ' \
            f'?term_map <{R2RML_CONSTANT}> ?constant . ' \
            f'OPTIONAL {{ ?term_map <{R2RML_TERM_TYPE}> ?termtype . }} . ' \
            'FILTER ( !bound(?termtype) && isBlank(?constant) ) }'
    for term_map, _ in mapping_graph.query(query):
        mapping_graph.add((term_map, rdflib.term.URIRef(R2RML_TERM_TYPE), rdflib.term.URIRef(R2RML_BLANK_NODE)))

    # add missing literal termtypes in the constant-valued object maps
    query = 'SELECT DISTINCT ?term_map ?constant WHERE { ' \
            f'?term_map <{R2RML_CONSTANT}> ?constant . ' \
            f'OPTIONAL {{ ?term_map <{R2RML_TERM_TYPE}> ?termtype . }} . ' \
            'FILTER ( !bound(?termtype) && isLiteral(?constant) ) }'
    for term_map, _ in mapping_graph.query(query):
        mapping_graph.add((term_map, rdflib.term.URIRef(R2RML_TERM_TYPE), rdflib.term.URIRef(R2RML_LITERAL)))

    # add missing literal termtypes in the object maps
    query = 'SELECT DISTINCT ?om ?pom WHERE { ' \
            f'?pom <{RML_STAR_OBJECT_MAP}> ?om . ' \
            f'OPTIONAL {{ ?om <{R2RML_TERM_TYPE}> ?termtype . }} . ' \
            f'OPTIONAL {{ ?om <{RML_REFERENCE}> ?column . }} . ' \
            f'OPTIONAL {{ ?om <{R2RML_LANGUAGE}> ?language . }} . ' \
            f'OPTIONAL {{ ?om <{R2RML_DATATYPE}> ?datatype . }} . ' \
            'FILTER ( !bound(?termtype) && ( bound(?column) || bound(?language) || bound(?datatype) ) ) }'
    for om, _ in mapping_graph.query(query):
        mapping_graph.add((om, rdflib.term.URIRef(R2RML_TERM_TYPE), rdflib.term.URIRef(R2RML_LITERAL)))

    # now all missing termtypes are IRIs
    for term_map_property in [RML_STAR_SUBJECT_MAP, R2RML_PREDICATE_MAP, RML_STAR_OBJECT_MAP, R2RML_GRAPH_MAP]:
        query = 'SELECT DISTINCT ?term_map ?x WHERE { ' \
                f'?x <{term_map_property}> ?term_map . ' \
                f'OPTIONAL {{ ?term_map <{R2RML_TERM_TYPE}> ?termtype . }} . ' \
                'FILTER ( !bound(?termtype) ) }'
        for term_map, _ in mapping_graph.query(query):
            mapping_graph.add((term_map, rdflib.term.URIRef(R2RML_TERM_TYPE), rdflib.term.URIRef(R2RML_IRI)))

    return mapping_graph


def _complete_triples_map_class(mapping_graph):
    """
    Adds rr:TriplesMap typing for triples maps. For rml:NonAssertedTriplesMap remove rr:TriplesMap typing.
    Triples maps without predicate object maps are transfored to non asserted triples maps as they do no generate
    triples (but can be used in join conditions in other triples maps).
    """

    query = 'SELECT DISTINCT ?triples_map ?logical_source WHERE { ' \
            f'?triples_map <{RML_LOGICAL_SOURCE}> ?logical_source . ' \
            f'OPTIONAL {{ ?triples_map a ?triples_map_class . }} . ' \
            'FILTER ( !bound(?triples_map_class) ) }'
    for triples_map, _ in mapping_graph.query(query):
        mapping_graph.add((triples_map, rdflib.term.URIRef(RDF_TYPE), rdflib.term.URIRef(R2RML_TRIPLES_MAP_CLASS)))

    # rr:TriplesMap without predicate object maps to rml:NonAssertedTriplesMaps
    query = 'SELECT DISTINCT ?triples_map ?logical_source WHERE { ' \
            f'?triples_map <{RML_LOGICAL_SOURCE}> ?logical_source . ' \
            f'OPTIONAL {{ ?triples_map <{R2RML_PREDICATE_OBJECT_MAP}> ?pom . }} . ' \
            'FILTER ( !bound(?pom) ) }'
    for triples_map, _ in mapping_graph.query(query):
        mapping_graph.add(
            (triples_map, rdflib.term.URIRef(RDF_TYPE), rdflib.term.URIRef(RML_STAR_NON_ASSERTED_TRIPLES_MAP_CLASS)))

    # for rml:NonAssertedTriplesMap remove triples typing them as rr:TriplesMap
    query = 'SELECT DISTINCT ?triples_map ?logical_source WHERE { ' \
            f'?triples_map <{RML_LOGICAL_SOURCE}> ?logical_source . ' \
            f'?triples_map a <{R2RML_TRIPLES_MAP_CLASS}> . ' \
            f'?triples_map a <{RML_STAR_NON_ASSERTED_TRIPLES_MAP_CLASS}> . }}'
    for triples_map, _ in mapping_graph.query(query):
        mapping_graph.remove((triples_map, rdflib.term.URIRef(RDF_TYPE), rdflib.term.URIRef(R2RML_TRIPLES_MAP_CLASS)))

    return mapping_graph


def _remove_string_datatypes(mapping_graph):
    """
    Removes xsd:string data types. xsd:string is equivalent to not specifying any data type.
    """

    mapping_graph.remove((None, rdflib.term.URIRef(R2RML_DATATYPE), rdflib.term.URIRef(XSD_STRING)))

    return mapping_graph


def _get_join_conditions_dict(join_query_results):
    """
    Creates a dictionary with the results of the JOIN_CONDITION_PARSING_QUERY. The keys are the identifiers of the
    child triples maps of the join condition. The values of the dictionary are in turn other dictionaries with two
    items, child_value and parent_value, representing a join condition.
    """

    join_conditions_dict = {}

    for join_condition in join_query_results:
        # add the child triples map identifier if it is not in the dictionary
        if join_condition.term_map not in join_conditions_dict:
            join_conditions_dict[join_condition.term_map] = {}

        # add the new join condition (note that several join conditions can apply in a join)
        join_conditions_dict[join_condition.term_map][str(join_condition.join_condition)] = \
            {'child_value': str(join_condition.child_value), 'parent_value': str(join_condition.parent_value)}

    return join_conditions_dict


def _transform_mappings_into_dataframe(mapping_graph, section_name):
    """
    Builds a Pandas DataFrame from the results obtained from MAPPING_PARSING_QUERY and
    JOIN_CONDITION_PARSING_QUERY for one source.
    """

    # parse the mappings with the parsing queries
    mapping_query_results = mapping_graph.query(MAPPING_PARSING_QUERY)
    join_query_results = mapping_graph.query(JOIN_CONDITION_PARSING_QUERY)

    # mapping rules in graph to DataFrame
    source_mappings_df = pd.DataFrame(mapping_query_results.bindings)
    source_mappings_df.columns = source_mappings_df.columns.map(str)

    # process mapping rules with joins
    # create a dict with child triples maps in the keys and its join conditions in the values
    join_conditions_dict = _get_join_conditions_dict(join_query_results)
    # map the dict with the join conditions to the mapping rules in the DataFrame
    source_mappings_df['object_join_conditions'] = source_mappings_df['object_map'].map(join_conditions_dict)
    source_mappings_df['subject_join_conditions'] = source_mappings_df['subject_map'].map(join_conditions_dict)
    # needed for later hashing the dataframe
    source_mappings_df['object_join_conditions'] = source_mappings_df['object_join_conditions'].where(
        pd.notna(source_mappings_df['object_join_conditions']), '')
    source_mappings_df['subject_join_conditions'] = source_mappings_df['subject_join_conditions'].where(
        pd.notna(source_mappings_df['subject_join_conditions']), '')
    # convert the join condition dicts to string (can later be converted back to dict)
    source_mappings_df['object_join_conditions'] = source_mappings_df['object_join_conditions'].astype(str)
    source_mappings_df['subject_join_conditions'] = source_mappings_df['subject_join_conditions'].astype(str)

    # convert all values to string
    for i, row in source_mappings_df.iterrows():
        for col in source_mappings_df.columns:
            if pd.notna(row[col]):
                source_mappings_df.at[i, col] = str(row[col])

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

    identifier = identifier
    if _is_delimited_identifier(identifier):
        return identifier[1:-1]
    return identifier


def _get_valid_template_identifiers(template):
    """
    Removes delimiters from delimited identifiers in a template.
    """

    return template.replace('{"', '{').replace('"}', '}')


def _validate_termtypes(mapping_graph):
    query = 'SELECT DISTINCT ?termtype ?pm WHERE { ' \
            f'?pom <{R2RML_PREDICATE_MAP}> ?pm . ' \
            f'?pm <{R2RML_TERM_TYPE}> ?termtype . }}'
    predicate_termtypes = [str(termtype) for termtype, _ in mapping_graph.query(query)]
    if not (set(predicate_termtypes) <= {R2RML_IRI}):
        raise ValueError(f'Found an invalid predicate termtype. Found values {predicate_termtypes}. '
                         f'Predicate maps must be {R2RML_IRI}.')

    query = 'SELECT DISTINCT ?termtype ?gm WHERE { ' \
            f'?pom <{R2RML_GRAPH_MAP}> ?gm . ' \
            f'?gm <{R2RML_TERM_TYPE}> ?termtype . }}'
    graph_termtypes = [str(termtype) for termtype, _ in mapping_graph.query(query)]
    if not (set(graph_termtypes) <= {R2RML_IRI}):
        raise ValueError(f'Found an invalid graph termtype. Found values {graph_termtypes}. '
                         f'Graph maps must be {R2RML_IRI}.')

    query = 'SELECT DISTINCT ?termtype ?sm WHERE { ' \
            f'?tm <{R2RML_SUBJECT_MAP}> ?sm . ' \
            f'?sm <{R2RML_TERM_TYPE}> ?termtype . }}'
    subject_termtypes = [str(termtype) for termtype, _ in mapping_graph.query(query)]
    if not (set(subject_termtypes) <= {R2RML_IRI, R2RML_BLANK_NODE, RML_STAR_RDF_STAR_TRIPLE}):
        raise ValueError(f'Found an invalid subject termtype. Found values {subject_termtypes}. '
                         f'Subject maps must be {R2RML_IRI}, {R2RML_BLANK_NODE} or {RML_STAR_RDF_STAR_TRIPLE}.')

    query = 'SELECT DISTINCT ?termtype ?om WHERE { ' \
            f'?pom <{R2RML_OBJECT_MAP}> ?om . ' \
            f'?om <{R2RML_TERM_TYPE}> ?termtype . }}'
    object_termtypes = [str(termtype) for termtype, _ in mapping_graph.query(query)]
    if not (set(object_termtypes) <= {R2RML_IRI, R2RML_BLANK_NODE, R2RML_LITERAL, RML_STAR_RDF_STAR_TRIPLE}):
        raise ValueError(f'Found an invalid object termtype. Found values {object_termtypes}. '
                         f'Object maps must be {R2RML_IRI}, {R2RML_BLANK_NODE}, {R2RML_LITERAL} '
                         f'or {RML_STAR_RDF_STAR_TRIPLE}.')


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
        self._preprocess_mappings()

        self._infer_datatypes()
        self.validate_mappings()

        logging.info(f'{len(self.mappings_df)} mapping rules retrieved.')

        # generate mapping partitions
        mapping_partitioner = MappingPartitioner(self.mappings_df, self.config)
        self.mappings_df = mapping_partitioner.partition_mappings()

        # replace empty strings with NaN
        self.mappings_df = self.mappings_df.replace(r'^\s*$', np.nan, regex=True)

        return self.mappings_df

    def _get_from_r2_rml(self):
        """
        Parses the mapping files of all data sources in the config file and adds the parsed mappings rules to a
        common DataFrame for all data sources. If parallelization is enabled and multiple data sources are provided,
        each mapping file is parsed in parallel.
        """

        if self.config.is_multiprocessing_enabled() and self.config.has_multiple_data_sources():
            pool = mp.Pool(self.config.get_number_of_processes())
            mappings_dfs = pool.map(self._parse_data_source_mapping_files, self.config.get_data_sources_sections())
            self.mappings_df = pd.concat([self.mappings_df, pd.concat(mappings_dfs)])
        else:
            for section_name in self.config.get_data_sources_sections():
                data_source_mappings_df = self._parse_data_source_mapping_files(section_name)
                self.mappings_df = pd.concat([self.mappings_df, data_source_mappings_df])

        self.mappings_df = self.mappings_df.reset_index(drop=True)
        # subject_map and object_map columns were used to handle join conditions, no longer needed
        self.mappings_df = self.mappings_df.drop(columns=['subject_map', 'object_map'])

    def _parse_data_source_mapping_files(self, section_name):
        """
        Creates a Pandas DataFrame with the mapping rules of a data source. It loads the mapping files in an rdflib
        graph and recognizes the mapping language used. Mappings are translated to RML.
        It performs queries MAPPING_PARSING_QUERY and JOIN_CONDITION_PARSING_QUERY and process the results to build a
        DataFrame with the mapping rules. Also verifies that there are not repeated triples maps in the mappings.
        """
        
        # create an empty graph
        mapping_graph = rdflib.Graph()

        mapping_file_paths = self.config.get_mappings_files(section_name)
        # load mapping rules to the graph
        for f in mapping_file_paths:
            try:
                mapping_graph.parse(f, format=os.path.splitext(f)[1][1:].strip())
            except:
                # if a file extension such as .rml or .r2rml is used, assume it is turtle (issue #80)
                try:
                    mapping_graph.parse(f)
                except Exception as n3_mapping_parse_exception:
                    raise Exception(n3_mapping_parse_exception)

        # convert R2RML and RML rules to RML-star, so that we can assume RML-star for parsing
        mapping_graph = _mapping_to_rml_star(mapping_graph)
        # convert rr:class to new POMs
        mapping_graph = _rdf_class_to_pom(mapping_graph)
        # expand constant shortcut properties rr:subject, rr:predicate, rr:object and rr:graph
        mapping_graph = _expand_constant_shortcut_properties(mapping_graph)
        # move graph maps in subject maps to the predicate object maps of subject maps
        mapping_graph = _subject_graph_maps_to_pom(mapping_graph)
        # complete predicate object maps without graph maps with rr:defaultGraph
        mapping_graph = _complete_pom_with_default_graph(mapping_graph)
        # if a term as no associated rr:termType, complete it according to R2RML specification
        mapping_graph = _complete_termtypes(mapping_graph)
        # remove xsd:string data types as it is equivalent to not specifying any data type
        mapping_graph = _remove_string_datatypes(mapping_graph)
        # add rr:TriplesMap typing
        mapping_graph = _complete_triples_map_class(mapping_graph)
        # check termtypes are correct
        _validate_termtypes(mapping_graph)

        # convert the SPARQL result set with the parsed mappings to DataFrame
        return _transform_mappings_into_dataframe(mapping_graph, section_name)

    def _preprocess_mappings(self):
        # start by removing duplicated triples
        self.mappings_df = self.mappings_df.drop_duplicates()

        # complete rml:source with file_paths specified in the config file
        self._complete_rml_source_with_config_file_paths()

        # complete source type with config parameters and data file extensions
        self._complete_source_types()

        # ignore the delimited identifiers (this is not conformant with R2MRL specification)
        self._remove_delimiters_from_mappings()

        # create a unique id for each mapping rule
        self.mappings_df.insert(0, 'id', self.mappings_df.reset_index(drop=True).index)

        self._remove_self_joins_no_condition()

    def _complete_source_types(self):
        """
        Adds a column with the source type. The source type is inferred for RDB through the parameter db_url provided
        in the mapping file. If db_url is not provided but the logical source is rml:query, then it is an RML tabular
        view. For data files the source type is inferred from the file extension.
        """
        
        for i, mapping_rule in self.mappings_df.iterrows():
            if self.config.has_database_url(mapping_rule['source_name']):
                self.mappings_df.at[i, 'source_type'] = RDB
            elif self.mappings_df.at[i, 'logical_source_type'] == RML_QUERY:
                # it is a query, but it is not an RDB, hence it is a tabular view
                self.mappings_df.at[i, 'source_type'] = TV
            elif self.mappings_df.at[i, 'logical_source_type'] == RML_SOURCE:
                file_extension = os.path.splitext(str(mapping_rule['logical_source_value']))[1][1:].strip()
                self.mappings_df.at[i, 'source_type'] = file_extension.upper()
            else:
                raise Exception('No source type could be retrieved for some mapping rules.')

    def _complete_rml_source_with_config_file_paths(self):
        """
        Overrides rml:source in the mappings with the file_path parameter in the config file for each data source
        section if provided.
        """

        for section_name in self.config.get_data_sources_sections():
            if self.config.has_file_path(section_name):
                self.mappings_df.loc[
                    self.mappings_df['source_name'] == section_name, 'logical_source_type'] = RML_SOURCE
                self.mappings_df.loc[self.mappings_df['source_name'] == section_name, 'logical_source_value'] = \
                    self.config.get_file_path(section_name)

    def _remove_delimiters_from_mappings(self):
        """
        Removes delimiters from all identifiers in the mapping rules in the input DataFrame.
        """

        for i, mapping_rule in self.mappings_df.iterrows():
            if self.mappings_df.at[i, 'logical_source_type'] == R2RML_TABLE_NAME:
                self.mappings_df.at[i, 'logical_source_value'] = _get_undelimited_identifier(
                    mapping_rule['logical_source_value'])

            if self.mappings_df.at[i, 'subject_map_type'] == R2RML_TEMPLATE:
                self.mappings_df.at[i, 'subject_map_value'] = _get_valid_template_identifiers(
                    mapping_rule['subject_map_value'])
            elif self.mappings_df.at[i, 'subject_map_type'] == RML_REFERENCE:
                self.mappings_df.at[i, 'subject_map_value'] = _get_undelimited_identifier(
                    mapping_rule['subject_map_value'])

            if self.mappings_df.at[i, 'predicate_map_type'] == R2RML_TEMPLATE:
                self.mappings_df.at[i, 'predicate_map_value'] = _get_valid_template_identifiers(
                    mapping_rule['predicate_map_value'])
            elif self.mappings_df.at[i, 'predicate_map_type'] == RML_REFERENCE:
                self.mappings_df.at[i, 'predicate_map_value'] = _get_undelimited_identifier(
                    mapping_rule['predicate_map_value'])

            if self.mappings_df.at[i, 'object_map_type'] == R2RML_TEMPLATE:
                self.mappings_df.at[i, 'object_map_value'] = _get_valid_template_identifiers(
                    mapping_rule['object_map_value'])
            elif self.mappings_df.at[i, 'object_map_type'] == RML_REFERENCE:
                self.mappings_df.at[i, 'object_map_value'] = _get_undelimited_identifier(
                    mapping_rule['object_map_value'])

            if self.mappings_df.at[i, 'graph_map_type'] == R2RML_TEMPLATE:
                self.mappings_df.at[i, 'graph_map_value'] = _get_valid_template_identifiers(
                    mapping_rule['graph_map_value'])
            elif self.mappings_df.at[i, 'graph_map_type'] == RML_REFERENCE:
                self.mappings_df.at[i, 'graph_map_value'] = _get_undelimited_identifier(
                    mapping_rule['graph_map_value'])

            # if join_condition is not null and it is not empty
            for join_conditions_pos in ['subject_join_conditions', 'object_join_conditions']:
                if pd.notna(mapping_rule[join_conditions_pos]) and mapping_rule[join_conditions_pos]:
                    join_conditions = eval(mapping_rule[join_conditions_pos])
                    for key, value in join_conditions.items():
                        join_conditions[key]['child_value'] = _get_undelimited_identifier(
                            join_conditions[key]['child_value'])
                        join_conditions[key]['parent_value'] = _get_undelimited_identifier(
                            join_conditions[key]['parent_value'])
                        self.mappings_df.at[i, join_conditions_pos] = str(join_conditions)

    def _infer_datatypes(self):
        """
        Get RDF datatypes for rules corresponding to relational data sources if they are not overridden in the mapping
        rules. The inferring of RDF datatypes is defined in R2RML specification
        (https://www.w3.org/2001/sw/rdb2rdf/r2rml/#natural-mapping).
        """

        # return if datatype inferring is disabled in the config
        if not self.config.infer_sql_datatypes():
            return

        for i, mapping_rule in self.mappings_df.iterrows():
            # datatype inference only applies to relational data sources
            if (mapping_rule['source_type'] == RDB) and (
                    # datatype inference only applies to literals
                    str(mapping_rule['object_termtype']) == R2RML_LITERAL) and (
                    # if the literal has a language tag or an overridden datatype, datatype inference does not apply
                    pd.isna(mapping_rule['object_datatype']) and pd.isna(mapping_rule['object_language'])):

                if mapping_rule['object_map_type'] == RML_REFERENCE:
                    inferred_data_type = get_rdb_reference_datatype(self.config, mapping_rule,
                                                                    mapping_rule['object_map_value'])

                    if not inferred_data_type:
                        # no data type was inferred
                        continue

                    self.mappings_df.at[i, 'object_datatype'] = inferred_data_type
                    if self.mappings_df.at[i, 'logical_source_type'] == R2RML_TABLE_NAME:
                        logging.debug(f"`{inferred_data_type}` datatype inferred for column "
                                      f"`{mapping_rule['object_map_value']}` of table "
                                      f"`{mapping_rule['logical_source_value']}` "
                                      f"in data source `{mapping_rule['source_name']}`.")
                    elif self.mappings_df.at[i, 'logical_source_type'] == RML_QUERY:
                        logging.debug(f"`{inferred_data_type}` datatype inferred for reference "
                                      f"`{mapping_rule['object_map_value']}` in query "
                                      f"[{mapping_rule['logical_source_value']}] "
                                      f"in data source `{mapping_rule['source_name']}`.")

    def validate_mappings(self):
        """
        Checks that the mapping rules in the input DataFrame are valid. If something is wrong in the mappings the
        execution is stopped. Specifically it is checked that language tags and
        datatypes are used properly. Also checks that different data sources do not have triples map with the same id.
        """

        # if there is a datatype or language tag then the object map termtype must be a rr:Literal
        if len(self.mappings_df.loc[(self.mappings_df['object_termtype'] != R2RML_LITERAL) &
                                    pd.notna(self.mappings_df['object_datatype']) &
                                    pd.notna(self.mappings_df['object_language'])]) > 0:
            raise Exception('Found object maps with a language tag or a datatype, '
                            'but that do not have termtype rr:Literal.')

        # language tags and datatypes cannot be used simultaneously, language tags are used if both are given
        if len(self.mappings_df.loc[pd.notna(self.mappings_df['object_language']) &
                                    pd.notna(self.mappings_df['object_datatype'])]) > 0:
            logging.warning('Found object maps with a language tag and a datatype. Both of them cannot be used '
                            'simultaneously for the same object map, and the language tag has preference.')

        # check that language tags are valid
        language_tags = set(self.mappings_df['object_language'].dropna())
        # the place to look for valid language subtags is the IANA Language Subtag Registry
        # (https://www.iana.org/assignments/language-subtag-registry/language-subtag-registry)
        for language_tag in language_tags:
            # in general, if the language subtag is longer than 3 characters it is not valid
            if len(language_tag.split('-')[0]) > 3:
                raise ValueError(f'Found invalid language tag `{language_tag}`. '
                                 'Language tags must be in the IANA Language Subtag Registry.')

        # check that a triples map id is not repeated in different data sources
        # Get unique source names and triples map identifiers
        aux_mappings_df = self.mappings_df[['source_name', 'triples_map_id']].drop_duplicates()
        # get repeated triples map identifiers
        repeated_triples_map_ids = get_repeated_elements_in_list(list(aux_mappings_df['triples_map_id'].astype(str)))
        # of those repeated identifiers
        repeated_triples_map_ids = [tm_id for tm_id in repeated_triples_map_ids]
        if len(repeated_triples_map_ids) > 0:
            raise Exception('The following triples maps appear in more than one data source: '
                            f'{repeated_triples_map_ids}. '
                            'Check the mapping files, one triple map cannot be repeated in different data sources.')

    # TODO: deprecate
    def _remove_self_joins_no_condition(self):
        for i, mapping_rule in self.mappings_df.iterrows():
            if mapping_rule['object_map_type'] == R2RML_PARENT_TRIPLES_MAP:
                parent_triples_map_rule = get_mapping_rule(self.mappings_df, mapping_rule['object_map_value'])
                if mapping_rule['logical_source_value'] == parent_triples_map_rule['logical_source_value'] and str(
                        # str() is to be able to compare np.nan
                        mapping_rule['iterator']) == str(parent_triples_map_rule['iterator']):

                    remove_join = True
                    # check that all conditions in the join condition have the same references
                    try:
                        join_conditions = eval(mapping_rule['object_join_conditions'])
                        for key, join_condition in join_conditions.items():
                            if join_condition['child_value'] != join_condition['parent_value']:
                                remove_join = False
                    except:
                        # eval() has failed because there are no join conditions, the join can be removed
                        remove_join = True

                    if remove_join and pd.notna(mapping_rule['object_join_conditions']):
                        self.mappings_df.at[i, 'object_map_type'] = parent_triples_map_rule.at['subject_map_type']
                        self.mappings_df.at[i, 'object_map_value'] = parent_triples_map_rule.at['subject_map_value']
                        self.mappings_df.at[i, 'object_termtype'] = parent_triples_map_rule.at['subject_termtype']
                        self.mappings_df.at[i, 'object_join_conditions'] = np.nan
                        logging.debug(f"Removed self-join from mapping rule `{mapping_rule['id']}`.")
