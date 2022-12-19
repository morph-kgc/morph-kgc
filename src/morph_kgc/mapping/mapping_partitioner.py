__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import logging
import multiprocessing as mp

from itertools import permutations

from ..constants import *
from ..utils import get_rml_rule


def get_invariant_of_template(template):
    """
    Retrieves the part of the template before the first reference. This part of the template does not depend on
    reference and therefore is invariable. If the template has no references, it is an invalid template, and an
    exception is thrown.
    """

    template_for_splitting = template.replace('\\{', AUXILIAR_UNIQUE_REPLACING_STRING)
    if '{' in template_for_splitting:
        invariant_of_template = template_for_splitting.split('{')[0]
        invariant_of_template = invariant_of_template.replace(AUXILIAR_UNIQUE_REPLACING_STRING, '\\{')
    else:
        # no references were found in the template, and therefore the template is invalid
        raise Exception(f'Invalid template `{template}`. No pairs of unescaped curly braces were found.')

    return invariant_of_template


def _generate_maximal_partition_for_a_position_ordering(rml_df, position_ordering):
    """
    Generates a maximal mapping partition for a position ordering.
    """

    for position in position_ordering:
        current_global_group = rml_df.at[0, 'mapping_partition']  # to simulate a groupby
        current_group = 0
        current_invariant = AUXILIAR_UNIQUE_REPLACING_STRING
        current_literal_type = AUXILIAR_UNIQUE_REPLACING_STRING

        # ---------------------------- SUBJECT ----------------------------

        if position == 'S':
            rml_df.sort_values(by=['mapping_partition', 'subject_invariant'], inplace=True, ascending=True)

            for i, rml_rule in rml_df.iterrows():
                if current_global_group != rml_rule['mapping_partition']:
                    current_group = 0
                    current_invariant = AUXILIAR_UNIQUE_REPLACING_STRING
                    current_global_group = rml_rule['mapping_partition']

                if rml_rule['subject_termtype'] == R2RML_BLANK_NODE:
                    rml_df.at[i, 'mapping_partition'] = f"{rml_df.at[i, 'mapping_partition']}-0"
                elif rml_rule['subject_invariant'].startswith(current_invariant):
                    rml_df.at[i, 'mapping_partition'] = f"{rml_df.at[i, 'mapping_partition']}-{current_group}"
                else:
                    current_group += 1
                    current_invariant = rml_rule['subject_invariant']
                    rml_df.at[i, 'mapping_partition'] = f"{rml_df.at[i, 'mapping_partition']}-{current_group}"

        # ---------------------------- PREDICATE ----------------------------

        if position == 'P':
            rml_df.sort_values(by=['mapping_partition', 'predicate_invariant'], inplace=True, ascending=True)

            # if all predicates are constant terms we can use full string comparison instead of startswith
            enforce_invariant_non_subset = set(rml_df['predicate_map_value']) == set(R2RML_CONSTANT)

            for i, rml_rule in rml_df.iterrows():
                if current_global_group != rml_rule['mapping_partition']:
                    current_group = 0
                    current_invariant = AUXILIAR_UNIQUE_REPLACING_STRING
                    current_global_group = rml_rule['mapping_partition']

                if enforce_invariant_non_subset and rml_rule['predicate_invariant'] == current_invariant:
                    rml_df.at[i, 'mapping_partition'] = f"{rml_df.at[i, 'mapping_partition']}-{current_group}"
                elif not enforce_invariant_non_subset and rml_rule['predicate_invariant'].startswith(
                        current_invariant):
                    rml_df.at[i, 'mapping_partition'] = f"{rml_df.at[i, 'mapping_partition']}-{current_group}"
                else:
                    current_group += 1
                    current_invariant = rml_rule['predicate_invariant']
                    rml_df.at[i, 'mapping_partition'] = f"{rml_df.at[i, 'mapping_partition']}-{current_group}"

        # ---------------------------- OBJECT ----------------------------

        if position == 'O':
            rml_df.sort_values(by=['mapping_partition', 'object_termtype', 'literal_type', 'object_invariant'],
                                    inplace=True, ascending=True)

            for i, rml_rule in rml_df.iterrows():
                if current_global_group != rml_rule['mapping_partition']:
                    current_group = 0
                    current_invariant = AUXILIAR_UNIQUE_REPLACING_STRING
                    current_global_group = rml_rule['mapping_partition']

                if rml_rule['object_termtype'] == R2RML_BLANK_NODE:
                    rml_df.at[i, 'mapping_partition'] = f"{rml_df.at[i, 'mapping_partition']}-0"
                elif rml_rule['object_termtype'] == R2RML_LITERAL:
                    # str() is necessary for NULL literal types
                    if str(rml_rule['literal_type']) != current_literal_type:
                        current_group += 1
                        current_literal_type = str(rml_rule['literal_type'])
                    rml_df.at[i, 'mapping_partition'] = f"{rml_df.at[i, 'mapping_partition']}-{current_group}"
                elif rml_rule['object_invariant'].startswith(current_invariant):
                    rml_df.at[i, 'mapping_partition'] = f"{rml_df.at[i, 'mapping_partition']}-{current_group}"
                else:
                    current_group += 1
                    current_invariant = rml_rule['object_invariant']
                    rml_df.at[i, 'mapping_partition'] = f"{rml_df.at[i, 'mapping_partition']}-{current_group}"

        # ---------------------------- GRAPH ----------------------------

        if position == 'G':
            rml_df.sort_values(by=['mapping_partition', 'graph_invariant'], inplace=True, ascending=True)

            # if all graph are constant terms we can use full string comparison instead of startswith
            enforce_invariant_non_subset = set(rml_df['graph_map_value']) == set(R2RML_CONSTANT)

            for i, rml_rule in rml_df.iterrows():
                if current_global_group != rml_rule['mapping_partition']:
                    current_group = 0
                    current_invariant = AUXILIAR_UNIQUE_REPLACING_STRING
                    current_global_group = rml_rule['mapping_partition']

                if enforce_invariant_non_subset and rml_rule['graph_invariant'] == current_invariant:
                    rml_df.at[i, 'mapping_partition'] = f"{rml_df.at[i, 'mapping_partition']}-{current_group}"
                elif not enforce_invariant_non_subset and rml_rule['graph_invariant'].startswith(current_invariant):
                    rml_df.at[i, 'mapping_partition'] = f"{rml_df.at[i, 'mapping_partition']}-{current_group}"
                else:
                    current_group += 1
                    current_invariant = rml_rule['graph_invariant']
                    rml_df.at[i, 'mapping_partition'] = f"{rml_df.at[i, 'mapping_partition']}-{current_group}"

    return rml_df


class MappingPartitioner:

    def __init__(self, rml_df, config):
        self.rml_df = rml_df
        self.config = config

    def __str__(self):
        return str(self.rml_df)

    def __repr__(self):
        return repr(self.rml_df)

    def __len__(self):
        return len(self.rml_df)

    def partition_mappings(self):
        """
        Generates the mapping partition for the mapping rules in the input DataFrame based on the provided mapping
        partitioning criteria. A new column in the DataFrame is added indicating the mapping partition assigned to every
        mapping rule.
        """

        self.rml_df = self.rml_df.reset_index(drop=True)

        # if RML-star or TMs without POMs (rml:NonAssertedTriplesMap) do not partition mappings (assign empty partition)
        if RML_STAR_QUOTED_TRIPLES_MAP in self.rml_df['subject_map_type'].values or \
            RML_STAR_QUOTED_TRIPLES_MAP in self.rml_df['object_map_type'] or \
            RML_STAR_NON_ASSERTED_TRIPLES_MAP_CLASS in set(self.rml_df['triples_map_type']):

            # TODO: enable mapping partitioning for these cases
            self.rml_df['mapping_partition'] = '0-0-0-0'

            return self.rml_df

        if self.config.get_mapping_partitioning() == PARTIAL_AGGREGATIONS_PARTITIONING:
            self._get_term_invariants()
            self._generate_partial_aggregations_partition()
        elif self.config.get_mapping_partitioning() == MAXIMAL_PARTITIONING:
            self._get_term_invariants()
            self._generate_maximal_partition()
        elif self.config.get_mapping_partitioning() in NO_PARTITIONING:
            # assign empty partition
            self.rml_df['mapping_partition'] = '0-0-0-0'
        else:
            logging.error('Selected mapping partitioning algorithm is not valid.')

        logging.info(f"Mapping partition with {len(set(self.rml_df['mapping_partition']))} groups generated.")
        logging.info('Maximum number of rules within mapping group: '
                     f"{self.rml_df['mapping_partition'].value_counts()[0]}.")

        return self.rml_df

    def _generate_maximal_partition(self):
        """
        Generates a mapping partition with the maximum number of mapping groups.
        """

        self.rml_df['literal_type'] = self.rml_df['object_language'] + self.rml_df['object_datatype']
        self.rml_df['mapping_partition'] = ''

        rml_df = self.rml_df.copy()

        position_orderings = list(permutations(['S', 'P', 'O', 'G']))

        if self.config.is_multiprocessing_enabled():
            pool = mp.Pool(self.config.get_number_of_processes())
            mapping_partitions_dfs = pool.starmap(_generate_maximal_partition_for_a_position_ordering,
                                                  zip([self.rml_df.copy()] * len(position_orderings),
                                                      position_orderings))
        else:
            mapping_partitions_dfs = []
            for position_ordering in position_orderings:
                mapping_partitions_dfs.append(
                    _generate_maximal_partition_for_a_position_ordering(self.rml_df.copy(), position_ordering))

        max_num_groups = -1
        maximal_partition = None
        for mapping_partitions_df in mapping_partitions_dfs:
            if len(set(mapping_partitions_df['mapping_partition'])) > max_num_groups:
                max_num_groups = len(set(mapping_partitions_df['mapping_partition']))
                maximal_partition = mapping_partitions_df

        maximal_partition['mapping_partition'] = maximal_partition['mapping_partition'].str[1:]
        # drop the auxiliary columns that were created just to generate the mapping partition
        maximal_partition.drop([
            'subject_invariant',
            'predicate_invariant',
            'object_invariant',
            'graph_invariant',
            'literal_type'],
            axis=1, inplace=True)

        self.rml_df = maximal_partition

    def _generate_partial_aggregations_partition(self):
        """
        Generates a mapping partition by independently partitioning by Subject, Predicate, Object and Graph, and
        aggregating this independent partitions.
        """

        # initialize empty mapping partition
        self.rml_df['subject_partition'] = ''
        self.rml_df['predicate_partition'] = ''
        self.rml_df['object_partition'] = ''
        self.rml_df['graph_partition'] = ''

        self.rml_df['literal_type'] = self.rml_df['object_language'] + self.rml_df['object_datatype']

        # generate partial mapping partition for subjects, predicates, objects and graphs

        # ---------------------------- SUBJECT ----------------------------

        # sort the mapping rules based on the subject invariants. An invariant that starts with another
        # invariant is placed behind in the ordering e.g. http://example.org/term/something and
        # http://example.org/term: http://example.org/term is placed first
        self.rml_df.sort_values(by=['subject_invariant'], inplace=True, ascending=True)

        current_group = 0
        current_invariant = AUXILIAR_UNIQUE_REPLACING_STRING

        # iterate over the mapping rules and check if the invariant starts with the invariant of the previous rule
        # if it does, then it is in the same mapping partition than the previous mapping rule
        # if it does not, then the mapping rule is in a new mapping partition
        for i, rml_rule in self.rml_df.iterrows():
            if rml_rule['subject_termtype'] == R2RML_BLANK_NODE:
                self.rml_df.at[i, 'subject_partition'] = '0'
            elif rml_rule['subject_invariant'].startswith(current_invariant):
                self.rml_df.at[i, 'subject_partition'] = str(current_group)
            else:
                current_group += 1
                current_invariant = rml_rule['subject_invariant']
                self.rml_df.at[i, 'subject_partition'] = str(current_group)

        # ---------------------------- PREDICATE ----------------------------

        self.rml_df.sort_values(by='predicate_invariant', inplace=True, ascending=True)

        current_group = 0
        current_invariant = AUXILIAR_UNIQUE_REPLACING_STRING

        # if all predicates are constant terms we can use full string comparison instead of startswith
        enforce_invariant_non_subset = set(self.rml_df['predicate_map_value']) == set(R2RML_CONSTANT)

        if enforce_invariant_non_subset:
            logging.debug('All predicate maps are constant-valued, invariant subset is not enforced.')

        for i, rml_rule in self.rml_df.iterrows():
            if enforce_invariant_non_subset and rml_rule['predicate_invariant'] == current_invariant:
                self.rml_df.at[i, 'predicate_partition'] = str(current_group)
            elif not enforce_invariant_non_subset and rml_rule['predicate_invariant'].startswith(current_invariant):
                self.rml_df.at[i, 'predicate_partition'] = str(current_group)
            else:
                current_group += 1
                current_invariant = rml_rule['predicate_invariant']
                self.rml_df.at[i, 'predicate_partition'] = str(current_group)

        # ---------------------------- OBJECT ----------------------------

        self.rml_df.sort_values(by=['object_termtype', 'literal_type', 'object_invariant'],
                                     inplace=True, ascending=True)

        current_group = 0
        current_literal_type = AUXILIAR_UNIQUE_REPLACING_STRING
        current_invariant = AUXILIAR_UNIQUE_REPLACING_STRING

        for i, rml_rule in self.rml_df.iterrows():
            if rml_rule['object_termtype'] == R2RML_BLANK_NODE:
                self.rml_df.at[i, 'object_partition'] = '0'
            elif rml_rule['object_termtype'] == R2RML_LITERAL:
                # str() is necessary for NULL literal types
                if str(rml_rule['literal_type']) != current_literal_type:
                    current_group += 1
                    current_literal_type = str(rml_rule['literal_type'])
                self.rml_df.at[i, 'object_partition'] = str(current_group)
            elif rml_rule['object_invariant'].startswith(current_invariant):
                self.rml_df.at[i, 'object_partition'] = str(current_group)
            else:
                current_group += 1
                current_invariant = rml_rule['object_invariant']
                self.rml_df.at[i, 'object_partition'] = str(current_group)

        # ---------------------------- GRAPH ----------------------------

        self.rml_df.sort_values(by=['graph_invariant'], inplace=True, ascending=True)

        current_group = 0
        current_invariant = AUXILIAR_UNIQUE_REPLACING_STRING

        # if all graph are constant terms we can use full string comparison instead of startswith
        enforce_invariant_non_subset = set(self.rml_df['graph_map_value']) == set(R2RML_CONSTANT)

        if enforce_invariant_non_subset:
            logging.debug('All graph maps are constant-valued, invariant subset is not enforced.')

        for i, rml_rule in self.rml_df.iterrows():
            if enforce_invariant_non_subset and rml_rule['graph_invariant'] == current_invariant:
                self.rml_df.at[i, 'graph_partition'] = str(current_group)
            elif not enforce_invariant_non_subset and rml_rule['graph_invariant'].startswith(current_invariant):
                self.rml_df.at[i, 'graph_partition'] = str(current_group)
            else:
                current_group += 1
                current_invariant = rml_rule['graph_invariant']
                self.rml_df.at[i, 'graph_partition'] = str(current_group)

        # aggregate the independent mapping partition generated for subjects, predicates and graphs to generate the
        # final mapping partition
        self.rml_df['mapping_partition'] = self.rml_df['subject_partition'].astype(str) + '-' + \
            self.rml_df['predicate_partition'].astype(str) + '-' + \
            self.rml_df['object_partition'].astype(str) + '-' + self.rml_df['graph_partition'].astype(str)

        # drop the auxiliary columns that were created just to generate the mapping partition
        self.rml_df.drop([
            'subject_partition',
            'subject_invariant',
            'predicate_partition',
            'predicate_invariant',
            'object_partition',
            'object_invariant',
            'graph_partition',
            'graph_invariant',
            'literal_type'],
            axis=1, inplace=True)

    def _get_term_invariants(self):
        """
        Adds in the input DataFrame new columns for the invariants of mapping rules. Columns for the invariants of
        subjects, predicates and graphs are added, and they are completed based on the provided mapping
        partitioning criteria.
        """

        # initialize empty invariants for all terms
        self.rml_df['subject_invariant'] = ''
        self.rml_df['predicate_invariant'] = ''
        self.rml_df['object_invariant'] = ''
        self.rml_df['graph_invariant'] = ''

        for i, rml_rule in self.rml_df.iterrows():
            # SUBJECT
            if rml_rule['subject_map_type'] == R2RML_TEMPLATE:
                self.rml_df.at[i, 'subject_invariant'] = \
                    get_invariant_of_template(str(rml_rule['subject_map_value']))
            elif rml_rule['subject_map_type'] == R2RML_CONSTANT:
                self.rml_df.at[i, 'subject_invariant'] = str(rml_rule['subject_map_value'])
            # PREDICATE
            if rml_rule['predicate_map_type'] == R2RML_CONSTANT:
                self.rml_df.at[i, 'predicate_invariant'] = str(rml_rule['predicate_map_value'])
            elif rml_rule['predicate_map_type'] == R2RML_TEMPLATE:
                self.rml_df.at[i, 'predicate_invariant'] = \
                    get_invariant_of_template(str(rml_rule['predicate_map_value']))

            # OBJECT
            if rml_rule['object_map_type'] == R2RML_CONSTANT:
                self.rml_df.at[i, 'object_invariant'] = str(rml_rule['object_map_value'])
            elif rml_rule['object_map_type'] == R2RML_TEMPLATE:
                self.rml_df.at[i, 'object_invariant'] = \
                    get_invariant_of_template(str(rml_rule['object_map_value']))
            # elif pd.notna(rml_rule['object_parent_triples_map']) and rml_rule['object_parent_triples_map']!="":
            elif rml_rule['object_map_type'] == R2RML_PARENT_TRIPLES_MAP:

                # get the invariant for referencing object maps
                # parent_rml_rule = get_rml_rule(self.rml_df, rml_rule['object_parent_triples_map'])
                parent_rml_rule = get_rml_rule(self.rml_df, rml_rule['object_map_value'])


                if rml_rule['subject_map_type'] == R2RML_CONSTANT:
                    self.rml_df.at[i, 'object_invariant'] = str(parent_rml_rule['subject_map_value'])
                elif rml_rule['subject_map_type'] == R2RML_TEMPLATE:
                    self.rml_df.at[i, 'object_invariant'] = \
                        get_invariant_of_template(str(parent_rml_rule['subject_map_value']))

            # GRAPH
            if rml_rule['graph_map_type'] == R2RML_CONSTANT:
                self.rml_df.at[i, 'graph_invariant'] = str(rml_rule['graph_map_value'])
            elif rml_rule['graph_map_type'] == R2RML_TEMPLATE:
                self.rml_df.at[i, 'graph_invariant'] = \
                    get_invariant_of_template(str(rml_rule['graph_map_value']))
