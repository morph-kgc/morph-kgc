""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright (C) 2020-2021 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import constants
import logging
import pandas as pd

from utils import get_invariable_part_of_template


class MappingPartitioner:

    def __init__(self, mappings_df, config):
        self.mappings_df = mappings_df
        self.config = config

    def __str__(self):
        return str(self.mappings_df)

    def __repr__(self):
        return repr(self.mappings_df)

    def __len__(self):
        return len(self.mappings_df)

    def partition_mappings(self):
        """
        Generates the mapping partitions for the mapping rules in the input DataFrame based on the provided mapping
        partitioning criteria. A new column in the DataFrame is added indicating the mapping partition assigned to every
        mapping rule.
        """

        self._validate_mapping_partition_criteria()
        self._get_mapping_partitions_invariable_parts()
        self.generate_mapping_partitions()

        return self.mappings_df

    def generate_mapping_partitions(self):
        mapping_partitions = self.config.get(constants.CONFIG_SECTION, 'mapping_partitions')

        # initialize empty mapping partitions
        self.mappings_df['subject_partition'] = ''
        self.mappings_df['predicate_partition'] = ''
        self.mappings_df['object_partition'] = ''
        self.mappings_df['graph_partition'] = ''

        # generate independent mapping partitions for subjects, predicates objects and graphs
        if 's' in mapping_partitions:
            # sort the mapping rules based on the subject invariable part an invariable part that starts with another
            # invariable part is placed behind in the ordering e.g. http://example.org/term/something and
            # http://example.org/term: http://example.org/term is placed first
            self.mappings_df.sort_values(by=['subject_invariable_part', 'subject_termtype'], inplace=True,
                                         ascending=True)

            num_partition = 0
            root_last_partition = constants.AUXILIAR_UNIQUE_REPLACING_STRING

            # iterate over the mapping rules and check if the invariable part starts with the invariable part of the
            # previous rule
            # if it does, then it is in the same mapping partition than the previous mapping rule
            # if it does not, then the mapping rule is in a new mapping partition
            for i, mapping_rule in self.mappings_df.iterrows():
                if mapping_rule['subject_termtype'] == constants.R2RML['blank_node']:
                    pass  # assign the partition `no partition`
                elif not mapping_rule['subject_invariable_part']:
                    # this case is to handle templates without invariable part (no mapping partitions will be generated)
                    self.mappings_df.at[i, 'subject_partition'] = '0'
                elif mapping_rule['subject_invariable_part'].startswith(root_last_partition):
                    self.mappings_df.at[i, 'subject_partition'] = str(num_partition)
                else:
                    num_partition = num_partition + 1
                    root_last_partition = mapping_rule['subject_invariable_part']
                    self.mappings_df.at[i, 'subject_partition'] = str(num_partition)

        if 'p' in mapping_partitions:
            self.mappings_df.sort_values(by='predicate_invariable_part', inplace=True, ascending=True)
            num_partition = 0
            root_last_partition = constants.AUXILIAR_UNIQUE_REPLACING_STRING

            # if all predicates are constant terms we can use full string comparison instead of startswith
            use_equal = self.mappings_df['predicate_constant'].notna().all()
            if use_equal:
                logging.debug('All predicate maps are constant-valued, '
                              'using strict criteria to generate mapping partitions.')

            for i, mapping_rule in self.mappings_df.iterrows():
                if not mapping_rule['predicate_invariable_part']:
                    self.mappings_df.at[i, 'predicate_partition'] = '0'
                elif use_equal and mapping_rule['predicate_invariable_part'] == root_last_partition:
                    self.mappings_df.at[i, 'predicate_partition'] = str(num_partition)
                elif not use_equal and mapping_rule['predicate_invariable_part'].startswith(root_last_partition):
                    self.mappings_df.at[i, 'predicate_partition'] = str(num_partition)
                else:
                    num_partition = num_partition + 1
                    root_last_partition = mapping_rule['predicate_invariable_part']
                    self.mappings_df.at[i, 'predicate_partition'] = str(num_partition)

        if 'o' in mapping_partitions:
            self.mappings_df.sort_values(by=['object_invariable_part', 'object_termtype'], inplace=True, ascending=True)

            num_partition = 0
            root_last_partition = constants.AUXILIAR_UNIQUE_REPLACING_STRING

            for i, mapping_rule in self.mappings_df.iterrows():
                if mapping_rule['object_termtype'] == constants.R2RML['blank_node']:
                    pass  # assign the partition `no partition`
                elif not mapping_rule['object_invariable_part']:
                    self.mappings_df.at[i, 'object_partition'] = '0'
                elif mapping_rule['object_invariable_part'].startswith(root_last_partition):
                    self.mappings_df.at[i, 'object_partition'] = str(num_partition)
                else:
                    num_partition = num_partition + 1
                    root_last_partition = mapping_rule['object_invariable_part']
                    self.mappings_df.at[i, 'object_partition'] = str(num_partition)

        if 'g' in mapping_partitions:
            self.mappings_df.sort_values(by='graph_invariable_part', inplace=True, ascending=True)
            num_partition = 0
            root_last_partition = constants.AUXILIAR_UNIQUE_REPLACING_STRING

            # if all graph are constant terms we can use full string comparison instead of startswith
            use_equal = self.mappings_df['graph_constant'].notna().all()
            if use_equal:
                logging.debug('All graph maps are constant-valued, '
                              'using strict criteria to generate mapping partitions.')

            for i, mapping_rule in self.mappings_df.iterrows():
                if use_equal and mapping_rule['graph_invariable_part'] == root_last_partition:
                    self.mappings_df.at[i, 'graph_partition'] = str(num_partition)
                elif not mapping_rule['graph_invariable_part']:
                    self.mappings_df.at[i, 'graph_partition'] = '0'
                elif not use_equal and mapping_rule['graph_invariable_part'].startswith(root_last_partition):
                    self.mappings_df.at[i, 'graph_partition'] = str(num_partition)
                else:
                    num_partition = num_partition + 1
                    root_last_partition = mapping_rule['graph_invariable_part']
                    self.mappings_df.at[i, 'graph_partition'] = str(num_partition)

        # aggregate the independent mapping partitions generated for subjects, predicates and graphs to generate the
        # final mapping partitions
        self.mappings_df['mapping_partition'] = ''
        if 's' in mapping_partitions:
            self.mappings_df['mapping_partition'] = self.mappings_df['subject_partition'] + '_'
        if 'p' in mapping_partitions:
            self.mappings_df['mapping_partition'] = self.mappings_df['mapping_partition'] + self.mappings_df[
                'predicate_partition'] + '_'
        if 'o' in mapping_partitions:
            self.mappings_df['mapping_partition'] = self.mappings_df['mapping_partition'] + self.mappings_df[
                'object_partition'] + '_'
        if 'g' in mapping_partitions:
            self.mappings_df['mapping_partition'] = self.mappings_df['mapping_partition'] + self.mappings_df[
                'graph_partition'] + '_'

        if len(mapping_partitions) > 0:
            # remove the last underscore
            self.mappings_df['mapping_partition'] = self.mappings_df['mapping_partition'].astype(str).str[:-1]
        else:
            # no mapping partitions generated, assign unique mapping partition
            self.mappings_df['mapping_partition'] = '1'

        # drop the auxiliary columns that were created just to generate the mapping partitions
        self.mappings_df.drop([
            'subject_partition',
            'subject_invariable_part',
            'predicate_partition',
            'predicate_invariable_part',
            'object_partition',
            'object_invariable_part',
            'graph_partition',
            'graph_invariable_part'],
            axis=1, inplace=True)

        if mapping_partitions:
            logging.info(str(len(set(self.mappings_df['mapping_partition']))) + ' mapping partitions generated.')

    def _validate_mapping_partition_criteria(self):
        """
        Checks that the mapping partitioning criteria is valid. A criteria (subject (s), predicate(p), or graph(g)) is
        not valid if there is a mapping rule that uses reference terms to generate terms for that criteria ((s), (p),
        (g)). Any invalid criteria is omitted and a valid partitioning criteria is returned. If `guess` is selected as
        mapping partitioning criteria, then all valid criteria for the mapping rules in the input DataFrame is returned.
        """

        mapping_partition_criteria = self.config.get(constants.CONFIG_SECTION, 'mapping_partitions')
        valid_mapping_partition_criteria = ''

        if 'guess' in mapping_partition_criteria:
            # add as mapping partitioning criteria all criteria that is valid for the mapping rules in the DataFrame
            # a criteria is not valid if there is a reference term
            if not self.mappings_df['subject_reference'].notna().any():
                valid_mapping_partition_criteria += 's'
            if not self.mappings_df['predicate_reference'].notna().any():
                valid_mapping_partition_criteria += 'p'
            # object is always a valid criteria
            valid_mapping_partition_criteria += 'o'
            if not self.mappings_df['graph_reference'].notna().any():
                valid_mapping_partition_criteria += 'g'

        else:
            if 's' in mapping_partition_criteria:
                # subject is used as partitioning criteria.
                # if there is any subject that is a reference that means it is not a template nor a constant, and it
                # cannot be used as partitioning criteria. The same for predicate and graph.
                if self.mappings_df['subject_reference'].notna().any():
                    logging.warning('Invalid mapping partition criteria `' + mapping_partition_criteria +
                                    '`: mappings cannot be partitioned by subject because mappings contain subject '
                                    'terms that are rr:column or rml:reference.')
                else:
                    valid_mapping_partition_criteria += 's'

            if 'p' in mapping_partition_criteria:
                if self.mappings_df['predicate_reference'].notna().any():
                    logging.warning('Invalid mapping partition criteria `' + mapping_partition_criteria +
                                    '`: mappings cannot be partitioned by predicate because mappings contain'
                                    ' predicate terms that are rr:column or rml:reference.')
                else:
                    valid_mapping_partition_criteria += 'p'

            if 'o' in mapping_partition_criteria:
                valid_mapping_partition_criteria += 'o'

            if 'g' in mapping_partition_criteria:
                if self.mappings_df['graph_reference'].notna().any():
                    logging.warning('Invalid mapping partition criteria `' + mapping_partition_criteria +
                                    '`: mappings cannot be partitioned by graph because mappings '
                                    'contain graph terms that are rr:column or rml:reference.')
                else:
                    valid_mapping_partition_criteria += 'g'

        if valid_mapping_partition_criteria:
            logging.info('Using `' + valid_mapping_partition_criteria + '` as mapping partition criteria.')
        else:
            logging.info('Not using mapping partitioning.')

        self.config.set(constants.CONFIG_SECTION, 'mapping_partitions', valid_mapping_partition_criteria)

    def _get_mapping_partitions_invariable_parts(self):
        """
        Adds in the input DataFrame new columns for the invariable parts of mapping rules. Columns for the invariable
        parts of subjects, predicates and graphs are added, and they are completed based on the provided mapping
        partitioning criteria.
        """

        # initialize empty invariable parts for all terms
        self.mappings_df['subject_invariable_part'] = ''
        self.mappings_df['predicate_invariable_part'] = ''
        self.mappings_df['object_invariable_part'] = ''
        self.mappings_df['graph_invariable_part'] = ''

        for i, mapping_rule in self.mappings_df.iterrows():
            if 's' in self.config.get(constants.CONFIG_SECTION, 'mapping_partitions'):
                if pd.notna(mapping_rule['subject_template']):
                    self.mappings_df.at[i, 'subject_invariable_part'] = \
                        get_invariable_part_of_template(str(mapping_rule['subject_template']))
                elif pd.notna(mapping_rule['subject_constant']):
                    self.mappings_df.at[i, 'subject_invariable_part'] = str(mapping_rule['subject_constant'])
                else:
                    logging.error("Could not get the invariable part of the subject for mapping rule `" +
                                  str(mapping_rule['id']) + "`.")

            if 'p' in self.config.get(constants.CONFIG_SECTION, 'mapping_partitions'):
                if pd.notna(mapping_rule['predicate_constant']):
                    self.mappings_df.at[i, 'predicate_invariable_part'] = str(mapping_rule['predicate_constant'])
                elif pd.notna(mapping_rule['predicate_template']):
                    self.mappings_df.at[i, 'predicate_invariable_part'] = \
                        get_invariable_part_of_template(str(mapping_rule['predicate_template']))
                else:
                    logging.error("Could not get the invariable part of the predicate for mapping rule `" +
                                  str(mapping_rule['id']) + "`.")

            if 'o' in self.config.get(constants.CONFIG_SECTION, 'mapping_partitions'):
                if pd.notna(mapping_rule['object_constant']):
                    self.mappings_df.at[i, 'object_invariable_part'] = str(mapping_rule['object_constant'])
                elif pd.notna(mapping_rule['object_template']):
                    self.mappings_df.at[i, 'object_invariable_part'] = \
                        get_invariable_part_of_template(str(mapping_rule['object_template']))
                elif pd.notna(mapping_rule['object_reference']):
                    if pd.notna(mapping_rule['object_language']):
                        self.mappings_df.at[i, 'object_invariable_part'] = '""@' + str(mapping_rule['object_language'])
                    elif pd.notna(mapping_rule['object_datatype']):
                        self.mappings_df.at[i, 'object_invariable_part'] = '""^^' + str(mapping_rule['object_datatype'])
                    else:
                        pass    # no invariable part
                elif pd.notna(mapping_rule['object_parent_triples_map']):
                    pass    # no invariable part
                    # mapping partitions could be extended with URI invariable part of parent triples map
                else:
                    logging.error("Could not get the invariable part of the object for mapping rule `" +
                                  str(mapping_rule['id']) + "`.")

            if 'g' in self.config.get(constants.CONFIG_SECTION, 'mapping_partitions'):
                if pd.notna(mapping_rule['graph_constant']):
                    self.mappings_df.at[i, 'graph_invariable_part'] = str(mapping_rule['graph_constant'])
                elif pd.notna(mapping_rule['graph_template']):
                    self.mappings_df.at[i, 'graph_invariable_part'] = \
                        get_invariable_part_of_template(str(mapping_rule['graph_template']))
                else:
                    logging.error("Could not get the invariable part of the graph for mapping rule `" +
                                  str(mapping_rule['id']) + "`.")
