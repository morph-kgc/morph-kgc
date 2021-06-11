__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import constants
import logging
import pandas as pd


def get_invariant_of_template(template):
    """
    Retrieves the part of the template before the first reference. This part of the template does not depend on
    reference and therefore is invariable. If the template has no references, it is an invalid template, and an
    exception is thrown.
    """

    template_for_splitting = template.replace('\\{', constants.AUXILIAR_UNIQUE_REPLACING_STRING)
    if '{' in template_for_splitting:
        invariant_of_template = template_for_splitting.split('{')[0]
        invariant_of_template = invariant_of_template.replace(constants.AUXILIAR_UNIQUE_REPLACING_STRING,
                                                                          '\\{')
    else:
        # no references were found in the template, and therefore the template is invalid
        raise Exception("Invalid template `" + template + "`. No pairs of unescaped curly braces were found.")

    return invariant_of_template


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
        Generates the mapping partition for the mapping rules in the input DataFrame based on the provided mapping
        partitioning criteria. A new column in the DataFrame is added indicating the mapping partition assigned to every
        mapping rule.
        """

        if self.config.get_mapping_partition() == constants.PARTIAL_AGGREGATION_PARTITION:
            self._get_term_invariants()
            self._generate_partial_aggregations_partition()
        elif self.config.get_mapping_partition() == constants.NO_PARTITIONING:
            self.mappings_df['mapping_partition'] = ''

        return self.mappings_df

    def _generate_partial_aggregations_partition(self):
        """
        Generates a mapping partition based on the algorithm presented in XXXXXXXXX.
        """

        # initialize empty mapping partition
        self.mappings_df['subject_partition'] = ''
        self.mappings_df['predicate_partition'] = ''
        self.mappings_df['object_partition'] = ''
        self.mappings_df['graph_partition'] = ''

        # generate partial mapping partition for subjects, predicates, objects and graphs

        self.mappings_df['literal_type'] = self.mappings_df['object_language'] + self.mappings_df['object_datatype']

        # ---------------------------- SUBJECT ----------------------------

        # sort the mapping rules based on the subject invariants. An invariant that starts with another
        # invariant is placed behind in the ordering e.g. http://example.org/term/something and
        # http://example.org/term: http://example.org/term is placed first
        self.mappings_df.sort_values(by=['subject_invariant'], inplace=True, ascending=True)

        current_group = 0
        current_invariant = constants.AUXILIAR_UNIQUE_REPLACING_STRING

        # iterate over the mapping rules and check if the invariant starts with the invariant of the previous rule
        # if it does, then it is in the same mapping partition than the previous mapping rule
        # if it does not, then the mapping rule is in a new mapping partition
        for i, mapping_rule in self.mappings_df.iterrows():
            if mapping_rule['subject_termtype'] == constants.R2RML_BLANK_NODE:
                self.mappings_df.at[i, 'subject_partition'] = '0'
            elif mapping_rule['subject_invariant'].startswith(current_invariant):
                self.mappings_df.at[i, 'subject_partition'] = str(current_group)
            else:
                current_group = current_group + 1
                current_invariant = mapping_rule['subject_invariant']
                self.mappings_df.at[i, 'subject_partition'] = str(current_group)

        # ---------------------------- PREDICATE ----------------------------

        self.mappings_df.sort_values(by='predicate_invariant', inplace=True, ascending=True)

        current_group = 0
        current_invariant = constants.AUXILIAR_UNIQUE_REPLACING_STRING

        # if all predicates are constant terms we can use full string comparison instead of startswith
        enforce_invariant_non_subset = self.mappings_df['predicate_constant'].notna().all()
        if enforce_invariant_non_subset:
            logging.debug('All predicate maps are constant-valued, invariant subset is not enforced.')

        for i, mapping_rule in self.mappings_df.iterrows():
            if enforce_invariant_non_subset and mapping_rule['predicate_invariant'] == current_invariant:
                self.mappings_df.at[i, 'predicate_partition'] = str(current_group)
            elif not enforce_invariant_non_subset and mapping_rule['predicate_invariant'].startswith(current_invariant):
                self.mappings_df.at[i, 'predicate_partition'] = str(current_group)
            else:
                current_group = current_group + 1
                current_invariant = mapping_rule['predicate_invariant']
                self.mappings_df.at[i, 'predicate_partition'] = str(current_group)

        # ---------------------------- OBJECT ----------------------------

        self.mappings_df.sort_values(by=['object_termtype', 'literal_type', 'object_invariant'],
                                     inplace=True, ascending=True)

        current_group = 0
        current_literal_type = constants.AUXILIAR_UNIQUE_REPLACING_STRING
        current_invariant = constants.AUXILIAR_UNIQUE_REPLACING_STRING

        for i, mapping_rule in self.mappings_df.iterrows():
            if mapping_rule['object_termtype'] == constants.R2RML_BLANK_NODE:
                self.mappings_df.at[i, 'object_partition'] = '0'
            elif mapping_rule['object_termtype'] == constants.R2RML_LITERAL:
                if mapping_rule['literal_type'] != current_literal_type:
                    current_group = current_group + 1
                    current_literal_type = mapping_rule['literal_type']
                self.mappings_df.at[i, 'object_partition'] = current_group
            elif mapping_rule['object_invariant'].startswith(current_invariant):
                self.mappings_df.at[i, 'object_partition'] = str(current_group)
            else:
                current_group = current_group + 1
                current_invariant = mapping_rule['object_invariant']
                self.mappings_df.at[i, 'object_partition'] = str(current_group)

        # ---------------------------- GRAPH ----------------------------

        self.mappings_df.sort_values(by='graph_invariant', inplace=True, ascending=True)

        current_group = 0
        current_invariant = constants.AUXILIAR_UNIQUE_REPLACING_STRING

        # if all graph are constant terms we can use full string comparison instead of startswith
        enforce_invariant_non_subset = self.mappings_df['graph_constant'].notna().all()
        if enforce_invariant_non_subset:
            logging.debug('All graph maps are constant-valued, invariant subset is not enforced.')

        for i, mapping_rule in self.mappings_df.iterrows():
            if enforce_invariant_non_subset and mapping_rule['graph_invariant'] == current_invariant:
                self.mappings_df.at[i, 'graph_partition'] = str(current_group)
            elif not enforce_invariant_non_subset and mapping_rule['graph_invariant'].startswith(current_invariant):
                self.mappings_df.at[i, 'graph_partition'] = str(current_group)
            else:
                current_group = current_group + 1
                current_invariant = mapping_rule['graph_invariant']
                self.mappings_df.at[i, 'graph_partition'] = str(current_group)

        # aggregate the independent mapping partition generated for subjects, predicates and graphs to generate the
        # final mapping partition
        self.mappings_df['mapping_partition'] = self.mappings_df['subject_partition'].astype(str) + '-' + \
            self.mappings_df['predicate_partition'].astype(str) + '-' + \
            self.mappings_df['object_partition'].astype(str) + '-' + self.mappings_df['graph_partition'].astype(str)

        # drop the auxiliary columns that were created just to generate the mapping partition
        self.mappings_df.drop([
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

        logging.info(str(len(set(self.mappings_df['mapping_partition']))) + ' mapping partition generated.')
        logging.info('Maximum number of rules within mapping group: ' + str(
            self.mappings_df['mapping_partition'].value_counts()[0]) + '.')

    def _get_term_invariants(self):
        """
        Adds in the input DataFrame new columns for the invariants of mapping rules. Columns for the invariants of
        subjects, predicates and graphs are added, and they are completed based on the provided mapping
        partitioning criteria.
        """

        # initialize empty invariants for all terms
        self.mappings_df['subject_invariant'] = ''
        self.mappings_df['predicate_invariant'] = ''
        self.mappings_df['object_invariant'] = ''
        self.mappings_df['graph_invariant'] = ''

        for i, mapping_rule in self.mappings_df.iterrows():
            # SUBJECT
            if pd.notna(mapping_rule['subject_template']):
                self.mappings_df.at[i, 'subject_invariant'] = \
                    get_invariant_of_template(str(mapping_rule['subject_template']))
            elif pd.notna(mapping_rule['subject_constant']):
                self.mappings_df.at[i, 'subject_invariant'] = str(mapping_rule['subject_constant'])

            # PREDICATE
            if pd.notna(mapping_rule['predicate_constant']):
                self.mappings_df.at[i, 'predicate_invariant'] = str(mapping_rule['predicate_constant'])
            elif pd.notna(mapping_rule['predicate_template']):
                self.mappings_df.at[i, 'predicate_invariant'] = \
                    get_invariant_of_template(str(mapping_rule['predicate_template']))

            # OBJECT
            if pd.notna(mapping_rule['object_constant']):
                self.mappings_df.at[i, 'object_invariant'] = str(mapping_rule['object_constant'])
            elif pd.notna(mapping_rule['object_template']):
                self.mappings_df.at[i, 'object_invariant'] = \
                    get_invariant_of_template(str(mapping_rule['object_template']))
            elif pd.notna(mapping_rule['object_parent_triples_map']):
                pass    # no invariant
                # mapping partition could be extended with URI invariant of parent triples map

            # GRAPH
            if pd.notna(mapping_rule['graph_constant']):
                self.mappings_df.at[i, 'graph_invariant'] = str(mapping_rule['graph_constant'])
            elif pd.notna(mapping_rule['graph_template']):
                self.mappings_df.at[i, 'graph_invariant'] = \
                    get_invariant_of_template(str(mapping_rule['graph_template']))
