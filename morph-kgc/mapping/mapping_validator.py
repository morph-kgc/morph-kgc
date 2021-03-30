""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright (C) 2020-2021 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import logging
import rfc3987
import constants
import utils
import pandas as pd


class MappingValidator:

    def __init__(self, mappings_df, config):
        self.mappings_df = mappings_df
        self.config = config

    def __str__(self):
        return str(self.mappings_df)

    def __repr__(self):
        return repr(self.mappings_df)

    def __len__(self):
        return len(self.mappings_df)

    def validate_mappings(self):
        """
        Checks that the mapping rules in the input DataFrame are valid. If something is wrong in the mappings the
        execution is stopped. Specifically it is checked that termtypes are correct, constants and templates are valid
        IRIs and that language tags and datatypes are used properly. Also checks that different data sources do not
        have triples map with the same id.
        """

        # check termtypes are correct (i.e. that they are rr:IRI, rr:BlankNode or rr:Literal and that subject map is
        # not a rr:literal). Use subset operation
        if not (set(self.mappings_df['subject_termtype'].astype(str)) <= {constants.R2RML['IRI'],
                                                                          constants.R2RML['blank_node']}):
            raise ValueError('Found an invalid subject termtype. Found values ' +
                             str(set(self.mappings_df['subject_termtype'].astype(str))) +
                             '. Subject maps must be rr:IRI or rr:BlankNode.')

        if not (set(self.mappings_df['object_termtype'].astype(str)) <= {constants.R2RML['IRI'],
                                                                         constants.R2RML['blank_node'],
                                                                         constants.R2RML['literal']}):
            raise ValueError('Found an invalid object termtype. Found values ' +
                             str(set(self.mappings_df['subject_termtype'].astype(str))) +
                             '. Object maps must be rr:IRI, rr:BlankNode or rr:Literal.')

        # if there is a datatype or language tag then the object map termtype must be a rr:Literal
        if len(self.mappings_df.loc[(self.mappings_df['object_termtype'] != constants.R2RML['literal']) &
                                    pd.notna(self.mappings_df['object_datatype']) &
                                    pd.notna(self.mappings_df['object_language'])]) > 0:
            raise Exception('Found object maps with a language tag or a datatype, '
                            'but that do not have termtype rr:Literal.')

        # language tags and datatypes cannot be used simultaneously, language tags are used if both are given
        if len(self.mappings_df.loc[pd.notna(self.mappings_df['object_language']) &
                                    pd.notna(self.mappings_df['object_datatype'])]) > 0:
            logging.warning('Found object maps with a language tag and a datatype. Both of them cannot be used '
                            'simultaneously for the same object map, and the language tag has preference.')

        # check constants are valid IRIs. Get all constants in predicate, graph, subject and object
        constants_terms = list(self.mappings_df['predicate_constant'].dropna())
        constants_terms.extend(list(self.mappings_df['graph_constant'].dropna()))
        constants_terms.extend(list(self.mappings_df.loc[
                                        (self.mappings_df['subject_termtype'] == constants.R2RML['IRI']) &
                                        pd.notna(self.mappings_df['subject_constant'])]['subject_constant']))
        constants_terms.extend(list(self.mappings_df.loc[
                                        (self.mappings_df['object_termtype'] == constants.R2RML['IRI']) &
                                        pd.notna(self.mappings_df['object_constant'])]['object_constant']))
        # validate that each of the constants retrieved are valid URIs
        for constant in set(constants_terms):
            rfc3987.parse(constant, rule='IRI')

        # check templates are valid IRIs. Get all templates in predicate, graph, subject and object
        templates = list(self.mappings_df['predicate_template'].dropna())
        templates.extend(list(self.mappings_df['graph_template'].dropna()))
        templates.extend(list(self.mappings_df.loc[(self.mappings_df['subject_termtype'] == constants.R2RML['IRI']) &
                                                   pd.notna(self.mappings_df['subject_template'])]['subject_template']))
        templates.extend(list(self.mappings_df.loc[(self.mappings_df['object_termtype'] == constants.R2RML['IRI']) &
                                                   pd.notna(self.mappings_df['object_template'])]['object_template']))
        for template in templates:
            # validate that at least the INVARIABLE part of the template is a valid IRI
            rfc3987.parse(utils.get_invariable_part_of_template(str(template)), rule='IRI')

        # check that a triples map id is not repeated in different data sources
        # Get unique source names and triples map identifiers
        aux_mappings_df = self.mappings_df[['source_name', 'triples_map_id']].drop_duplicates()
        # get repeated triples map identifiers
        repeated_triples_map_ids = utils.get_repeated_elements_in_list(
            list(aux_mappings_df['triples_map_id'].astype(str)))
        # of those repeated identifiers
        repeated_triples_map_ids = [tm_id for tm_id in repeated_triples_map_ids]
        if len(repeated_triples_map_ids) > 0:
            raise Exception('The following triples maps appear in more than one data source: ' +
                            str(repeated_triples_map_ids) +
                            '. Check the mapping files, one triple map cannot be repeated in different data sources.')
