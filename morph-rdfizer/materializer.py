import logging
import pandas as pd


def _materialize_mapping_rule(mapping_rule):
    mapping_result = pd.DataFrame(columns=['subject', 'predicate', 'object'])
    if mapping_rule['subject_template']:
        pass
    elif mapping_rule['subject_reference']:
        pass
    elif mapping_rule['subject_constant']:
        pass
    else:
        raise



def materialize(mappings_df):
    mappings_df.to_csv('out.csv', index=False)
    mapping_partitions = [group for _, group in mappings_df.groupby(by='mapping_partition')]

    for mapping_partition in mapping_partitions:
        for i, mapping_rule in mapping_partition.iterrows():
            mapping_result = _materialize_mapping_rule(mapping_rule)

