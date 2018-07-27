import fedelemflowlist
from electricitylci.globals import fedelemflowlist_version,inventories

flowlist = fedelemflowlist.get_flowlist()
mapping_to_fedelemflows = fedelemflowlist.get_flowmapping(version=fedelemflowlist_version,source_list=None)
mapping_to_fedelemflows = mapping_to_fedelemflows[['Source','OriginalName','OriginalCategory','OriginalProperty',
                                                   'NewName','NewCategory', 'NewSubCategory', 'NewUnit','UUID']]

def map_to_fedelemflows(df_with_flows_compartments):
    mapped_df = pd.merge(df_with_flows_compartments,mapping_to_fedelemflows,
                         left_on=['Source','FlowName','Compartment'],
                         right_on=['Source','OriginalName','OriginalCategory'],
                         how='left')
    #If a NewName is present there was a match, replace FlowName and Compartment with new names
    mapped_df.loc[mapped_df['NewName'].notnull(), 'FlowName'] = mapped_df['NewName']
    mapped_df.loc[mapped_df['NewName'].notnull(), 'Compartment'] = mapped_df['NewCategory']
    mapped_df = mapped_df.rename(columns={'NewUnit':'Unit','UUID':'FlowUUID'})
    #Drop all unneeded cols
    mapped_df = mapped_df.drop(columns=['OriginalName','OriginalCategory','OriginalProperty','NewName','NewCategory','NewSubCategory'])
    return mapped_df




