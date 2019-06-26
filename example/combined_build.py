# -*- coding: utf-8 -*-

import electricitylci
import pandas as pd
import pickle as pkl

upstream_df = electricitylci.get_upstream_process_df()
upstream_df.to_csv(f"upstream_df.csv")
upstream_df = pd.read_csv(f"upstream_df.csv", index_col=0)
upstream_dict = electricitylci.write_upstream_process_database_to_dict(
    upstream_df
)
upstream_dict = electricitylci.write_upstream_dicts_to_jsonld(upstream_dict)
with open("upstream_dict.pickle", "wb") as handle:
    pkl.dump(upstream_dict, handle, protocol=pkl.HIGHEST_PROTOCOL)
with open("upstream_dict.pickle", "rb") as handle:
    upstream_dict = pkl.load(handle)
gen_df = electricitylci.get_alternate_gen_plus_netl()
# The combined DF below should be the final dataframe for generic analysis
combined_df = electricitylci.combine_upstream_and_gen_df(gen_df, upstream_df)
combined_df.to_csv(f"combined_df.csv")
combined_df = pd.read_csv(f"combined_df.csv", index_col=0)
gen_plus_fuels = electricitylci.add_fuels_to_gen(
    gen_df, upstream_df, upstream_dict
)
gen_plus_fuels.to_csv(f"gen_plus_fuels.csv")
gen_plus_fuels = pd.read_csv(f"gen_plus_fuels.csv", index_col=0)
aggregate_df = electricitylci.aggregate_gen(gen_plus_fuels, subregion="US")
aggregate_df.to_csv(f"aggregate_df.csv")
aggregate_df = pd.read_csv(f"aggregate_df.csv", index_col=0)
aggregate_dict = electricitylci.write_gen_fuel_database_to_dict(
    aggregate_df, upstream_dict, subregion="US"
)
with open("aggregate_dict.pickle", "wb") as handle:
    pkl.dump(aggregate_dict, handle, protocol=pkl.HIGHEST_PROTOCOL)
with open("aggregate_dict.pickle", "rb") as handle:
    aggregate_dict = pkl.load(handle)
agregate_dict = electricitylci.write_process_dicts_to_jsonld(aggregate_dict)
