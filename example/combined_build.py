# -*- coding: utf-8 -*-

import electricitylci
import pandas as pd
import pickle as pkl


<<<<<<< HEAD
use_cache = False
subregion = "NERC"
=======
use_cache = True
subregion = "BA"
>>>>>>> 0529a22cd700ef71dc6e86f4f7fa8e0aeec3304b
if use_cache is True:
    with open("upstream_dict.pickle", "rb") as handle:
        upstream_dict = pkl.load(handle)
    upstream_df=pd.read_csv(f"upstream_df.csv")
    upstream_dict = electricitylci.write_upstream_dicts_to_jsonld(upstream_dict)
#    gen_df = electricitylci.get_alternate_gen_plus_netl()
#    combined_df, canadian_gen = electricitylci.combine_upstream_and_gen_df(gen_df, upstream_df)
#    combined_df = pd.read_csv(f"combined_df.csv", index_col=0)
#    gen_plus_fuels = electricitylci.add_fuels_to_gen(
#        gen_df, upstream_df, canadian_gen, upstream_dict
#    )
    gen_plus_fuels = pd.read_csv(f"gen_plus_fuels.csv", index_col=0)
#    aggregate_df = pd.read_csv(f"aggregate_df.csv", index_col=0)
    with open("aggregate_dict.pickle", "rb") as handle:
        aggregate_dict = pkl.load(handle)
#    aggregate_df = electricitylci.aggregate_gen(gen_plus_fuels, subregion=subregion)
#    aggregate_df.to_csv(f"aggregate_df.csv")
#    aggregate_dict = electricitylci.write_gen_fuel_database_to_dict(
#        aggregate_df, upstream_dict, subregion=subregion
#    )
#    with open("aggregate_dict.pickle", "wb") as handle:
#        pkl.dump(aggregate_dict, handle, protocol=pkl.HIGHEST_PROTOCOL)
    aggregate_dict = electricitylci.write_process_dicts_to_jsonld(aggregate_dict)
#    gen_mix_df = electricitylci.get_generation_mix_process_df(regions=subregion)
    gen_mix_df = pd.read_csv(f"gen_mix_df.csv",index_col=0)
#    gen_mix_dict = electricitylci.write_generation_mix_database_to_dict(gen_mix_df,aggregate_dict,regions=subregion)
    with open("gen_mix_dict.pickle","rb") as handle:
        gen_mix_dict = pkl.load(handle)
    gen_mix_dict = electricitylci.write_process_dicts_to_jsonld(gen_mix_dict)
    dist_mix_df = electricitylci.get_distribution_mix_df(gen_plus_fuels,subregion=subregion)
    dist_mix_dict = electricitylci.write_distribution_mix_to_dict(dist_mix_df,gen_mix_dict,subregion=subregion)
    dist_mix_dict = electricitylci.write_process_dicts_to_jsonld(dist_mix_dict)
    with open("dist_mix_dict.pickle", "wb") as handle:
        pkl.dump(dist_mix_dict, handle, protocol=pkl.HIGHEST_PROTOCOL)
    
else:
    upstream_df = electricitylci.get_upstream_process_df()
    upstream_df.to_csv(f"upstream_df.csv")
    upstream_dict = electricitylci.write_upstream_process_database_to_dict(
        upstream_df
    )
    upstream_dict = electricitylci.write_upstream_dicts_to_jsonld(upstream_dict)
    with open("upstream_dict.pickle", "wb") as handle:
        pkl.dump(upstream_dict, handle, protocol=pkl.HIGHEST_PROTOCOL)
    upstream_dict = electricitylci.write_upstream_dicts_to_jsonld(upstream_dict)
    gen_df = electricitylci.get_alternate_gen_plus_netl()
    # The combined DF below should be the final dataframe for generic analysis
    combined_df, canadian_gen = electricitylci.combine_upstream_and_gen_df(gen_df, upstream_df)
    combined_df.to_csv(f"combined_df.csv")
    gen_plus_fuels = electricitylci.add_fuels_to_gen(
        gen_df, upstream_df, canadian_gen, upstream_dict
    )
    gen_plus_fuels.to_csv(f"gen_plus_fuels.csv")
    aggregate_df = electricitylci.aggregate_gen(gen_plus_fuels, subregion=subregion)
    aggregate_df.to_csv(f"aggregate_df.csv")
    aggregate_dict = electricitylci.write_gen_fuel_database_to_dict(
        aggregate_df, upstream_dict, subregion=subregion
    )
    with open("aggregate_dict.pickle", "wb") as handle:
        pkl.dump(aggregate_dict, handle, protocol=pkl.HIGHEST_PROTOCOL)
    aggregate_dict = electricitylci.write_process_dicts_to_jsonld(aggregate_dict)
    gen_mix_df = electricitylci.get_generation_mix_process_df(regions=subregion)
    gen_mix_dict = electricitylci.write_generation_mix_database_to_dict(gen_mix_df,aggregate_dict,regions=subregion)
    gen_mix_dict = electricitylci.write_process_dicts_to_jsonld(gen_mix_dict)
    with open("gen_mix_dict.pickle", "wb") as handle:
        pkl.dump(gen_mix_dict, handle, protocol=pkl.HIGHEST_PROTOCOL)
    dist_mix_df = electricitylci.get_distribution_mix_df(gen_df,subregion=subregion)
    dist_mix_dict = electricitylci.write_distribution_mix_to_dict(dist_mix_df,gen_mix_dict,subregion=subregion)
    dist_mix_dict = electricitylci.write_process_dicts_to_jsonld(dist_mix_dict)
    with open("dist_mix_dict.pickle", "wb") as handle:
        pkl.dump(dist_mix_dict, handle, protocol=pkl.HIGHEST_PROTOCOL)