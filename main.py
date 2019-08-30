import electricitylci

from electricitylci.globals import output_dir
from electricitylci.model_config import model_name, model_specs
from electricitylci.utils import fill_default_provider_uuids


def main():

    # Create dataframe with all generation process data. If the alternate NETL method
    # is being used this will also include upstream and Canadian data.
    print("get generation process")

    print("write generation process to dict")
    if model_specs['use_alt_gen_process'] is True:
        # UUID's for upstream processes are created when converting to JSON-LD. This
        # has to be done here if the information is going to be included in final
        # outputs.
        upstream_df = electricitylci.get_upstream_process_df()
        upstream_dict = electricitylci.write_upstream_process_database_to_dict(
            upstream_df
        )
        upstream_dict = electricitylci.write_upstream_dicts_to_jsonld(upstream_dict)
        generation_process_df = electricitylci.get_generation_process_df(
            upstream_df=upstream_df
        )
        generation_process_dict = electricitylci.write_gen_fuel_database_to_dict(
            generation_process_df, upstream_dict
        )
    else:
        generation_process_df = electricitylci.get_generation_process_df()
        generation_process_dict = (
            electricitylci.write_generation_process_database_to_dict(
                generation_process_df
            )
        )

    print("write gen process to jsonld")
    generation_process_dict = electricitylci.write_process_dicts_to_jsonld(
        generation_process_dict
    )
    print("get gen mix process")
    generation_mix_df = electricitylci.get_generation_mix_process_df()
    print("write gen mix to dict")
    generation_mix_dict = electricitylci.write_generation_mix_database_to_dict(
        generation_mix_df, generation_process_dict)
    print("write gen mix to jsonld")
    generation_mix_dict = electricitylci.write_process_dicts_to_jsonld(
        generation_mix_dict
    )


    # At this point the two methods diverge from underlying functions enough that
    # it's just easier to split here.
    if model_specs['use_alt_gen_process'] is True:
        print("using alt gen method for consumption mix")
        cons_mix_df = electricitylci.get_consumption_mix_df()
        print("write consumption mix to dict")
        cons_mix_dict = electricitylci.write_consumption_mix_to_dict(
            cons_mix_df, generation_mix_dict
        )
        print("write consumption mix to jsonld")
        cons_mix_dict = electricitylci.write_process_dicts_to_jsonld(cons_mix_dict)
        print("get distribution mix")
        dist_mix_df = electricitylci.get_distribution_mix_df(generation_process_df)
        print("write dist mix to dict")
        dist_mix_dict = electricitylci.write_distribution_mix_to_dict(
            dist_mix_df, cons_mix_dict
        )
        print("write dist mix to jsonld")
        dist_mix_dict = electricitylci.write_process_dicts_to_jsonld(dist_mix_dict)
    else:
        #Get surplus and consumption mix dictionary
        sur_con_mix_dict = electricitylci.write_surplus_pool_and_consumption_mix_dict()
        #Get dist dictionary
        dist_dict = electricitylci.write_distribution_dict()
        generation_mix_dict = electricitylci.write_process_dicts_to_jsonld(
            generation_mix_dict
        )
        sur_con_mix_dict = fill_default_provider_uuids(
            sur_con_mix_dict, generation_mix_dict
        )
        sur_con_mix_dict = electricitylci.write_process_dicts_to_jsonld(sur_con_mix_dict)
        sur_con_mix_dict = fill_default_provider_uuids(
            sur_con_mix_dict, sur_con_mix_dict, generation_mix_dict
        )
        sur_con_mix_dict = electricitylci.write_process_dicts_to_jsonld(sur_con_mix_dict)
        dist_dict = fill_default_provider_uuids(dist_dict, sur_con_mix_dict)
        dist_dict = electricitylci.write_process_dicts_to_jsonld(dist_dict)



if __name__ == "__main__":
    main()
