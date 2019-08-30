#1 - Group data based on {FlowName,Compartment} combination
#2 - For each group find datapoints, each data point should have 5 values
#3 - Make 1 figure for every combination of {FlowName,Compatment}
#4 - save as jpeg

import pandas as pd
import matplotlib.pyplot as plt
import os
import re
import seaborn as sns

from electricitylci.globals import output_dir#,model_name

def get_valid_filename(s):
    """From Django framework"""
    s = str(s).strip().replace(' ', '_')
    return re.sub(r'(?u)[^-\w.]', '', s)


def create_generation_ef_boxplots(gen_df):
    sns.set_style("darkgrid")
    plt.rcParams["figure.figsize"] = [16,9]
    verbose = True
    #def main(verbose, input_file, output_folder):

    output_folder = output_dir + model_name + '_generation_ef_boxplots/'
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
        if verbose: print("{} not found. Creating new directory {}".format(output_folder, output_folder))

#Use gen
# Group by str(FlowName) + "_" + str(Compartment)
    gen_df = gen_df.assign(PlotName=lambda x: x.FlowName + "_" + x.Compartment)
    grouped = gen_df.groupby(["PlotName", "FuelCategory"])

    if verbose: print("Processing data")
    plot_data = {}
    max_len = 0
    for name, group in grouped:
        # print(name, group)
        plot_name = name[0]
        fuel_type = name[1]
        # print(list(group.Emission_factor))
        # break
        col_list = list(group.Emission_factor)
        if len(col_list) > max_len: max_len = len(col_list)

        if plot_name not in plot_data:
            dat_len = len(col_list)
            plot_data[plot_name] = pd.DataFrame(data={"fuel": [fuel_type]*dat_len, "emission": col_list})
        else:
            dat_len = len(col_list)
            plot_data[plot_name] = pd.concat([plot_data[plot_name], pd.DataFrame(data={"fuel": [fuel_type]*dat_len, "emission": col_list})])


    #slug = os.path.splitext( os.path.split(input_file)[1] )[0]

    if verbose: print("Plotting and saving figures")
    for plot_name in plot_data.keys():
        print(plot_data[plot_name])
        ax = sns.boxplot(x="fuel", y="emission", data=plot_data[plot_name]).set_title(plot_name)

        plt.ylabel("emission factor mean across eGRID subregions (kg/MWh)")
        plt.setp(plt.xticks()[1], rotation=30, ha='right')
        if verbose: print(os.path.join(output_folder, model_name + "_" + get_valid_filename(plot_name) + ".png"))
        if verbose: print(plot_name, plot_data[plot_name].keys())

        try:
            plt.savefig(os.path.join(output_folder, model_name + "_" + get_valid_filename(plot_name) + ".png"))
        except OSError:
            pass
        plt.cla()
        plt.clf()
        # plt.show()

    print("Process complete. Please check the output folder")


