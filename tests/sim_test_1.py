from components.terminal import Terminal
from components.ec.wi import WI
from components.quay.vessel import Vessel
from components.inventory.container import Container
import pandas as pd
import simpy
import random
import sys
sys.path.append('../')


def df_row_to_wi(row):
    """
    Convert a row from the dataframe to a work instruction object
    """
    tc_fields = ["id", 'category', 'freight_kind', 'line_op']
    dict_container = {field: row[field] for field in tc_fields}
    dict_wi = {field: row[field]
               for field in row.index if field not in tc_fields}
    dict_wi['container_obj'] = Container(**dict_container)
    return WI(**dict_wi)


def generate_pow(count=[10, 10, 10, 10, 10, 10, 10, 10, 10],
                 move_kind_list=["DSCH", "LOAD"]):
    """
    Generate the point of work for the vessel
    count: list of number of instruction to test for each type : 
    [DSCH, LOAD, SHOB, YARD, SHFT, DLVR, RECV, RLOD, RDSC]
    """
    if move_kind_list is None:
        move_kind_list = ["DSCH", "LOAD", "SHOB", "YARD",
                          "SHFT", "DLVR", "RECV", "RLOD", "RDSC"]
    df_wi = pd.read_csv("data/SPARCSN4_WI_clean_13Dec24.csv", header=0)
    cols = ['UFV_GKEY', 'GKEY', 'ID', 'LINE_OP', 'CATEGORY', 'FREIGHT_KIND', 'MOVE_KIND', 'POW', 'CARRIER_VISIT', 'FM_BLOCK', 'FM_BAY', 'FM_ROW', 'FM_TIER',
            'TO_BLOCK', 'TO_BAY', 'TO_ROW', 'TO_TIER']
    vf_cols = [c for c in df_wi.columns if c in cols]
    df_wi = df_wi[vf_cols]
    df_wi.rename(columns={v: v.lower() for v in df_wi.columns}, inplace=True)
    unique_pow = df_wi['pow'].unique()
    pow_dict = {}
    df_pow = pd.DataFrame()
    for pow_name in unique_pow:
        pow_dict[pow_name] = []
        df_tmp = df_wi[df_wi['pow'] == pow_name].copy()
        df_tmp = df_tmp.drop_duplicates(subset=['id'], keep='first')
        df_tmp_f = pd.DataFrame()
        for i, mv_type in enumerate(move_kind_list):
            df_tmp_tmp = df_tmp[df_tmp['move_kind'] == mv_type].head(count[i])
            if not df_tmp_tmp.empty:
                df_tmp_f = pd.concat([df_tmp_f, df_tmp_tmp], ignore_index=True)
        for index, row in df_tmp_f.iterrows():
            pow_dict[pow_name].append(df_row_to_wi(row))
        pow_dict[pow_name].sort(key=lambda wi: wi.id, reverse=False)
        df_pow = pd.concat([df_pow, df_tmp_f], ignore_index=True)
    df_pow_report = df_pow.groupby(['pow', 'move_kind']).size().reset_index(
        name='count').sort_values(by=['pow', 'move_kind'])
    print("- "*50)
    print(df_pow_report)
    print("- "*50)
    return pow_dict, df_pow


def get_n_first_keys(pow_dict, n=3):
    """
    Get the first n keys from the dictionary
    """
    keys = list(pow_dict.keys())
    first_keys = keys[:n]
    return {key: pow_dict[key] for key in first_keys}


def generate_block_dict(df_pow, move_kind_list=["DSCH", "LOAD"]):
    """
    Generate the block dictionary for the yard cranes
    """
    # all_block_list = []
    # for i, mv_type in enumerate(move_kind_list):
    #     if mv_type in ['DSCH', "RDSC"]:
    #         all_block_list += df_pow.loc[df_pow.move_kind == mv_type].to_block.unique()
    #     elif mv_type in ['LOAD', "RLOD"]:
    #         all_block_list += df_pow.loc[df_pow.move_kind == mv_type].fm_block.unique()
    #     elif mv_type in ['YARD', 'SHFT']:
    #         block_list_fm = df_pow.loc[df_pow.move_kind == mv_type].fm_block.unique()
    #         block_list_to = df_pow.loc[df_pow.move_kind == mv_type].to_block.unique()
    #         all_block_list += list(set(block_list_fm).union(set(block_list_to)))
    #     elif mv_type == 'DLVR':
    #         all_block_list += df_pow.loc[df_pow.move_kind == mv_type].fm_block.unique()
    #     elif mv_type == 'RECV':
    #         all_block_list += df_pow.loc[df_pow.move_kind == mv_type].to_block.unique()
    dsch_block = df_pow.loc[df_pow.move_kind == 'DSCH'].to_block.unique()
    load_block = df_pow.loc[df_pow.move_kind == 'LOAD'].fm_block.unique()
    all_block_list = list(set(dsch_block).union(set(load_block)))
    bloc_dict = {}
    yc_id = 1
    for b in all_block_list:
        bloc_dict[f"RTG{yc_id:02d}"] = [b]
        yc_id += 1
    print("- "*50)
    print(bloc_dict)
    print("- "*50)
    return bloc_dict


def run_terminal_activity(env: simpy.Environment, terminal: Terminal, activity_dict: dict):
    """
    Generate the arrialve of ships and notifies the terminal it has
    has arrived and waiting for a birth
    """
    # print("Starting run_terminal_activity")
    i = 0
    while i < len(activity_dict):
        # Get carrier information from the dictionary
        carrier_id = list(activity_dict.keys())[i]
        pow_dict = activity_dict[carrier_id]

        # Process the vessel arrival
        env.process(terminal.initialize_vessel(Vessel, carrier_id, pow_dict))

        # Wait for the next arrival (e.g., after a random interval if desired)
        # Adjust timeout duration as needed
        yield env.timeout(random.expovariate(1.0/(5*60*60)))
        i += 1


def sim():
    """
    init and run the simulation
    """
    print("starting simulation")

    env = simpy.Environment()  # env,  n_itv: int, yc_block_dict: int, pow_dict
    pow_dict, df_pow = generate_pow(
        count=[10, 10], move_kind_list=["DSCH", "LOAD"])
    carrier_visit_id = df_pow['carrier_visit'].unique()[0]
    ex_pow_dict = get_n_first_keys(pow_dict, 3)
    activity_dict = {carrier_visit_id: ex_pow_dict}
    pow_carrier_dict = {k: carrier_visit_id for k in ex_pow_dict.keys()}
    yc_block_dict = generate_block_dict(df_pow)
    terminal = Terminal(env,
                        n_itv=6,
                        yc_block_dict=yc_block_dict,
                        pow_dict=pow_carrier_dict,
                        output_to_csv_file=True
                        )

    env.process(run_terminal_activity(env, terminal, activity_dict))

    env.run(until=8*60*60)  # run for 8 hours

    print(f"{env.now:.2f} simulation has finished")


if __name__ == "__main__":
    sim()
