import re

import click
import pandas as pd
import numpy as np
import yaml

import badexperiment.sheet2yaml as s2y

# import linkml

dupe_unresolved_filename = "iot_duplciated_names.tsv"


def coalesce_package_names(df, orig_col_name="name", repaired_col_name="mixs_6_slot_name",
                           coalesced="repaired_name", ):
    df[coalesced] = df[repaired_col_name]
    df[coalesced].loc[
        df[coalesced] == ""
        ] = df[orig_col_name].loc[df[coalesced] == ""]
    return df


@click.command()
# @click.option('--count', default=1, help='Number of greetings.')
# @click.option('--name', prompt='Your name',
#               help='The person to greet.')
@click.option('--cred', default='google_api_credentials.json', help="path to google_api_credentials.json",
              type=click.Path(exists=True))
@click.option('--mixs', default='google_api_credentials.json', help="path to mixs.yaml and friends",
              type=click.Path(exists=True))
@click.option('--yamlout', default='iot.yaml', help="YAML output file name",
              type=click.Path())
def make_iot_yaml(cred, yamlout):
    """Command line wrapper for processing the Index of Terms."""
    print(f"Getting credentials from {cred}")

    my_iot_glossary_frame = s2y.get_iot_glossary_frame(client_secret_file="google_api_credentials.json")

    ctf = s2y.get_iot_controlled_terms_frame()

    ct_dol = s2y.get_ct_dol(ctf)

    ct_keys = s2y.get_ct_keys(ct_dol)

    print(ct_keys)

    my_iot_glossary_frame = coalesce_package_names(my_iot_glossary_frame, "name", "mixs_6_slot_name", "coalesced")

    # replace leading ?s in slot names with Q
    raw_name = my_iot_glossary_frame['name']
    no_quest = raw_name.str.replace('^\?+', 'Q', regex=True)
    my_iot_glossary_frame['no_quest'] = no_quest

    temp = list(my_iot_glossary_frame['Associated Packages'].unique())
    temp = [i for i in temp if i not in ['all', '', None]]
    temp = [re.split('; *', i) for i in temp]
    temp = [item for sublist in temp for item in sublist]
    all_packages_list = list(set(temp))
    all_packages_list.sort()
    all_packages_str = '; '.join(all_packages_list)

    my_iot_glossary_frame['explicit_packs'] = my_iot_glossary_frame['Associated Packages']

    my_iot_glossary_frame['explicit_packs'].loc[
        my_iot_glossary_frame['Associated Packages'].eq('all')] = all_packages_str

    my_iot_glossary_frame['packlist'] = my_iot_glossary_frame['explicit_packs'].str.split(pat='; *')

    # are there any rows that share names?
    dupe_search = my_iot_glossary_frame['coalesced'].value_counts()
    dupe_yes = dupe_search.loc[dupe_search > 1]
    dupe_yes_slots = dupe_yes.index
    dupe_yes_frame = my_iot_glossary_frame.loc[my_iot_glossary_frame['coalesced'].isin(dupe_yes_slots)]

    # dupe_no = dupe_search.loc[dupe_search == 1]
    # dupe_no_slots = dupe_no.index
    dupe_no_frame = my_iot_glossary_frame.loc[~ my_iot_glossary_frame['coalesced'].isin(dupe_yes_slots)]

    dupe_unresolved_frame = pd.DataFrame(columns=dupe_no_frame.columns)

    # working agreement with Montana:
    #   if there are two rows with the same name, use the one with the larger number of packages
    #   if that is inclusive of the other row
    #   no attributes from the discarded row will be propagated
    print("\n")
    dysl = list(dupe_yes_slots)
    dysl.sort()
    for i in dysl:
        print(i)
        per_slot_frame = dupe_yes_frame.loc[dupe_yes_frame['name'].eq(i)]
        dupe_unresolved_frame = dupe_unresolved_frame.append(per_slot_frame)
        dupe_row_count = len(per_slot_frame.index)
        if dupe_row_count > 2:
            print("More than two rows with the same name. Discarding.")
            break

        packlists = list(per_slot_frame["packlist"])
        pl0 = packlists[0]
        pl0_len = len(pl0)
        pl1 = packlists[1]
        pl1_len = len(pl1)
        pl0_only = set(pl0) - set(pl0)
        pl1_only = set(pl1) - set(pl0)
        # p_intersection = set(pl0).intersection(set(pl1))

        if pl0_len > pl1_len:
            print("Row 0 has more packages")
            pl1_only = set(pl1) - set(pl0)
            if len(pl1_only) > 0:
                print(f"But only row 1 contains {pl1_only}")
            else:
                print("and includes all row 1 packages")
                temp = per_slot_frame.iloc[[0]]
                dupe_no_frame = dupe_no_frame.append(temp)

        elif pl0_len < pl1_len:
            print("Row 1 has more packages")
            if len(pl0_only) > 0:
                print(f"But only row 0 contains {pl0_only}")
            else:
                print("and includes all row 0 packages")
                temp = per_slot_frame.iloc[[1]]
                dupe_no_frame = dupe_no_frame.append(temp)

        elif pl0_len == pl1_len == 0:
            print("Both rows have 0 packages")

        else:
            print("Both rows have the same, non-zero number of packages")
            intersection_only = True
            # print(p_intersection)
            if len(pl0_only) > 0:
                print(f"but only row 0 contains packages: {pl0_only}")
                intersection_only = False
            elif len(pl1_only) > 0:
                print(f"but only row 1 contains packages: {pl1_only}")
                intersection_only = False
            else:
                print("and both rows contain the same packages")
                temp = per_slot_frame.iloc[[0]]
                dupe_no_frame = dupe_no_frame.append(temp)
        print("\n")

    dupe_unresolved_frame.to_csv(dupe_unresolved_filename, index=False, sep="\t")

    iot_glossary_exploded = dupe_no_frame.explode('packlist')

    made_yaml = s2y.make_yaml()

    collected_classes = {}
    all_slots = set()
    for package in all_packages_list:
        package_details_row = iot_glossary_exploded.loc[iot_glossary_exploded['packlist'].eq(package)]
        pack_slots = []
        slot_usages = {}
        for slot in package_details_row['coalesced']:
            pack_slots.append(slot)
            all_slots.add(slot)
        collected_classes[package] = {'slots': pack_slots}

    made_yaml['classes'] = collected_classes

    enums = {}

    all_slots = list(all_slots)
    all_slots.sort()
    model_slots = {}
    for slot in all_slots:
        model_slots[slot] = {}
        slot_details = dupe_no_frame.loc[dupe_no_frame['coalesced'].eq(slot)].to_dict(orient="records")
        # don't forget duplicated slot names -> per class usage
        # check for matching mixs term
        # check Column Header against ???
        # check definition
        annotations = []
        if len(slot_details) == 1:
            temp = slot_details[0]
            if temp['Column Header'] != "":
                model_slots[slot]['aliases'] = temp['Column Header']
            if temp['Guidance'] != "":
                annotations.append({'guidance': temp['Guidance']})
            if temp['name'] != temp['mixs_6_slot_name']:
                if temp['mixs_6_slot_name'] == "":
                    annotations.append({'supplementary_slot': True})
                else:
                    annotations.append({'source_name': temp['name']})
                    # linkml pattern?
            if temp['syntax'] != "":
                annotations.append({'syntax': temp['syntax']})
            if temp['Category'] != "":
                annotations.append({'category': temp['Category']})
            # look for better LinkML term
            if temp['Origin'] != "":
                annotations.append({'origin': temp['Origin']})
            # are notes internal or external
            if temp['Notes'] != "":
                model_slots[slot]['notes'] = temp['Notes']
            # if temp['GitHub Ticket'] != "":
            #     annotations.append({'ght': temp['GitHub Ticket']})
        model_slots[slot]['annotations'] = annotations
        # change some of these from annotations to slot slots
        # look for enum ranges
        # are any enums duplicated?
        if slot in ct_keys:
            current_pvs = ct_dol[slot]
            current_pvs.sort()
            values, counts = np.unique(current_pvs, return_counts=True)
            any_over = any(i for i in counts if i > 1)
            if any_over:
                print(slot)
                unique_count = len(counts)
                for current_index in range(unique_count):
                    if counts[current_index] > 1:
                        print("  " + values[current_index])
            enum_name = slot + "_enum"
            model_slots[slot]['range'] = enum_name
            current_pvs_set = list(set(current_pvs))
            enums[enum_name] = {"permissible_values": current_pvs_set}

    made_yaml['slots'] = model_slots
    made_yaml['enums'] = enums

    # use slot usage in cases where a slot name appears on two rows,
    #   with completely different packages on the two rows?
    # made_yaml['classes']['soil']['slot_usage'] = {"samp_name": {'required': True, 'aliases': ['specimen moniker 2']}}

    with open(yamlout, 'w') as outfile:
        yaml.dump(made_yaml, outfile, default_flow_style=False, sort_keys=False)


if __name__ == '__main__':
    make_iot_yaml()
