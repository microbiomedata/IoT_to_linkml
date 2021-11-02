import re

import click
import yaml

import badexperiment.sheet2yaml as s2y


# import linkml


@click.command()
# @click.option('--count', default=1, help='Number of greetings.')
# @click.option('--name', prompt='Your name',
#               help='The person to greet.')
@click.option('--cred', default='google_api_credentials.json', help="path to google_api_credentials.json",
              type=click.Path(exists=True))
@click.option('--yamlout', default='iot.yaml', help="YAML output file name",
              type=click.Path())
def make_iot_yaml(cred, yamlout):
    """Command line wrapper for processing the Index of Terms."""
    print(f"Getting credentials from {cred}")

    my_iot_glossary_frame = s2y.get_iot_glossary_frame(client_secret_file="google_api_credentials.json")

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

    iot_glossary_exploded = my_iot_glossary_frame.explode('packlist')

    made_yaml = s2y.make_yaml()

    collected_classes = {}
    all_slots = set()
    for package in all_packages_list:
        package_details_row = iot_glossary_exploded.loc[iot_glossary_exploded['packlist'].eq(package)]
        pack_slots = []
        for slot in package_details_row['name']:
            pack_slots.append(slot)
            all_slots.add(slot)
        collected_classes[package] = {'slots': pack_slots}

    made_yaml['classes'] = collected_classes

    all_slots = list(all_slots)
    all_slots.sort()
    model_slots = {}
    for slot in all_slots:
        model_slots[slot] = {}

    made_yaml['slots'] = model_slots

    made_yaml['classes']['soil']['slot_usage'] = {"samp_name": {'required': True}}

    with open(yamlout, 'w') as outfile:
        yaml.dump(made_yaml, outfile, default_flow_style=False, sort_keys=False)

    # unique_packages = list(iot_glossary_exploded['packlist'].unique())
    #
    # unique_packages = [i for i in unique_packages if i not in ['all', '', None]]
    #
    # unique_packages.sort()
    #
    # print(unique_packages)
    #
    # iot_glossary_exploded['packlist'].loc[iot_glossary_exploded['packlist'].eq('all')] = unique_packages
    #
    # print(iot_glossary_exploded)

    # my_slot_to_pack = s2y.get_slot_to_pack(my_iot_glossary_frame)
    #
    # my_iot_packages = s2y.get_iot_packages(my_slot_to_pack)
    #
    # coalesced_package_names = s2y.coalesce_package_names(my_slot_to_pack)
    #
    # isolated_slot_to_package = s2y.get_pack_to_slot(coalesced_package_names, my_iot_packages)
    #
    # iot_controlled_terms_frame = s2y.get_iot_controlled_terms_frame()
    # ct_dol = s2y.get_ct_dol(iot_controlled_terms_frame)
    # ct_keys = s2y.get_ct_keys(ct_dol)
    #
    # # ----
    #
    # my_iot_glossary_frame["Category"].loc[my_iot_glossary_frame["Category"] == ""] = "optional"
    # my_iot_glossary_frame["Category"].loc[my_iot_glossary_frame["Category"].isnull()] = "optional"
    #
    # slot_categories = list(set(list(my_iot_glossary_frame["Category"])))
    #
    # # expects to have repaired name column in slot_details_df
    # #   which would be my_iot_glossary_frame
    # # looks like there's some slot names that appear on multiple rows due to different use cases
    # # chem_administration
    # # size_frac_up
    # # size_frac_low
    # # other
    # # isol_growth_condt
    #
    # n_to_rn = coalesced_package_names[['name', 'repaired_name']]
    #
    # name_counts = my_iot_glossary_frame['name'].value_counts()
    # dupe_names = name_counts.loc[name_counts > 1]
    # print(dupe_names)
    #
    # my_iot_glossary_frame = pd.merge(left=my_iot_glossary_frame, right=n_to_rn, how="left", on="name")
    #
    # made_yaml = s2y.make_yaml()
    # class_dict = {i: {} for i in list(isolated_slot_to_package['package'])}
    # made_yaml['classes'] = class_dict
    #
    # # should have been a dict of lists
    # for i in isolated_slot_to_package['package']:
    #     print(i)
    #     temp = isolated_slot_to_package[''].loc[isolated_slot_to_package[''].eq(i)]
    #
    # # print(isolated_slot_to_package)
    #
    # # slot_list = n_to_rn.drop_duplicates()
    # # slot_list = list(slot_list['repaired_name'])
    # # slot_list.sort()
    # # slot_dict = {i: {} for i in slot_list}
    # # made_yaml['slots'] = slot_dict
    # #
    # # # check this all against mixs
    # # # don't add anything that mixs already knows
    # # for k, v in slot_dict.items():
    # #     current_row = my_iot_glossary_frame.loc[my_iot_glossary_frame['repaired_name'].eq(k)]
    # #     current_annotations = []
    # #     for ak, av in current_row.items():
    # #         print(f"{ak}: {list(av)}")
    # #         # if ak == "Column Header":
    # #         #     slot_dict[k]['title'] = "hello"
    # #         # if ak == "Definition":
    # #         #     slot_dict[k]['definition'] = list[av]
    # #         # if ak == "Notes":
    # #         #     slot_dict[k]['notes'] = list[av]
    # # #     #     # only add unique non blank entities
    # # #     #     avl = list(av)
    # # #     #     avl = [i for i in avl if i != ""]
    # # #     #     avl = list(set(avl))
    # # #     #     if len(avl) > 0:
    # # #     #         current_annotations.append({ak: avl})
    # # #     # slot_dict[k]['annotations'] = current_annotations
    # # #     pass
    #


if __name__ == '__main__':
    make_iot_yaml()
