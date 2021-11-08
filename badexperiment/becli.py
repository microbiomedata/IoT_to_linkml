import re

import click
import yaml

import badexperiment.sheet2yaml as s2y


# import linkml

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
@click.option('--yamlout', default='iot.yaml', help="YAML output file name",
              type=click.Path())
def make_iot_yaml(cred, yamlout):
    """Command line wrapper for processing the Index of Terms."""
    print(f"Getting credentials from {cred}")

    my_iot_glossary_frame = s2y.get_iot_glossary_frame(client_secret_file="google_api_credentials.json")

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

    dupe_search = my_iot_glossary_frame['coalesced'].value_counts()
    dupe_yes = dupe_search.loc[dupe_search > 1]
    dupe_yes_slots = dupe_yes.index
    dupe_yes_frame = my_iot_glossary_frame.loc[my_iot_glossary_frame['coalesced'].isin(dupe_yes_slots)]

    dupe_no = dupe_search.loc[dupe_search == 1]
    dupe_no_slots = dupe_no.index
    dupe_no_frame = my_iot_glossary_frame.loc[~ my_iot_glossary_frame['coalesced'].isin(dupe_yes_slots)]

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
        # change some of these from annotatiosn to slot slots
        # look for enum ranges
        

    made_yaml['slots'] = model_slots

    phase2 = dupe_yes_frame.explode('packlist')

    temp = phase2.loc[phase2['packlist'].eq('water')]
    temp.to_csv("temp.tsv", sep="\t")

    # print(phase2.columns)
    #
    # # Index(['Column Header', 'name', 'mixs_6_slot_name', 'different?', 'Definition',
    # #        'Guidance', 'Expected Value', 'syntax', 'Category',
    # #        'Associated Packages', 'Origin', 'Notes', 'GitHub Ticket', 'coalesced',
    # #        'no_quest', 'explicit_packs', 'packlist'],
    # #       dtype='object')
    #
    # phase2_packages = list(phase2['packlist'].unique())
    # phase2_packages.sort()
    # for i in phase2_packages:
    #     print(i)
    #     temp = phase2.loc[phase2['packlist'].eq(i)]
    #     # t2 = temp.drop(
    #     #     labels=['no_quest', 'explicit_packs', 'Associated Packages', 'different?', 'GitHub Ticket', 'Definition',
    #     #             'Notes'], axis=1, inplace=True)
    #     t2 = temp.drop(
    #         labels=['no_quest', 'explicit_packs', 'Associated Packages', 'different?'], axis=1)
    #     t3 = t2.drop_duplicates()
    #     print(t3)
    #     # slots_per_pack = list(i['coalesced'].unique())
    #     # # slots_per_pack.sort()
    #     # # temp = phase2.loc[phase2['packlist'].eq(i) & phase2['coalesced'].eq(j)]
    #     # # print(temp)

    # made_yaml['classes']['soil']['slot_usage'] = {"samp_name": {'required': True, 'aliases': ['specimen moniker 2']}}

    with open(yamlout, 'w') as outfile:
        yaml.dump(made_yaml, outfile, default_flow_style=False, sort_keys=False)


if __name__ == '__main__':
    make_iot_yaml()
