import re
from io import StringIO

import click
import numpy as np
import pandas as pd
import yaml
from linkml.generators import yamlgen
from linkml_runtime.utils.schemaview import SchemaView

import badexperiment.sheet2yaml as s2y

dupe_unresolved_filename = "iot_duplciated_names.tsv"
mixs_uri = "https://gensc.org/mixs/"
emsl_uri = "https://www.emsl.pnnl.gov/"


# WARNING:YAMLGenerator:File "<file>", line 820, col 15: Unrecognized prefix: IoT
# WARNING:YAMLGenerator:File "<file>", line 838, col 15: Unrecognized prefix: MIXS


def coalesce_package_names(df, orig_col_name="name", repaired_col_name="mixs_6_slot_name",
                           coalesced="repaired_name", ):
    df[coalesced] = df[repaired_col_name]
    df[coalesced].loc[
        df[coalesced] == ""
        ] = df[orig_col_name].loc[df[coalesced] == ""]
    return df


@click.command()
@click.option('--cred', default='google_api_credentials.json', help="path to google_api_credentials.json",
              type=click.Path(exists=True))
@click.option('--mixs', default='../mixs-source/model/schema/mixs.yaml', help="path to mixs.yaml and friends",
              type=click.Path(exists=True))
@click.option('--yamlout', default='iot.yaml', help="YAML output file name",
              type=click.Path())
def make_iot_yaml(cred, mixs, yamlout):
    """Command line wrapper for processing the Index of Terms."""

    mixs_view = SchemaView(mixs)

    mixs_slotnames = []
    mixs_slots = mixs_view.all_slots()
    # there's got to be a better way
    for i in mixs_slots:
        mixs_slotnames.append(i)

    print(f"Getting credentials from {cred}")

    my_iot_glossary_frame = s2y.get_iot_glossary_frame(client_secret_file="google_api_credentials.json")

    ctf = s2y.get_iot_controlled_terms_frame()
    ct_dol = s2y.get_ct_dol(ctf)
    ct_keys = s2y.get_ct_keys(ct_dol)

    my_iot_glossary_frame = coalesce_package_names(my_iot_glossary_frame, "name", "mixs_6_slot_name", "coalesced")

    # replace leading ?s in slot names with Q
    raw_name = my_iot_glossary_frame['name']
    no_quest = raw_name.str.replace('^\?+', 'Q', regex=True)
    my_iot_glossary_frame['no_quest'] = no_quest

    uap = list(my_iot_glossary_frame['Associated Packages'].unique())
    uap = [i for i in uap if i not in ['all', '', None]]
    uap = [re.split('; *', i) for i in uap]
    uap = [item for sublist in uap for item in sublist]

    all_packages_list = list(set(uap))
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

    # this generates a report and an export, which can be used to update or delete rows
    # could also use slot_usages to customize per-class (package) slot usage

    # also report duplicated enums
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
            # intersection_only = True
            if len(pl0_only) > 0:
                print(f"but only row 0 contains packages: {pl0_only}")
                # intersection_only = False
            elif len(pl1_only) > 0:
                print(f"but only row 1 contains packages: {pl1_only}")
                # intersection_only = False
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
        # slot_usages = {}
        for slot in package_details_row['coalesced']:
            pack_slots.append(slot)
            all_slots.add(slot)
        collected_classes[package] = {'slots': pack_slots}

    made_yaml['classes'] = collected_classes

    enums = {}

    all_slots = list(all_slots)
    all_slots.sort()
    model_slots = {}
    ranges = []
    for slot in all_slots:
        model_slots[slot] = {}
        slot_details = dupe_no_frame.loc[dupe_no_frame['coalesced'].eq(slot)].to_dict(orient="records")
        if len(slot_details) == 1:
            sd_row = slot_details[0]
        annotations = []

        if slot in mixs_slotnames:
            # when to take value as is
            # when to explicitly cast to str
            # when to iterate?
            mixs_slot_def = mixs_view.get_slot(slot)
            model_slots[slot]['comments'] = []
            for i in mixs_slot_def.comments:
                model_slots[slot]['comments'].append(str(i))
            model_slots[slot]['conforms_to'] = mixs_uri
            model_slots[slot]['description'] = str(mixs_slot_def.description)
            model_slots[slot]['examples'] = []
            for i in mixs_slot_def.examples:
                temp = {"value": str(i['value'])}
                model_slots[slot]['examples'].append(temp)
            model_slots[slot]['notes'] = mixs_slot_def.notes
            model_slots[slot]['recommended'] = mixs_slot_def.recommended
            # don't assert a range that isn't already defined as an element
            # some ranges will be enums
            # does IoT overwrite them?
            model_slots[slot]['range'] = str(mixs_slot_def.range)
            current_range = str(mixs_slot_def.range)
            ranges.append(current_range)

            model_slots[slot]['required'] = mixs_slot_def.required
            model_slots[slot]['slot_uri'] = str(mixs_slot_def.slot_uri)
            model_slots[slot]['see_also'] = str(mixs_slot_def.see_also)
            model_slots[slot]['title'] = str(mixs_slot_def.title)
            model_slots[slot]['pattern'] = str(mixs_slot_def.pattern)
            model_slots[slot]['multivalued'] = str(mixs_slot_def.multivalued)
            # core attribute in is_a could have gone in category?
        else:
            if len(slot_details) == 1:
                if sd_row['Category'] == "required":
                    model_slots[slot]['required'] = True
                    annotations.append({"Category": 'required where applicable'})
                if sd_row['Category'] == "required where applicable":
                    model_slots[slot]['recommended'] = True
                    annotations.append({"Category": 'required where applicable'})
                if sd_row['Category'] == "sample identification":
                    annotations.append({"Category": 'sample identification'})
                if sd_row['Notes'] != "":
                    model_slots[slot]['notes'] = sd_row['Notes']
                if sd_row['Origin'] == "EMSL":
                    model_slots[slot]['conforms_to'] = emsl_uri
                if sd_row['syntax'] != "":
                    model_slots[slot]['pattern'] = sd_row['syntax']
                model_slots[slot]['description'] = sd_row['Definition']
                model_slots[slot]['slot_uri'] = "IoT:" + slot

        if len(slot_details) == 1:
            # allow IoT "Column Header" to override title
            if sd_row['Column Header'] != "" and sd_row['Column Header'] is not None:
                # temp = {"local_name_source": "IoT", "local_name_value": sd_row['Column Header']}
                # model_slots[slot]['local_names'] = temp
                if "title" in list(model_slots[slot].keys()):
                    prev = model_slots[slot]['title']
                    if prev != sd_row['Column Header']:
                        annotations.append({"overwritten_title": prev})
                model_slots[slot]['title'] = sd_row['Column Header']
            # might not even make it into DH so don't overwrite
            if sd_row['GitHub Ticket'] != "" and sd_row['GitHub Ticket'] is not None:
                annotations.append({"ticket": sd_row['GitHub Ticket']})
            # allow IoT "Guidance" to override comments
            if sd_row['Guidance'] != "" and sd_row['Guidance'] is not None:
                if "comments" in list(model_slots[slot].keys()):
                    prev = model_slots[slot]['comments']
                    prev = "|".join(prev)
                    # hard to believe that IoT Guidance will even match the previous comments
                    #   not checking for opportunities to omit a useless annotation
                    annotations.append({"overwritten_comments": prev})
                    model_slots[slot]['comments'] = sd_row['Guidance']
                    # annotations.append({"Guidance": sd_row['Guidance']})
            if sd_row['name'] != '' and sd_row['mixs_6_slot_name'] != '' and sd_row['name'] != sd_row[
                'mixs_6_slot_name']:
                if "aliases" in list(model_slots[slot].keys()):
                    prev = model_slots[slot]['aliases']
                    prev = "|".join(prev)
                    # not checking for opportunities to omit a useless annotation
                    annotations.append({"overwritten_aliases": prev})
                    model_slots[slot]['aliases'] = sd_row['name']
                    # model_slots[slot]['aliases'] = sd_row['name']

        model_slots[slot]['annotations'] = annotations

        if slot in ct_keys:
            current_pvs = ct_dol[slot]
            current_pvs.sort()
            values, counts = np.unique(current_pvs, return_counts=True)
            any_over = any(i for i in counts if i > 1)
            if any_over:
                print(f"{slot} has duplicated enumerated values")
                unique_count = len(counts)
                for current_index in range(unique_count):
                    if counts[current_index] > 1:
                        print("  " + values[current_index])
            enum_name = slot + "_enum"
            model_slots[slot]['range'] = enum_name
            current_pvs_set = list(set(current_pvs))
            enums[enum_name] = {"permissible_values": current_pvs_set}
    print("\n")

    made_yaml['slots'] = model_slots
    made_yaml['enums'] = enums

    made_yaml_enums = list(made_yaml['enums'].keys())
    made_yaml_enums.sort()

    ranges = list(set(ranges))
    ranges.sort()
    for i in ranges:
        # print(i)
        type_attempt = mixs_view.get_type(i)
        class_attempt = mixs_view.get_class(i)
        mixs_enum_attempt = mixs_view.get_enum(i)
        mixs_enum_finding = mixs_enum_attempt is not None
        iot_enum_finding = i in made_yaml_enums
        if mixs_enum_finding:
            if iot_enum_finding:
                pass
            else:
                print("mixs_only")
                yaml_string = yamlgen.as_yaml(mixs_enum_attempt)
                s = StringIO(yaml_string)
                loaded_yaml = yaml.load(s)
                made_yaml['enums'][i] = loaded_yaml
        else:
            if iot_enum_finding:
                pass
            else:
                pass

        if type_attempt is not None:
            yaml_string = yamlgen.as_yaml(type_attempt)
            s = StringIO(yaml_string)
            loaded_yaml = yaml.load(s)
            # assume all types are from linkml anyway?
            # made_yaml['types'][i] = loaded_yaml
        if class_attempt is not None:
            yaml_string = yamlgen.as_yaml(class_attempt)
            s = StringIO(yaml_string)
            loaded_yaml = yaml.load(s)
            made_yaml['classes'][i] = loaded_yaml

    # use slot usage in cases where a slot name appears on two rows,
    #   with completely different packages on the two rows?
    # made_yaml['classes']['soil']['slot_usage'] = {"samp_name": {'required': True, 'aliases': ['specimen moniker 2']}}

    with open(yamlout, 'w') as outfile:
        yaml.dump(made_yaml, outfile, default_flow_style=False, sort_keys=False)


if __name__ == '__main__':
    make_iot_yaml()
