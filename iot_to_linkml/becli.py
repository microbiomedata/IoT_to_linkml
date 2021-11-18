import re
from io import StringIO

import click
import numpy as np
import pandas as pd
import yaml
from linkml.generators import yamlgen
from linkml_runtime.utils.schemaview import SchemaView

import iot_to_linkml.sheet2yaml as s2y

dupe_unresolved_filename = "iot_duplciated_names.tsv"

# TODO review these with other stakeholders
mixs_uri = "https://gensc.org/mixs/"
emsl_uri = "https://www.emsl.pnnl.gov/"

hardcoded_prefixes = {"MIXS": mixs_uri, "IoT": emsl_uri}

required_categories = ["sample identification", "required", "required where applicable"]
recommended_categories = []


# def coalesce_package_names(df, orig_col_name,
#                            repaired_col_name,
#                            coalesced="coalesced", ):
#     df[coalesced] = df[repaired_col_name]
#     df[coalesced].loc[
#         df[coalesced] == ""
#         ] = df[orig_col_name].loc[df[coalesced] == ""]
#     return df


@click.command()
@click.option('--cred', default='google_api_credentials.json', help="path to google_api_credentials.json",
              type=click.Path(exists=True), show_default=True)
@click.option('--mixs', default='../mixs-source/model/schema/mixs.yaml', help="path to mixs.yaml and friends",
              type=click.Path(exists=True), show_default=True)
@click.option('--yamlout', default='iot.yaml', help="YAML output file name",
              type=click.Path(), show_default=True)
@click.option('--idcol', default='Globally Unique ID', help="this column will get unique validation in DH",
              show_default=True)
def make_iot_yaml(cred, mixs, yamlout, idcol):
    """Command line wrapper for converting the Index of Terms into LinkML,
    for subsequent conversion into a DataHarmonizer template."""

    mixs_view = SchemaView(mixs)
    mixs_slotnames = list(mixs_view.all_slots().keys())
    mixs_slotnames.sort()

    mixs_classnames = list(mixs_view.all_classes().keys())
    mixs_classnames.sort()

    mixs_parent_classes = set()

    iot_parent_slots = set()

    # especially interested in those with mixins but not subclasses of anything else?
    # can also get packages from some enum?
    # mixs_view.all_enums()
    for one_mc in mixs_classnames:
        current_class = mixs_view.get_class(one_mc)
        current_parent = current_class.is_a
        if current_parent is not None:
            mixs_parent_classes.add(current_parent)

    mixs_parent_classes = list(mixs_parent_classes)
    mixs_parent_classes.sort()

    print(f"Getting credentials from {cred}")

    ctf = s2y.get_iot_controlled_terms_frame()
    ct_dol = s2y.get_ct_dol(ctf)
    ct_keys = s2y.get_ct_keys(ct_dol)

    my_iot_glossary_frame = s2y.get_iot_glossary_frame(client_secret_file="google_api_credentials.json")

    # # TIDY UP SLOT NAMES AND RECONCILE WITH MIXS (PREFERRING MIXS)
    # # SHOULD USE coalesced FROM HERE ON OUT
    # # replace leading ?s in slot names with Q
    # # apply any other name tidying?
    # my_iot_glossary_frame['no_quest'] = my_iot_glossary_frame['name'].str.replace(r'^\?+', 'Q', regex=True)
    # my_iot_glossary_frame = coalesce_package_names(my_iot_glossary_frame, "no_quest", "mixs_6_slot_name", "coalesced")

    # CONVERT PACKAGE "LIST" string, INCLUDING "all" ALIAS
    # TO REAL LISTS
    # CHECK FOR DUPLICATE NAMES AFTER THAT
    # AND THEN EXPLODE FOR UNIQUE SLOT NAME/PACKAGE ROWS
    iot_packages = list(my_iot_glossary_frame['Associated Packages'].unique())
    iot_packages = [i for i in iot_packages if i not in ['all', '', None]]
    iot_packages = [re.split('; *', i) for i in iot_packages]
    iot_packages = [item for sublist in iot_packages for item in sublist]
    iot_packages = list(set(iot_packages))
    iot_packages.sort()
    all_packages_str = '; '.join(iot_packages)
    my_iot_glossary_frame['explicit_packs'] = my_iot_glossary_frame['Associated Packages']
    my_iot_glossary_frame['explicit_packs'].loc[
        my_iot_glossary_frame['Associated Packages'].eq('all')] = all_packages_str
    my_iot_glossary_frame['packlist'] = my_iot_glossary_frame['explicit_packs'].str.split(pat='; *')

    # are there any rows that share names?
    dupe_search = my_iot_glossary_frame['name'].value_counts()
    dupe_yes = dupe_search.loc[dupe_search > 1]
    dupe_yes_slots = dupe_yes.index
    dupe_yes_frame = my_iot_glossary_frame.loc[my_iot_glossary_frame['name'].isin(dupe_yes_slots)]
    dupe_no_frame = my_iot_glossary_frame.loc[~ my_iot_glossary_frame['name'].isin(dupe_yes_slots)]
    dupe_unresolved_frame = pd.DataFrame(columns=dupe_no_frame.columns)
    # working agreement with Montana:
    #   if there are two rows with the same name, use the one with the larger number of packages
    #   if that is inclusive of the other row
    #   no attributes from the discarded row will be propagated

    # FOLLOWING CODE prints a report to the screen and saves dataframe dupe_unresolved_frame,
    #   which can be used to update or delete rows
    # could also use slot_usages to customize per-class (package) slot usage
    print("\n")
    dysl = list(dupe_yes_slots)
    dysl.sort()
    for i in dysl:
        print(f"{i} defined on more than one row.")
        per_slot_frame = dupe_yes_frame.loc[dupe_yes_frame['name'].eq(i)]
        dupe_unresolved_frame = dupe_unresolved_frame.append(per_slot_frame)
        dupe_row_count = len(per_slot_frame.index)
        if dupe_row_count > 2:
            print("Actually defined on two or more rows! Discarding.")
            break
        packlists = list(per_slot_frame["packlist"])
        pl0 = packlists[0]
        pl0_len = len(pl0)
        pl1 = packlists[1]
        pl1_len = len(pl1)
        pl0_only = set(pl0) - set(pl0)
        pl1_only = set(pl1) - set(pl0)
        if pl0_len > pl1_len:
            print("Row 0 has more packages")
            pl1_only = set(pl1) - set(pl0)
            if len(pl1_only) > 0:
                print(f"But discarding because only row 1 contains {pl1_only}")
            else:
                print("and includes all row 1 packages")
                temp = per_slot_frame.iloc[[0]]
                dupe_no_frame = dupe_no_frame.append(temp)
        elif pl0_len < pl1_len:
            print("Row 1 has more packages")
            if len(pl0_only) > 0:
                print(f"But discarding because only row 0 contains {pl0_only}")
            else:
                print("and includes all row 0 packages")
                temp = per_slot_frame.iloc[[1]]
                dupe_no_frame = dupe_no_frame.append(temp)
        elif pl0_len == pl1_len == 0:
            print("Both rows have 0 packages")
        else:
            print("Both rows have the same, non-zero number of packages")
            if len(pl0_only) > 0:
                print(f"but only row 0 contains packages: {pl0_only}")
            elif len(pl1_only) > 0:
                print(f"but only row 1 contains packages: {pl1_only}")
            else:
                print("and both rows contain the same packages")
                temp = per_slot_frame.iloc[[0]]
                dupe_no_frame = dupe_no_frame.append(temp)
        print("\n")
    dupe_unresolved_frame.to_csv(dupe_unresolved_filename, index=False, sep="\t")

    # DUPLICATE SLOT NAME/PACKAGE ROWS HAVE BEEN RESOLVED, SO NOW EXPLODE
    iot_glossary_exploded = dupe_no_frame.explode('packlist')

    made_yaml = s2y.initialize_yaml()

    # create YAML that says which slots go with which packages
    # we are looping over the IoT slots
    # and what are we gathering in all_used_iot_slots?
    collected_classes = {}
    all_used_iot_slots = set()
    for package in iot_packages:
        package_details_row = iot_glossary_exploded.loc[iot_glossary_exploded['packlist'].eq(package)]
        pack_slots = []
        # slot_usages = {}
        sorted_packages = list(package_details_row['name'])
        sorted_packages.sort()
        for slot in sorted_packages:
            pack_slots.append(slot)
            all_used_iot_slots.add(slot)
        collected_classes[package] = {'slots': pack_slots}

    made_yaml['classes'] = collected_classes
    # how is this different from the mixs slot list or the IoT slot list?
    all_used_iot_slots = list(all_used_iot_slots)
    all_used_iot_slots.sort()

    enums = {}

    model_slots = {}
    ranges = []
    for slot in all_used_iot_slots:
        model_slots[slot] = {}
        slot_details = dupe_no_frame.loc[dupe_no_frame['name'].eq(slot)].to_dict(orient="records")
        if len(slot_details) == 1:
            sd_row = slot_details[0]
        annotations = []

        if slot in mixs_slotnames:
            # when to take value as is
            #   when to explicitly cast to str
            #   when to iterate?
            mixs_slot_def = mixs_view.get_slot(slot)
            model_slots[slot]['comments'] = []
            for one_comment in mixs_slot_def.comments:
                model_slots[slot]['comments'].append(str(one_comment))
            model_slots[slot]['conforms_to'] = mixs_uri
            model_slots[slot]['description'] = str(mixs_slot_def.description)
            model_slots[slot]['examples'] = []
            for one_example in mixs_slot_def.examples:
                temp = {"value": str(one_example['value'])}
                model_slots[slot]['examples'].append(temp)
            model_slots[slot]['notes'] = mixs_slot_def.notes

            mrq = mixs_slot_def.required
            mrc = mixs_slot_def.recommended
            irq = sd_row['Category'] in required_categories
            irc = sd_row['Category'] in recommended_categories

            if mrq or irq:
                # model_slots[slot]["is_a"] = "required"
                # iot_parent_slots.add("required")
                model_slots[slot]['required'] = True
            elif mrc or irc:
                # model_slots[slot]["is_a"] = "required where applicable"
                # iot_parent_slots.add("required where applicable")
                model_slots[slot]['recommended'] = True
            else:
                pass
                # model_slots[slot]["is_a"] = "optional"
                # iot_parent_slots.add("optional")

            model_slots[slot]["is_a"] = sd_row['Category']
            iot_parent_slots.add(sd_row['Category'])

            # don't assert a range that isn't already defined as an element
            # some ranges will be enums
            # does IoT overwrite them?
            model_slots[slot]['range'] = str(mixs_slot_def.range)
            current_range = str(mixs_slot_def.range)
            ranges.append(current_range)

            model_slots[slot]['slot_uri'] = str(mixs_slot_def.slot_uri)
            model_slots[slot]['see_also'] = str(mixs_slot_def.see_also)
            model_slots[slot]['title'] = str(mixs_slot_def.title)
            model_slots[slot]['pattern'] = str(mixs_slot_def.pattern)
            model_slots[slot]['multivalued'] = str(mixs_slot_def.multivalued)

        else:
            if len(slot_details) == 1:
                if sd_row['Category'] in required_categories:
                    model_slots[slot]['required'] = True
                elif sd_row['Category'] in recommended_categories:
                    model_slots[slot]['recommended'] = True
                else:
                    pass

                if sd_row['Category'] != "" and sd_row['Category'] is not None:
                    model_slots[slot]["is_a"] = sd_row['Category']
                    # iot_parent_slots.add(sd_row['Category'])
                else:
                    pass
                    # model_slots[slot]["is_a"] = "other"
                    # iot_parent_slots.add("other")

                if sd_row['Notes'] != "":
                    model_slots[slot]['notes'] = sd_row['Notes']
                if sd_row['Origin'] == "EMSL":
                    model_slots[slot]['conforms_to'] = emsl_uri
                if sd_row['syntax'] != "":
                    model_slots[slot]['pattern'] = sd_row['syntax']
                model_slots[slot]['description'] = sd_row['Definition']
                model_slots[slot]['slot_uri'] = "IoT:" + slot

        # what if len(slot_details) != 1 ???
        if len(slot_details) == 1:
            # allow IoT "Column Header" to override title
            if sd_row['Column Header'] != "" and sd_row['Column Header'] is not None:
                # temp = {"local_name_source": "IoT", "local_name_value": sd_row['Column Header']}
                # model_slots[slot]['local_names'] = temp
                # print(model_slots[slot])
                if 'alias' in model_slots[slot]:
                    print('alias')
                    # print(model_slots[slot]['alias'])
                if 'aliases' in model_slots[slot]:
                    print('aliases')
                    # print(model_slots[slot]['aliases'])
                if 'local_names' in model_slots[slot]:
                    print('local names')
                    # print(model_slots[slot]['local_names'])
                if "title" in list(model_slots[slot].keys()):
                    prev = model_slots[slot]['title']
                    if prev != sd_row['Column Header']:
                        annotations.append({"overwritten_title": prev})
                        # todo make it an alias
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

            # if sd_row['name'] != '' and sd_row['mixs_6_slot_name'] != '' and sd_row['name'] != sd_row[
            #     'mixs_6_slot_name']:
            #     if "aliases" in list(model_slots[slot].keys()):
            #         prev = model_slots[slot]['aliases']
            #         prev = "|".join(prev)
            #         # not checking for opportunities to omit a useless annotation
            #         annotations.append({"overwritten_aliases": prev})
            #     model_slots[slot]['aliases'] = sd_row['name']
            #     # model_slots[slot]['aliases'] = sd_row['name']

            if sd_row["Column Header"] == idcol:
                # # print(f"{sd_row['Column Header']} is the unique ID col")
                # annotations.append({"unique_id": True})
                model_slots[slot]['identifier'] = True

        model_slots[slot]['annotations'] = annotations

        # process enums
        # identify and uniqify enums with duplicate permitted values
        if slot in ct_keys:
            current_pvs = ct_dol[slot]
            current_pvs.sort()
            values, counts = np.unique(current_pvs, return_counts=True)
            any_over = any(guilty for guilty in counts if guilty > 1)
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
    for one_range in ranges:
        type_attempt = mixs_view.get_type(one_range)
        class_attempt = mixs_view.get_class(one_range)
        mixs_enum_attempt = mixs_view.get_enum(one_range)
        mixs_enum_finding = mixs_enum_attempt is not None
        iot_enum_finding = one_range in made_yaml_enums
        if mixs_enum_finding:
            # both
            if iot_enum_finding:
                pass
            # mixs only
            else:
                print(f"{one_range} only defined in MIxS")
                yaml_string = yamlgen.as_yaml(mixs_enum_attempt)
                s = StringIO(yaml_string)
                loaded_yaml = yaml.safe_load(s)
                made_yaml['enums'][one_range] = loaded_yaml
        else:
            # iot only
            if iot_enum_finding:
                pass
            # neither !?
            else:
                pass

        if type_attempt is not None:
            # yaml_string = yamlgen.as_yaml(type_attempt)
            # s = StringIO(yaml_string)
            # loaded_yaml = yaml.safe_load(s)
            # # assume all types are from linkml anyway?
            # made_yaml['types'][one_range] = loaded_yaml
            pass
        if class_attempt is not None:
            yaml_string = yamlgen.as_yaml(class_attempt)
            s = StringIO(yaml_string)
            loaded_yaml = yaml.safe_load(s)
            made_yaml['classes'][one_range] = loaded_yaml

    print("\n")

    # use slot usage in cases where a slot name appears on two rows,
    #   with completely different packages on the two rows?
    # made_yaml['classes']['soil']['slot_usage'] = {"samp_name": {'required': True, 'aliases': ['specimen moniker 2']}}

    for k, v in hardcoded_prefixes.items():
        print(f"expanding prefix {k} as {v}")
        made_yaml['prefixes'][k] = v
    print("\n")

    # iot_parent_slots = list(iot_parent_slots)

    iot_parent_slots = list(set(list(my_iot_glossary_frame['Category'])))
    iot_parent_slots = [slot for slot in iot_parent_slots if slot != "" and slot is not None]

    iot_parent_slots.sort()
    print("parent slots:")
    for one_parent in iot_parent_slots:
        print(f"  {one_parent}")
        made_yaml['slots'][one_parent] = {}

    with open(yamlout, 'w') as outfile:
        yaml.dump(made_yaml, outfile, default_flow_style=False, sort_keys=False)


if __name__ == '__main__':
    make_iot_yaml()
