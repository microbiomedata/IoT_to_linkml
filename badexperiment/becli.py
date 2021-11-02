import click
import badexperiment.sheet2yaml as s2y
import pandas as pd
import yaml
import re


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

    my_slot_to_pack = s2y.get_slot_to_pack(my_iot_glossary_frame)

    my_iot_packages = s2y.get_iot_packages(my_slot_to_pack)

    coalesced_package_names = s2y.coalesce_package_names(my_slot_to_pack)

    isolated_slot_to_package = s2y.get_pack_to_slot(coalesced_package_names, my_iot_packages)

    iot_controlled_terms_frame = s2y.get_iot_controlled_terms_frame()
    ct_dol = s2y.get_ct_dol(iot_controlled_terms_frame)
    ct_keys = s2y.get_ct_keys(ct_dol)

    # ----

    my_iot_glossary_frame["Category"].loc[my_iot_glossary_frame["Category"] == ""] = "optional"
    my_iot_glossary_frame["Category"].loc[my_iot_glossary_frame["Category"].isnull()] = "optional"

    slot_categories = list(set(list(my_iot_glossary_frame["Category"])))

    # expects to have repaired name column in slot_details_df
    #   which would be my_iot_glossary_frame
    # looks like there's some slots that appear on different rows due to different use cases
    n_to_rn = coalesced_package_names[['name', 'repaired_name']]
    # print(my_iot_glossary_frame.shape)
    # my_iot_glossary_frame.to_csv("my_iot_glossary_frame_before.csv")
    # print(n_to_rn.shape)
    name_counts = my_iot_glossary_frame['name'].value_counts()
    # print(name_counts)
    # my_iot_glossary_frame.to_csv("temp.csv")
    my_iot_glossary_frame = pd.merge(left=my_iot_glossary_frame, right=n_to_rn, how="left", on="name")
    # print(my_iot_glossary_frame.shape)
    # my_iot_glossary_frame.to_csv("my_iot_glossary_frame_after.csv")

    # template_package(
    #     "soil",
    #     slot_to_package_df=slot_to_pack_4_dh,
    #     slot_details_df=slot_details_4_dh,
    #     enums_dict=ct_dol,
    #     template_prefix=dh_template_prefix,
    #     template_suffix=dh_template_suffix,
    # )

    # # this recreates IoT -> DH
    # # but we really want IoT -> LinkML YAML
    # s2y.template_package(
    #     current_package="soil",
    #     slot_to_package_df=isolated_slot_to_package,
    #     slot_details_df=my_iot_glossary_frame,
    #     enums_dict=ct_dol,
    #     template_prefix=1,
    #     template_suffix=1,
    #     slot_categories=slot_categories,
    #     ct_keys=ct_keys)

    made_yaml = s2y.make_yaml()
    class_dict = {i: {} for i in list(isolated_slot_to_package['package'])}
    made_yaml['classes'] = class_dict

    slot_list = n_to_rn.drop_duplicates()
    slot_list = list(slot_list['repaired_name'])
    # generalize this function
    #   apply earlier on
    #   add raw name back in as alias
    slot_list = [re.sub('^\?+', 'Q', i) for i in slot_list]
    slot_list.sort()
    slot_dict = {i: {} for i in slot_list}
    made_yaml['slots'] = slot_dict

    for k, v in slot_dict.items():
        # print(k)
        temp = my_iot_glossary_frame.loc[my_iot_glossary_frame['repaired_name'].eq(k)]
        ch = list(temp['Column Header'])
        ch = list(set(ch))
        # print(ch)
        made_yaml['slots'][k]['alias'] = ch
        made_yaml['slots'][k]['undefined'] = 'ha ha'

    with open(yamlout, 'w') as outfile:
        yaml.dump(made_yaml, outfile, default_flow_style=False, sort_keys=False)


if __name__ == '__main__':
    make_iot_yaml()
