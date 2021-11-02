import os
import pandas as pd
from shutil import copyfile

# pip install --upgrade google-api-python-client
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# sudo pip install google-auth-oauthlib
from google_auth_oauthlib.flow import InstalledAppFlow

# AttributeError: module 'pyparsing' has no attribute 'downcaseTokens'
# ERROR: httplib2 0.20.1 has requirement pyparsing<3,>=2.4.2, but you'll have pyparsing 3.0.3 which is incompatible.
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# The ID and range of a sample spreadsheet.
IOT_SPREADSHEET_ID = "1lj4OuEE4IYwy2v7RzcG79lHjNdFwmDETMDTDaRAWojY"
IOT_RANGE_NAME = "Glossary of terms!A1:Z"

CV_RANGE_NAME = "Controlled Terms!A1:Z"


# reusing google_api_credentials.json from https://github.com/cancerDHC/sheet2linkml
# client_secret_file = "../google_api_credentials.json"


def get_creds(client_secret_file="../google_api_credentials.json"):
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    return creds


def get_sheet_service(creds):
    service = build("sheets", "v4", credentials=creds)
    # Call the Sheets API
    sheet_service = service.spreadsheets()
    return sheet_service


def get_gsheet_tab(sheet_service, sheet_id, range_name):
    result = (
        sheet_service.values().get(spreadsheetId=sheet_id, range=range_name).execute()
    )
    return result


# # any benefit to doing this without pandas?
# #  lighter weight?
# #  harder to program?

def get_iot_glossary_frame(client_secret_file="../google_api_credentials.json"):
    creds = get_creds(client_secret_file=client_secret_file)
    sheet_service = get_sheet_service(creds)
    iot_glossary_tab = get_gsheet_tab(sheet_service, IOT_SPREADSHEET_ID, IOT_RANGE_NAME)
    iot_glossary_frame = pd.DataFrame(iot_glossary_tab["values"], columns=iot_glossary_tab["values"][0]).drop(0)
    return iot_glossary_frame


def get_iot_controlled_terms_frame(client_secret_file="../google_api_credentials.json"):
    creds = get_creds(client_secret_file=client_secret_file)
    sheet_service = get_sheet_service(creds)
    controlled_terms_tab = get_gsheet_tab(sheet_service, IOT_SPREADSHEET_ID, CV_RANGE_NAME)
    controlled_terms_frame = pd.DataFrame(controlled_terms_tab["values"],
                                          columns=controlled_terms_tab["values"][0]).drop(0)
    return controlled_terms_frame


def get_ct_dol(controlled_terms_frame):
    ct_dol = {k: [i for i in v if i] for (k, v) in controlled_terms_frame.items()}
    return ct_dol


def get_ct_keys(ct_dol):
    ct_keys = list(ct_dol.keys())
    ct_keys.sort()
    return ct_keys


# parameterize column names
def get_slot_to_pack(iot_glossary_frame):
    slot_to_pack = iot_glossary_frame[["name", "mixs_6_slot_name", "Associated Packages"]]
    temp = slot_to_pack["Associated Packages"].str.split(" *; *", expand=False).copy()
    slot_to_pack.loc[:, "ap_list"] = temp
    return slot_to_pack


def get_iot_packages(slot_to_pack):
    iot_packages = list(slot_to_pack["ap_list"])
    iot_packages = [i for i in iot_packages if i]
    iot_packages = sum(iot_packages, [])
    iot_packages = list(set(iot_packages))
    iot_packages.sort()
    iot_packages.remove("")
    iot_packages.remove("all")
    return iot_packages


# this takes Montana's columns names and coalesces them
#  with manually curated MIxS column names where possible
# assumes mixs_6_slot_name column is present
#  that's not the case in the XLSX based IoT Montana created
def coalesce_package_names(slot_to_pack, orig_col_name="name", repaired_col_name="mixs_6_slot_name",
                           coalesced="repaired_name", ):
    slot_to_pack[coalesced] = slot_to_pack[repaired_col_name]
    slot_to_pack[coalesced].loc[
        slot_to_pack[coalesced] == ""
        ] = slot_to_pack[orig_col_name].loc[slot_to_pack[coalesced] == ""]
    return slot_to_pack


def get_pack_to_slot(slot_to_pack, iot_packages, ap_colname="ap_list"):
    slot_to_pack[ap_colname].loc[
        slot_to_pack["Associated Packages"] == "all"
        ] = slot_to_pack["Associated Packages"].apply(lambda _: iot_packages)

    slot_to_pack = slot_to_pack[["repaired_name", ap_colname]]

    slot_to_pack = slot_to_pack.explode(ap_colname)

    slot_to_pack = (
        slot_to_pack.astype(str)
            .groupby(ap_colname, as_index=False)
            .agg(";".join)[slot_to_pack.columns]
    )

    slot_to_pack = slot_to_pack.loc[slot_to_pack[ap_colname].ne("")]

    slot_to_pack = slot_to_pack.loc[slot_to_pack[ap_colname].ne("None")]

    slot_to_pack = slot_to_pack.loc[~slot_to_pack[ap_colname].isnull()]

    slot_to_pack = slot_to_pack[[ap_colname, "repaired_name"]]

    slot_to_pack.columns = ["package", "slots"]
    # print(slot_to_pack)
    return slot_to_pack


# template_package(
#     "soil",
#     slot_to_package_df=slot_to_pack_4_dh,
#     slot_details_df=slot_details_4_dh,
#     enums_dict=ct_dol,
#     template_prefix=dh_template_prefix,
#     template_suffix=dh_template_suffix,
# )

# required columns can be asserted without putting them in a section entitled "required" etc.
#   ie it might be possible to use the section for something orthogonal
# required_sections = ["sample identification", "required", "required where applicable"]


def template_package(
        current_package,
        slot_to_package_df,
        slot_details_df,
        enums_dict,
        template_prefix,
        slot_categories,
        ct_keys,
        required_sections=["sample identification", "required", "required where applicable"],
        dh_template_root="../../DataHarmonizer/template/",
        ref_temp_filename="reference_template.html",
        template_suffix="/data.tsv",
        blank_row={
            "Ontology ID": "",
            "parent class": "",
            "label": "",
            "datatype": "",
            "source": "",
            "data status": "",
            "requirement": "",
            "min value": "",
            "max value": "",
            "capitalize": "",
            "pattern": "",
            "description": "",
            "guidance": "",
            "examples": "",
        }

):
    print(current_package)

    main_row_list = []
    enum_row_list = []

    for i in slot_categories:
        current_row = blank_row.copy()
        current_row["label"] = i
        main_row_list.append(current_row)

    package_slots = slot_to_package_df["slots"].loc[
        slot_to_package_df["package"] == current_package
        ]
    package_slots = package_slots.iloc[0]
    package_slots = package_slots.split(";")

    for i in package_slots:
        current_details = slot_details_df.loc[slot_details_df["repaired_name"] == i]
        current_row = blank_row.copy()
        #     "Ontology ID"
        current_row["parent class"] = current_details["Category"].iloc[0]
        current_row["label"] = i
        current_row["datatype"] = "xs:token"
        # day resolution may not be specific enough
        if current_details["syntax"].iloc[0] == "{timestamp}":
            current_row["datatype"] = "xs:date"
        if current_details["syntax"].iloc[0] == "{float}":
            current_row["datatype"] = "xs:decimal"
        if current_details["syntax"].iloc[0] == "{value}":
            current_row["datatype"] = "xs:decimal"
        # {integer} doesn't actually = xs:nonNegativeInteger
        if current_details["syntax"].iloc[0] == "{integer}":
            current_row["datatype"] = "xs:nonNegativeInteger"
        if i == "unique_ID":
            current_row["datatype"] = "xs:unique"
        if current_details["syntax"].iloc[0] == "{float} {unit}":
            current_row["pattern"] = "^[+-]?([0-9]*[.])?[0-9]+ \S+$"
        #     "source": "",
        #     "data status": "",
        if current_details["Category"].iloc[0] in required_sections:
            current_row["requirement"] = "required"
        #     "min value": "",
        #     "max value": "",
        #     "capitalize": "",
        current_row["description"] = current_details["Column Header"].iloc[0]
        current_row["guidance"] = current_details["Guidance"].iloc[0]
        current_row["examples"] = current_details["syntax"].iloc[0]
        if i in ct_keys:
            current_row["datatype"] = "select"
            # map?
            # indent?
            current_enums = enums_dict[i]
            current_enums.sort()
            for j in current_enums:
                current_enum_row = blank_row.copy()
                current_enum_row["label"] = j
                current_enum_row["parent class"] = i
                enum_row_list.append(current_enum_row)
        main_row_list.append(current_row)
    print("\n")

    current_frame = pd.DataFrame(main_row_list)
    enum_frame = pd.DataFrame(enum_row_list)
    assembled_frame = pd.concat([current_frame, enum_frame])

    # # create directory if necessary
    # required_directory = template_prefix + current_package
    # # print(required_directory)
    # os.makedirs(required_directory, exist_ok=True)
    # # copy from soil directory to new required_directory?
    # ref_temp_src = dh_template_root + ref_temp_filename
    # ref_temp_dest = required_directory + "/" + ref_temp_filename
    # copyfile(ref_temp_src, ref_temp_dest)
    #
    # ref_temp_src = dh_template_root + "export.js"
    # ref_temp_dest = required_directory + "/" + "export.js"
    # copyfile(ref_temp_src, ref_temp_dest)
    #
    # # should escape characters that break filenames like whitespaces
    # current_template_file = required_directory + template_suffix
    # assembled_frame.to_csv(current_template_file, index=False, sep="\t")


def make_yaml():
    the_yaml = {"name": "IndexOfTerms", "id": "http://example.com/IoT",
                "prefixes": {"linkml": "https://w3id.org/linkml/"},
                "imports": ["linkml:types"],
                "default_range": "string"}
    return the_yaml
