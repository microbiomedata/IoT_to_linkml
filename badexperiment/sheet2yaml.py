import os
import pandas as pd

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
client_secret_flie = "../google_api_credentials.json"


def get_creds():
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
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_flie, SCOPES)
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

def get_iot_glassary_frame():
    creds = get_creds()
    sheet_service = get_sheet_service(creds)
    iot_glossary_tab = get_gsheet_tab(sheet_service, IOT_SPREADSHEET_ID, IOT_RANGE_NAME)
    iot_glossary_frame = pd.DataFrame(iot_glossary_tab["values"], columns=iot_glossary_tab["values"][0]).drop(0)
    return iot_glossary_frame


def get_iot_controlled_terms_frame():
    creds = get_creds()
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
def get_slot_to_pack(iot_glassary_frame):
    slot_to_pack = iot_glassary_frame[["name", "mixs_6_slot_name", "Associated Packages"]]
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

# my_iot_glassary_frame = get_iot_glassary_frame()
# my_slot_to_pack = get_slot_to_pack(my_iot_glassary_frame)
# # print(slot_to_pack)
# my_iot_packages = get_iot_packages(my_slot_to_pack)
# # print(iot_packages)
# coalesced_package_names = coalesce_package_names(my_slot_to_pack)
# # print(coalesced_package_names)
# isolated_slot_to_package = get_pack_to_slot(coalesced_package_names, my_iot_packages)
# print(isolated_slot_to_package)
