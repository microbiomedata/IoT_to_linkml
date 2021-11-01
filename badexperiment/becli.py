import click
import badexperiment.sheet2yaml as s2y


@click.command()
# @click.option('--count', default=1, help='Number of greetings.')
# @click.option('--name', prompt='Your name',
#               help='The person to greet.')
@click.option('--cred', default='google_api_credentials.json', help="path to google_api_credentials.json",
              type=click.Path(exists=True))
def hello(cred):
    """Command line wrapper for processing the Index of Terms."""
    print(f"Getting credentials from {cred}")

    my_iot_glossary_frame = s2y.get_iot_glossary_frame(client_secret_file="google_api_credentials.json")

    my_slot_to_pack = s2y.get_slot_to_pack(my_iot_glossary_frame)

    my_iot_packages = s2y.get_iot_packages(my_slot_to_pack)

    coalesced_package_names = s2y.coalesce_package_names(my_slot_to_pack)

    isolated_slot_to_package = s2y.get_pack_to_slot(coalesced_package_names, my_iot_packages)
    print(isolated_slot_to_package)


if __name__ == '__main__':
    hello()
