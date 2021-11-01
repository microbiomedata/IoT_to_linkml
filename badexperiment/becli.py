import click
import badexperiment.sheet2yaml as s2y


@click.command()
@click.option('--count', default=1, help='Number of greetings.')
@click.option('--name', prompt='Your name',
              help='The person to greet.')
def hello(count, name):
    """Simple program that greets NAME for a total of COUNT times."""
    for x in range(count):
        click.echo(f"Hello {name}!")

    my_iot_glossary_frame = s2y.get_iot_glossary_frame(client_secret_file="google_api_credentials.json")
    # print(my_iot_glossary_frame)

    my_slot_to_pack = s2y.get_slot_to_pack(my_iot_glossary_frame)
    # print(slot_to_pack)

    my_iot_packages = s2y.get_iot_packages(my_slot_to_pack)
    # print(iot_packages)

    coalesced_package_names = s2y.coalesce_package_names(my_slot_to_pack)
    # print(coalesced_package_names)

    isolated_slot_to_package = s2y.get_pack_to_slot(coalesced_package_names, my_iot_packages)
    print(isolated_slot_to_package)


if __name__ == '__main__':
    hello()
