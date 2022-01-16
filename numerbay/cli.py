""" Access the NumerBay API via command line"""

import json
import datetime
import decimal

import click

import numerbay

nbay = numerbay.NumerBay()


class CommonJSONEncoder(json.JSONEncoder):
    """
    Common JSON Encoder
    json.dumps(jsonString, cls=CommonJSONEncoder)
    """

    def default(self, o):
        # Encode: Decimal
        if isinstance(o, decimal.Decimal):
            return str(o)
        # Encode: Date & Datetime
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()

        return None


def prettify(stuff):
    """prettify json"""
    return json.dumps(stuff, cls=CommonJSONEncoder, indent=4)


@click.group()
def cli():
    """Wrapper around the NumerBay API"""


@cli.command()
def account():
    """Get all information about your account!"""
    click.echo(prettify(nbay.get_account()))


@cli.command()
def orders():
    """Get all your orders!"""
    click.echo(prettify(nbay.get_my_orders()))


@cli.command()
def sales():
    """Get all your sales!"""
    click.echo(prettify(nbay.get_my_sales()))


@cli.command()
def listings():
    """Get all your listings!"""
    click.echo(prettify(nbay.get_my_listings()))


@cli.command()
@click.option("--product_id", type=int, default=None, help="NumerBay product ID")
@click.option(
    "--product_full_name",
    type=str,
    default=None,
    help="NumerBay product full name (e.g. numerai-predictions-numerbay), \
    used for resolving product_id if product_id is not provided",
)
@click.argument("path", type=click.Path(exists=True))
def submit(path, product_id, product_full_name):
    """Upload artifact from file."""
    click.echo(
        nbay.upload_artifact(
            path, product_id=product_id, product_full_name=product_full_name
        )
    )


@cli.command()
@click.option("--product_id", type=int, default=None, help="NumerBay product ID")
@click.option(
    "--product_full_name",
    type=str,
    default=None,
    help="NumerBay product full name (e.g. numerai-predictions-numerbay), \
    used for resolving product_id if product_id is not provided",
)
@click.option(
    "--artifact_id",
    type=int,
    default=None,
    help="Artifact ID for the file to download, \
    defaults to the first artifact for your active order for the product",
)
@click.option("--filename", help="filename to store as")
@click.option(
    "--dest_path",
    help="complate path where the file should be stored, \
    defaults to the same name as the source file",
)
def download(filename, dest_path, product_id, product_full_name, artifact_id):
    """Download artifact file."""
    click.echo(
        nbay.download_artifact(
            filename=filename,
            dest_path=dest_path,
            product_id=product_id,
            product_full_name=product_full_name,
            artifact_id=artifact_id,
        )
    )


@cli.command()
def version():
    """Installed numerbay version."""
    print(numerbay.__version__)


if __name__ == "__main__":
    cli()
