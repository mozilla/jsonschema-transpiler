import click


@click.command()
@click.option(
    "--source",
    type=click.Path(exists=True),
    required=True,
    help="The source JSON Schema file",
)
@click.option(
    "--format",
    type=click.Choice(["bigquery"]),
    required=True,
    help="The transpile target format",
)
@click.option("--output", type=click.Path(), help="Output path")
def transpile(source, format, output):
    pass


if __name__ == "__main__":
    transpile()
