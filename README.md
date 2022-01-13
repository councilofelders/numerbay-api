
# NumerBay Python API
Programmatic interaction with [numerbay.ai](https://numerbay.ai) - the Numerai community marketplace.

If you encounter a problem or have suggestions, feel free to open an issue or post in [#numerbay](https://community.numer.ai/channel/numerbay) in RocketChat.

Credit: code structure adapted from the `numerapi` [repo](https://github.com/uuazed/numerapi)

# Installation
`pip install --upgrade numerbay`

# Usage

## Python module

Checkout the NumerBay [Python Client Tutorials](https://docs.numerbay.ai/docs/tutorial-extras/ensemble) for usage examples.


## Command line interface

To get started with the cli interface, let's take a look at the help page:

    $ numerbay --help
    Usage: numerbay [OPTIONS] COMMAND [ARGS]...

      Wrapper around the NumerBay API

      Options:
        --help  Show this message and exit.

      Commands:
        Commands:
          account   Get all information about your account!
          download  Download artifact file.
          listings  Get all your listings!
          orders    Get all your orders!
          sales     Get all your sales!
          submit    Upload artifact from file.
          version   Installed numerbay version.



Each command has it's own help page, for example:

    $ numerbay submit --help
    Usage: numerbay submit [OPTIONS] PATH

      Upload artifact from file.

    Options:
      --product_id INTEGER      NumerBay product ID
      --product_full_name TEXT  NumerBay product full name (e.g. numerai-
                                predictions-numerbay), used for resolving
                                product_id if product_id is not provided
      --help                    Show this message and exit.



# API Reference

Checkout the [detailed API](https://docs.numerbay.ai/docs/reference/numerbay) docs to learn about all available methods, parameters and returned values.
