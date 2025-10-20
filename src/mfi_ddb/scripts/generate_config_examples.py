import argparse
import os

import yaml

import mfi_ddb

if __name__ == "__main__":
    # LOAD AVAILABLE DATA ADAPTERS
    # =======================================================
    data_adapters = {}
    for adapter in dir(mfi_ddb.data_adapters):
        try:
            adapter_class = getattr(mfi_ddb.data_adapters, adapter)
            if adapter_class.NAME:
                data_adapters[adapter] = adapter_class
        except AttributeError:
            continue
    
    # INPUT ARGUMENTS
    # =======================================================
    
    example_usage = """
    Example usage:
    $ python -m mfi_ddb.scripts.generate_config_examples --output_dir ./examples/configs
    """
    
    parser = argparse.ArgumentParser(
        description="Generate configuration example files for all available data adapters.",
        epilog=example_usage,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--output_dir",
        "-o",
        type=str,
        default="examples/configs",
        help="Directory to save the generated configuration example files.",
    )

    args = parser.parse_args()

    # Convert output directory to absolute path, and create it if it doesn't exist
    args.output_dir = os.path.abspath(args.output_dir)
    os.makedirs(args.output_dir, exist_ok=True)

    # GENERATE CONFIGURATION EXAMPLE FILES FOR DATA ADAPTERS
    # =======================================================

    data_adapter_config_dir = os.path.join(args.output_dir, "data_adapters")
    os.makedirs(data_adapter_config_dir, exist_ok=True)

    for adapter in data_adapters.values():
        config_example = adapter.CONFIG_EXAMPLE
        config_help = adapter.CONFIG_HELP
    
        config_example_pretty = yaml.dump(config_example, sort_keys=False)
        config_help_pretty = yaml.dump(config_help, sort_keys=False)        
        
        example_output_path = f"{data_adapter_config_dir}/{adapter.NAME.lower().replace(' ', '_')}_config.yaml"
        help_output_path = f"{data_adapter_config_dir}/{adapter.NAME.lower().replace(' ', '_')}_help.yaml"
        yaml.dump(config_example, open(example_output_path, "w"), sort_keys=False)
        yaml.dump(config_help, open(help_output_path, "w"), sort_keys=False)
        
    # GENERATE OVERALL CONFIGURATION EXAMPLE FILE FOR STREAMER
    # =======================================================
    streamer_config_dir = os.path.join(args.output_dir, "streamer")
    os.makedirs(streamer_config_dir, exist_ok=True)
    
    config_example = mfi_ddb.streamer.Streamer.CONFIG_EXAMPLE
    config_help = mfi_ddb.streamer.Streamer.CONFIG_HELP

    config_example_pretty = yaml.dump(config_example, sort_keys=False)
    config_help_pretty = yaml.dump(config_help, sort_keys=False)

    example_output_path = f"{streamer_config_dir}/streamer_config.yaml"
    help_output_path = f"{streamer_config_dir}/streamer_help.yaml"
    yaml.dump(config_example, open(example_output_path, "w"), sort_keys=False)
    yaml.dump(config_help, open(help_output_path, "w"), sort_keys=False)
