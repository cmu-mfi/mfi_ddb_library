import argparse
import os
import time

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
                data_adapters[adapter_class.NAME] = adapter_class
        except AttributeError:
            continue
    # supported_adapters = ", ".join(f"'{adapter}'" for adapter in data_adapters.values())
    supported_adapters = ", ".join(f"'{adapter}'" for adapter in data_adapters.keys())
    
    # INPUT ARGUMENTS
    # =======================================================
    
    example_usage = """
    Example usage:
    
    Use a configuration directory:
    $ python -m mfi_ddb.scripts.stream_adapter --data_adapter 'MQTT' --config_dir ./configs
    
    Use specific configuration files:
    $ python -m mfi_ddb.scripts.stream_adapter -d 'Local Files' --adapter_cfg ./configs/local_files.yaml --streamer_cfg ./configs/streamer.yaml

    Enable polling mode with a specific rate (in Hz):
    $ python -m mfi_ddb.scripts.stream_adapter -d 'MTConnect' -a ./configs/mtconnect.yaml -s ./configs/streamer.yaml -p True -r 2
    """
    parser = argparse.ArgumentParser(
        description="Stream data using MFI-DDB library.",
        epilog=example_usage,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    
    parser.add_argument(
        "--data_adapter",
        "-d",
        type=str,
        required=True,
        help=f"Type of data adapter to use. Supported: {supported_adapters}",
    )
    parser.add_argument(
        "--config_dir",
        "-cd",
        type=str,
        help="Directory containing the configuration files (local_files.yaml and mqtt.yaml).\
            If --streamer_cfg or --adapter_cfg are provided, this argument is ignored.",
    )
    parser.add_argument(
        "--adapter_cfg",
        "-a",
        type=str,
        help="Path to the local files adapter configuration file (local_files.yaml).",
    )
    parser.add_argument(
        "--streamer_cfg",
        "-s",
        type=str,
        help="Path to the Streamer configuration file (streamer.yaml).",
    )
    parser.add_argument(
        "--polling",
        "-p",
        type=bool,
        default=False,
        help="Enable polling mode. Default is False.",
    )
    parser.add_argument(
        "--poll_rate",
        "-r",
        type=int,
        default=1,
        help="Polling rate in Hz. Default is 1 Hz, if --polling is set to True.",
    )    
    args = parser.parse_args()
    
    if args.streamer_cfg and args.adapter_cfg:
        streamer_config_file = args.streamer_cfg
        adapter_config_file = args.adapter_cfg
    elif args.config_dir:
        streamer_config_file = os.path.join(args.config_dir, "streamer.yaml")
        adapter_config_name = args.data_adapter.lower().replace(" ", "_") + ".yaml"
        adapter_config_file = os.path.join(args.config_dir, adapter_config_name)
    else:
        exception_msg = (
            "Either --config_dir or both --streamer_cfg and --adapter_cfg must be provided."
        )
        raise Exception(exception_msg)  
    
    # LOAD CONFIG FILES
    # =======================================================

    try:
        adapter_class = data_adapters[args.data_adapter]
    except KeyError:
        exception_msg = (
            f"Data adapter '{args.data_adapter}' not found. Supported adapters: {supported_adapters}"
        )
        raise Exception(exception_msg)

    try:
        with open(adapter_config_file, "r") as file:
            adapter_config = yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        exception_msg = f"ADAPTER CONFIG NOT FOUND. ENSURE THE FILE EXISTS: {os.path.abspath(adapter_config_file)}"
        raise Exception(exception_msg)

    try:
        with open(streamer_config_file, "r") as file:
            streamer_config = yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        exception_msg = f"STREAMER CONFIG NOT FOUND. ENSURE THE FILE EXISTS: {os.path.abspath(streamer_config_file)}"
        raise Exception(exception_msg)

    
    # INITIALIZE ADAPTER AND STREAMER
    # =======================================================
    adapter = adapter_class(adapter_config)
    
    streamer = mfi_ddb.Streamer(streamer_config, adapter, stream_on_update=not args.polling)
    
    if args.polling:
        print("\nPolling method selected.")
        while True:
            streamer.poll_and_stream_data(polling_rate_hz=args.poll_rate)
    else:
        print("\nCallback method selected.")
        while True:
            # Wait for data update
            try:
                time.sleep(0.1)
            except KeyboardInterrupt:
                print("Exiting...")
                break