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
                data_adapters[adapter] = adapter_class.NAME
        except AttributeError:
            continue
    supported_adapters = ", ".join(f"'{adapter}'" for adapter in data_adapters.values())
    
    # INPUT ARGUMENTS
    # =======================================================
    
    example_usage = """
    Example usage:
    
    Use a configuration directory:
    $ python -m mfi_ddb.scripts.stream_adapter --data_adapter 'MQTT' --config_dir ./configs
    
    Use specific configuration files:
    $ python -m mfi_ddb.scripts.stream_adapter -d 'Local Files' --adapter_cfg ./configs/localfiles.yaml --mqtt_cfg ./configs/mqtt.yaml
    
    Enable polling mode with a specific rate (in Hz):
    $ python -m mfi_ddb.scripts.stream_adapter -d 'MTConnect' -a ./configs/mtconnect.yaml -m ./configs/mqtt.yaml -p True -r 2
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
        help="(optional) Directory containing the configuration files (localfiles.yaml and mqtt.yaml).\
            If --mqtt_cfg or --adapter_cfg are provided, this argument is ignored.",
    )
    parser.add_argument(
        "--adapter_cfg",
        "-a",
        type=str,
        help="(optional) Path to the local files adapter configuration file (localfiles.yaml).",
    )
    parser.add_argument(
        "--mqtt_cfg",
        "-m",
        type=str,
        help="(optional) Path to the MQTT configuration file (mqtt.yaml).",
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
    
    if args.mqtt_cfg and args.adapter_cfg:
        mqtt_config_file = args.mqtt_cfg
        adapter_config_file = args.adapter_cfg
    elif args.config_dir:
        mqtt_config_file = os.path.join(args.config_dir, "mqtt.yaml")
        adapter_config_file = os.path.join(args.config_dir, "localfiles.yaml")
    else:
        exception_msg = (
            "Either --config_dir or both --mqtt_cfg and --adapter_cfg must be provided."
        )
        raise Exception(exception_msg)  
    
    # LOAD CONFIG FILES
    # =======================================================

    with open(adapter_config_file, "r") as file:
        adapter_config = yaml.load(file, Loader=yaml.FullLoader)

    with open(mqtt_config_file, "r") as file:
        mqtt_config = yaml.load(file, Loader=yaml.FullLoader)

    try:
        adapter_class = getattr(mfi_ddb.data_adapters, args.data_adapter)
    except AttributeError:
        exception_msg = (
            f"Data adapter '{args.data_adapter}' not found. Supported adapters: {supported_adapters}"
        )
        raise Exception(exception_msg)
    
    # INITIALIZE ADAPTER AND STREAMER
    # =======================================================
    adapter = adapter_class(adapter_config)
    
    streamer = mfi_ddb.Streamer(mqtt_config, adapter, stream_on_update=not args.polling)
    
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