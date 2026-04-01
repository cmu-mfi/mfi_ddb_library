import argparse
import os
import time

import yaml

import mfi_ddb

class MetadataAdapter(mfi_ddb.BaseDataAdapter):
    NAME = "Metadata Adapter"
    SELF_UPDATE = True
    
    def __init__(self, metadata_type: str) -> None:
        super().__init__({})
        self.component_ids = ["metadata"]
        self.metadata_type = metadata_type
        
        self.attributes["metadata"] = {
            "type": "metadata_type"
        }
        
    def update_metadata(self, data: dict):
        data['msg_type'] = self.metadata_type
        try:
            self.cb_data["metadata"] = data
        except ValueError as e:
            print(f"Error processing metadata: {e}")
            raise e
    
 
if __name__ == "__main__":
    
    # INPUT ARGUMENTS
    # =======================================================
    
    example_usage = """
    Example usage:
    
    $ python -m mfi_ddb.scripts.metadata_stream_example --streamer_cfg ./configs/streamer.yaml --metadata_type project
    """
    parser = argparse.ArgumentParser(
        description="Stream metadata.",
        epilog=example_usage,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    
    parser.add_argument(
        "--streamer_cfg",
        "-s",
        type=str,
        required=True,
        help="Path to the Streamer configuration file (streamer.yaml).",
    )
    parser.add_argument(
        "--metadata_type",
        "-m",
        type=str,
        required=True,
        help="Type of metadata: user, project, tp-tag.",
    )
    args = parser.parse_args()
    
    streamer_config_file = args.streamer_cfg
    metadata_type = args.metadata_type
    
    if metadata_type not in ["user", "project", "tp-tag"]:
        raise ValueError("Invalid metadata type. Supported types: 'user', 'project', 'tp-tag'.")
    
    # LOAD CONFIG FILES
    # =======================================================
    
    try:
        with open(streamer_config_file, "r") as file:
            streamer_config = yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        exception_msg = f"STREAMER CONFIG NOT FOUND. ENSURE THE FILE EXISTS: {os.path.abspath(streamer_config_file)}"
        raise Exception(exception_msg)

    
    # INITIALIZE ADAPTER AND STREAMER
    # =======================================================
    adapter = MetadataAdapter(metadata_type)
    
    streamer = mfi_ddb.Streamer(streamer_config, adapter, stream_on_update=True)
 
    # TODO: enter value for `data` manually or through some interface
    data = ...
    
    adapter.update_metadata(data)