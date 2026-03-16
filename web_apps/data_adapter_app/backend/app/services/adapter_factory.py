import logging
import threading
from typing import Any, Dict, Optional

from pydantic import ValidationError

import mfi_ddb


class AdapterFactory:
    @classmethod
    def discover_adapters(cls) -> Dict[str, Any]:
        data_adapters = {}
        for adapter in dir(mfi_ddb.data_adapters):
            try:
                adapter_class = getattr(mfi_ddb.data_adapters, adapter)
                if adapter_class.NAME:
                    data_adapters[adapter_class.NAME] = adapter_class
            except AttributeError:
                continue
        #TODO: Change BaseDataAdapter class to have default values
        data_adapters["Base"] = mfi_ddb.data_adapters.BaseDataAdapter
        return data_adapters

    def __init__(
        self,
        adp_name: str = "Base",
        adp_cfg: dict = {},
        streamer_cfg: dict = {},
        is_polling: bool = True,
        polling_rate_hz: int = 1,
    ) -> None:
        data_adapters = AdapterFactory.discover_adapters()

        if adp_name not in data_adapters.keys():
            raise ValueError(
                f"Adapter '{adp_name}' not supported. Available adapters: {list(data_adapters.keys())}"
            )

        self.adp_name = adp_name
        self.adp_class = data_adapters[adp_name]
        self.adp_cfg = adp_cfg
        self.streamer_class = mfi_ddb.Streamer
        self.streamer_cfg = streamer_cfg
        self.is_polling = is_polling
        self.polling_rate_hz = polling_rate_hz
        self.logger = logging.getLogger(__name__)

        # STATE DATA MEMBERS
        self.is_connected = False
        self.is_streaming = False
        self.poll_streaming = threading.Event()

        if not self.adp_class.SELF_UPDATE and not self.is_polling:
            self.logger.warning(
                "Adapter '%s' does not support self-update. Forcing polling mode at %d Hz.",
                self.adp_name,
                self.polling_rate_hz,
            )
            self.is_polling = True

    def validate_data_adapter_config(self) -> bool:
        try:
            self.adp_class.SCHEMA.model_validate(self.adp_cfg)
            return True
        except ValidationError as ve:
            self.logger.warning(
                f"Invalid configuration for adapter '{self.adp_name}': {ve}"
            )
            return False
        except Exception as e:
            self.logger.error(
                f"Error validating configuration for adapter '{self.adp_name}': {e}"
            )
            return False

    def validate_streamer_config(self) -> bool:
        try:
            self.streamer_class.SCHEMA.model_validate(self.streamer_cfg)
            return True
        except ValidationError as ve:
            self.logger.warning(f"Invalid streamer configuration: {ve}")
            return False
        except Exception as e:
            self.logger.error(f"Error validating streamer configuration: {e}")
            return False

    def validate_configs(self) -> bool:
        return (self.validate_data_adapter_config() and self.validate_streamer_config())

    def connect_and_stream(self) -> None:
        if not self.validate_configs():
            self.logger.error("Configuration validation failed. Aborting connection.")
            print("ABORT ABORT ABORT")
            return

        self.data_adapter = self.adp_class(self.adp_cfg)
        
        self.streamer = self.streamer_class(
            self.streamer_cfg, self.data_adapter, stream_on_update=not self.is_polling
        )
        
        self.is_connected = True
        self.poll_streaming.set()
        
        if self.is_polling:
            def polling_loop():
                while self.poll_streaming.is_set():
                    self.streamer.poll_and_stream_data(self.polling_rate_hz)
            poll_thread = threading.Thread(target=polling_loop, daemon=True)
            poll_thread.start()
        
        self.is_streaming = True

    def resume_streaming(self) -> None:
        if self.is_streaming:
            self.logger.info("Streaming is already active.")
            return
        
        if not self.is_connected:
            self.connect_and_stream()
            return
        
        if self.is_polling:
            self.poll_streaming.set()
            self.is_streaming = True
            return
        
        self.streamer.reconnect()
        self.is_streaming = True
        
    def pause_streaming(self) -> None:
        if self.is_polling:
            self.poll_streaming.clear()
            self.is_streaming = False
            return
        
        raise Exception("Pause streaming is only supported in polling mode.")
    
    def disconnect(self) -> None:
        if not self.is_connected:
            self.logger.info("No active connection to disconnect.")
            return
        
        if self.is_polling:
            self.poll_streaming.clear()
        
        self.streamer.disconnect()
        self.is_connected = False
        self.is_streaming = False