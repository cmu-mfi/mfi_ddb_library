from typing import List, Literal
from pydantic import BaseModel, Field, ConfigDict

from mfi_ddb.data_adapters.base import BaseDataAdapter
from mfi_ddb.topic_families.key_value import KeyValueTopicFamily
from mfi_ddb.utils.exceptions import ConfigError


class _SCHEMA(BaseModel):
    class _Payload(BaseModel):
        model_config = ConfigDict(extra='allow')
        schema_version: str = Field("mfi-v1.0", description="Version of kv schema to validate the payload.")
        msg_type: Literal['data', 'project', 'tp-tag', 'user'] = Field("data", description="OneOf('data', 'project', 'tp-tag', 'user')")
        
    class SCHEMA(BaseModel):
        trial_id: str = Field("mfi-ddb-metadata", description="(optional) It is required if one of the payload is of type 'data'")
        payloads: List['_Payload'] = Field(..., description="List of payload messages. Payloads of type 'data', 'project', 'tp-tag', and 'user' allowed. Each will be verified against the kv.json schema.")
                
class KvDataAdapter(BaseDataAdapter):
    
    NAME = "KeyValue"
    
    CONFIG_HELP = {
        'trial_id': "(optional) It is required if one of the payload is of type 'data'",
        "payloads": "List of payload messages. Payloads of type 'data', 'project', 'tp-tag', and 'user' allowed. Each will be verified against the kv.json schema."
    }
    
    CONFIG_EXAMPLE = {
        'trial_id': 'booster-a1',
        'payloads':[
            {
            "schema_version": "mfi-v1.0",
            "msg_type": "project",
            "project_id": "p-550",
            "project_name": "Apollo Mission",
            "user_roles":[
                {
                "user_id": "john_smith",
                "domain": "Acme.Corp",
                "role": "admin"
                }
            ],
            "created_by_user_id": "user_01",
            "created_by_domain": "internal",
            "timestamp": "2026-03-31T23:34:00Z"
            },
            {
            "schema_version": "mfi-v1.0",
            "msg_type": "user",
            "user_id": "new-user-002",
            "domain": "external",
            "created_by_user_id": "admin_01",
            "created_by_domain": "admin_domain",
            "email": "user@example.com",
            "name": "Jane Doe",
            "timestamp": "2026-03-31T23:34:00Z"
            },
            {
            "schema_version": "mfi-v1.0",
            "msg_type": "tp-tag",
            "trial_id": "trial-abc-123",
            "project_id": "p-550",
            "time_start": "2026-03-31T23:00:00Z",
            "time_end": "2026-03-31T23:30:00Z",
            "trial_user_id": "subject-01",
            "trial_user_domain": "lab-1",
            "created_by_user_id": "researcher_01",
            "created_by_domain": "university_net",
            "timestamp": "2026-03-31T23:34:00Z"
            },                        
            {
            "schema_version": "mfi-v1.0",
            "msg_type": "data",
            "payload": "Custom data here",
            "value": 42
            }                       
        ]
    }
    
    RECOMMENDED_TOPIC_FAMILY = "kv"
    
    SELF_UPDATE = False
    
    class SCHEMA(BaseDataAdapter.SCHEMA, _SCHEMA.SCHEMA):
        pass

    def __init__(self, config: dict):
        super().__init__()
        
        self.cfg = config
        try:
            self.cfg = KvDataAdapter.SCHEMA(**config).model_dump()
        except Exception as e:
            raise ConfigError(f"Invalid configuration: {e}")   
        
        self.pending_payloads = {}
        schema_validator = KeyValueTopicFamily.get_schema_validator()
        for payload in self.cfg['payloads']:
            payload = KeyValueTopicFamily.apply_defaults(payload, schema_validator)
            if not schema_validator.is_valid(payload):
                self.logger.warning(f"Payload does not match schema: {schema_validator.iter_errors(payload)}")
                self.logger.warning("Skippidng the payload...")
                continue
            
            if payload['msg_type']=='data' and self.cfg['trial_id']=="mfi-ddb-metadata":
                self.logger.warning("WARNING: No trial_id provided. Payload of type 'data' is ignored.")
                continue
            
            if payload['msg_type'] in self.component_ids:
                self.pending_payloads[payload['msg_type']].append(payload)
            else:
                self.component_ids.append(payload['msg_type'])
                self.pending_payloads[payload['msg_type']] = [payload]
                self.attributes[payload['msg_type']] = {'type': payload['msg_type']}
                self._data[payload['msg_type']] = {}                               
        
    def get_data(self):
        for key in self._data.keys():
            self.logger.info(f"sending {key} payload")
            if len(self.pending_payloads[key])>0:
                self._data[key] = self.pending_payloads[key].pop(0)
            else:
                self._data[key] = {}