## MQTT Messages and MDS Connector

The json payloads below are example messages published on `mfi-v1.0-kv\#` MQTT topic family when an adapter starts and stops. The `birth` message is sent when the adapter starts, and the `death` message is sent when the adapter stops. These messages contain metadata about the trial, the source of the data, the adapter configuration, and the MQTT broker configuration.

`connector.py` subscribes to the `mfi-v.1.0-kv\#` topic family and listens for these messages to update the Metadata Store accordingly. It stores 
* the metadata from the json payload, and 
* the topic on which the message was received,
in appropriate tables in the Metadata Store.


## Sample MQTT Payloads

`birth.json`:
```
{
    "trial_id": "AIDF_FINAL_DEMO",
    "time": {
        "birth": "2025-09-18T10:40:49.543282"
    },
    "source": {
        "os": "Linux",
        "hostname": "mfi-twin",
        "fqdn": "mfi-twin"
    },
    "adapter": {
        "name": "",
        "config": {
            "trial_id": "AIDF_FINAL_DEMO",
            "set_ros_callback": false,
            "devices": {
                "yk_architect": {
                    "namespace": "yk_architect",
                    "rostopics": [
                        "fts",
                        "joint_states",
                        "robot_status",
                        "tool_offset",
                        "pick_place_classify",
                        "perception_checking"
                    ],
                    "attributes": {
                        "description": "Architect - Yaskawa GP4 Robot System",
                        "type": "Robot",
                        "version": 0.01,
                        "trial_id": "AIDF_FINAL_DEMO"
                    }
                },                                                                                                                              
            },                                                                                                                                                                        
            "max_wait_per_topic": 1                                                                                                                                                   
        },                                                                                                                                                                            
        "component_ids": [
            "yk_architect",
        ],                                                                                                                                                                            
        "attributes": {                                                                                                                                                               
            "yk_architect": {                                                                                                                                                         
                "description": "Architect - Yaskawa GP4 Robot System",                                                                                                                
                "type": "Robot",                                                                                                                                                      
                "version": 0.01,                                                                                                                                                      
                "trial_id": "AIDF_FINAL_DEMO"                                                                                                                                         
            },                                                                                                                                                                     
        },                                                                                                                                                                            
        "sample_data": {                                                                                                                                                              
            "yk_architect": {                                                                                                                                                         
                "fts/header.seq": 1428647,                                                                                                                                            
                "fts/header.stamp.secs": 1758206442,                                                                                                                                  
                "fts/header.stamp.nsecs": 693962060,                                                                                                                                  
                "fts/header.frame_id": "",                                                                                                                                            
                "fts/wrench.force.x": 1.1350674033164978,                                                                                                                             
                "fts/wrench.force.y": -5.871739685535431,                                                                                                                             
                "fts/wrench.force.z": 1.450018286705017,                                                                                                                              
                "fts/wrench.torque.x": 0.11151281744241714,                                                                                                                           
                "fts/wrench.torque.y": 0.047472357749938965,                                                                                                                          
                "fts/wrench.torque.z": -0.009146779775619507,                                                                                                                         
                "joint_states/header.seq": 124272,                                                                                                                                    
                "joint_states/header.stamp.secs": 1758206442,                                                                                                                         
                "joint_states/header.stamp.nsecs": 722410806,                                                                                                                         
                "joint_states/header.frame_id": "",                                                                                                                                   
                "joint_states/name/name.0": "joint_1_s",                                                                                                                              
                "joint_states/name/name.1": "joint_2_l",                                                                                                                              
                "joint_states/name/name.2": "joint_3_u",                                                                                                                              
                "joint_states/name/name.3": "joint_4_r",                                                                                                                              
                "joint_states/name/name.4": "joint_5_b",                                                                                                                              
                "joint_states/name/name.5": "joint_6_t",                                                                                                                              
                "joint_states/position/position.0": 3.393421502551064e-05,                                                                                                            
                "joint_states/position/position.1": -7.575214112875983e-05,                                                                                                           
                "joint_states/position/position.2": 0.0,                                                                                                                              
                "joint_states/position/position.3": 6.203598604770377e-05,
                "joint_states/position/position.4": -1.5714504718780518,                                                                                                              
                "joint_states/position/position.5": -7.669904152862728e-05,                                                                                                           
                "joint_states/velocity/velocity.0": 0.0,                                                                                                                              
                "joint_states/velocity/velocity.1": 0.0,                                                                                                                              
                "joint_states/velocity/velocity.2": 0.0,                                                                                                                              
                "joint_states/velocity/velocity.3": 0.0,                                                                                                                              
                "joint_states/velocity/velocity.4": 0.0,                                                                                                                              
                "joint_states/velocity/velocity.5": 0.0,                                                                                                                              
                "robot_status/header.seq": 124273,                                                                                                                                    
                "robot_status/header.stamp.secs": 1758206442,                                                                                                                         
                "robot_status/header.stamp.nsecs": 758189096,                                                                                                                         
                "robot_status/header.frame_id": "",                                                                                                                                   
                "robot_status/mode.val": 2,                                                                                                                                           
                "robot_status/e_stopped.val": 0,                                                                                                                                      
                "robot_status/drives_powered.val": 0,                                                                                                                                 
                "robot_status/motion_possible.val": 1,                                                                                                                                
                "robot_status/in_motion.val": 0,                                                                                                                                      
                "robot_status/in_error.val": 0,                                                                                                                                       
                "robot_status/error_code": 0,                                                                                                                                         
                "pick_place_classify/data": false                                                                                                                                     
            },                                                                                                     
        }                                                                                                                                                                             
    },                                                                                                                                                                                
    "broker": {                                                                                                                                                                       
        "topic_family": "historian",                                                                                                                                                  
        "mqtt": {                                                                                                                                                                     
            "broker_address": "128.237.92.30",                                                                                                                                        
            "broker_port": 1883,                                                                                                                                                      
            "enterprise": "Mill-19-test",                                                                                                                                             
            "site": "Mezzanine-Lab",        
            "username": "admin",                                                                                                                                                      
            "tls_enabled": false,                                                                                                                                                     
            "debug": true                                                                                                                                                             
        }                                                                                                                                                                             
    }                                                                                                                                                                                 
}
```

`death.json`:
```json
{
    "trial_id": "AIDF_FINAL_DEMO",   
    "time": {
        "birth": "2025-09-18T10:40:49.543282",
        // not clean
        "death": "2025-09-19T15:01:28.123456" // clean exit
    },
    "source": {
        "os": "Linux",
        "hostname": "mfi-twin",
        "fqdn": "mfi-twin"
    },
    "adapter": {
        "config": {
            "trial_id": "AIDF_FINAL_DEMO",
            "set_ros_callback": false,
            "devices": {
                "yk_architect": {
                    "namespace": "yk_architect",
                    "rostopics": [
                        "fts",
                        "joint_states",
                        "robot_status",
                        "tool_offset",
                        "pick_place_classify",
                        "perception_checking"
                    ],
                    "attributes": {
                        "description": "Architect - Yaskawa GP4 Robot System",
                        "type": "Robot",
                        "version": 0.01,
                        "trial_id": "AIDF_FINAL_DEMO"
                    }
                }
            },
            "max_wait_per_topic": 1
        },
        "component_ids": [
            "yk_architect"
        ],
        "attributes": {
            "yk_architect": {
                "description": "Architect - Yaskawa GP4 Robot System",
                "type": "Robot",
                "version": 0.01,
                "trial_id": "AIDF_FINAL_DEMO"
            }
        },
        "sample_data": {
            "yk_architect": {
                "fts/header.seq": 1428647,
                "fts/header.stamp.secs": 1758206442,
                "fts/header.stamp.nsecs": 693962060,
                "fts/header.frame_id": "",
                "fts/wrench.force.x": 1.1350674033164978,
                "fts/wrench.force.y": -5.871739685535431,
                "fts/wrench.force.z": 1.450018286705017,
                "fts/wrench.torque.x": 0.11151281744241714,
                "fts/wrench.torque.y": 0.047472357749938965,
                "fts/wrench.torque.z": -0.009146779775619507,
                "joint_states/header.seq": 124272,
                "joint_states/header.stamp.secs": 1758206442,
                "joint_states/header.stamp.nsecs": 722410806,
                "joint_states/header.frame_id": "",
                "joint_states/name/name.0": "joint_1_s",
                "joint_states/name/name.1": "joint_2_l",
                "joint_states/name/name.2": "joint_3_u",
                "joint_states/name/name.3": "joint_4_r",
                "joint_states/name/name.4": "joint_5_b",
                "joint_states/name/name.5": "joint_6_t",
                "joint_states/position/position.0": 3.393421502551064e-05,
                "joint_states/position/position.1": -7.575214112875983e-05,
                "joint_states/position/position.2": 0.0,
                "joint_states/position/position.3": 6.203598604770377e-05,
                "joint_states/position/position.4": -1.5714504718780518,
                "joint_states/position/position.5": -7.669904152862728e-05,
                "joint_states/velocity/velocity.0": 0.0,
                "joint_states/velocity/velocity.1": 0.0,
                "joint_states/velocity/velocity.2": 0.0,
                "joint_states/velocity/velocity.3": 0.0,
                "joint_states/velocity/velocity.4": 0.0,
                "joint_states/velocity/velocity.5": 0.0,
                "robot_status/header.seq": 124273,
                "robot_status/header.stamp.secs": 1758206442,
                "robot_status/header.stamp.nsecs": 758189096,
                "robot_status/header.frame_id": "",
                "robot_status/mode.val": 2,
                "robot_status/e_stopped.val": 0,
                "robot_status/drives_powered.val": 0,
                "robot_status/motion_possible.val": 1,
                "robot_status/in_motion.val": 0,
                "robot_status/in_error.val": 0,
                "robot_status/error_code": 0,
                "pick_place_classify/data": false
            }
        }
    },
    "broker": {
        "topic_family": "historian",
        "mqtt": {
            "broker_address": "128.128.128.128",
            "broker_port": 1883,
            "enterprise": "Mill-19",
            "site": "Mezzanine-Lab",
            "username": "admin",
            "tls_enabled": false,
            "debug": true
        }
    }
}
```
