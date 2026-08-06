# Getting Started With MFI DDB

## Pre-requisites
1. Docker Engine
    - Download Docker using the instructions here ()

## Steps to Follow
1. Go to the `docker` folder.
2. In each folder, there are config yaml files for each of the services. The config yaml files are pre-populated with configs which you can directly use.
    - If you need to change the configs, you can traverse each folder and adjust the config files according to yourself. The config files have commented instructions above each of the parameters, which you can use to fill the configs accordingly.
4. Once the configs are filled, open a terminal window in `docker` folder.
5. Select the services you want to use. Make sure that all the pre-requisites are available on your machine. It is recommended to use the Data Adapter App in our stack, to establish connection between broker and the data generator.
6. If you don't have a data generator, you can use `mqttmock.py` file to replicate a data generator behaviour.
    - //add the pre-reqs for this too//
    - You can start publishing using the command `python mqttmock.py`.
7. Use the following command to start you required services

    - **For all services**
        ```
        docker compose --profile '*' up -d
        ```

    - **For specific services**
        ```
        docker compose --profile '<profile-name1>' --profile '<profile-name2>' ... up -d
        ```

8. Use command `docker ps` to check if all services are up and healthy. Make sure `mock-publisher` and `dev-tools` are up to test a dummy flow.
9. Now if you are using the Data Adapter App, you can create a connection with the data generator to start streaming.
10. Select the Data Adapter your Data Generator requires.
11. You will now have to fill in the Adapter and Streamer. You can click the (?) help icon to understand a parameter and expected input.
    - The configs won't be validated if incorrect config is put in. Make sure that correct config format is put in.
    - add in the test.mosqiutto.org as url and port is 1883 etc etc

        **Adapter Config**
        ```
        mqtt:
        broker_address: test.mosquitto.org
        broker_port: 1883
        trial_id: test_trial_001
        queue_size: 10
        topics:
        - component_id: telemetry
        topic: admin/feeds/avroom/telemetry/#
        ```
        **Streamer Config**
        ```
        user:
        user_id: test_user
        domain: default
        mqtt:
        broker_address: localhost
        broker_port: 1883
        enterprise: test_ent
        site: Test-Siteuser:
        user_id: test_user
        domain: default
        mqtt:
        broker_address: localhost
        broker_port: 1883
        enterprise: test_ent
        site: Test-Site
        ```
12. Click `Save` and the connection will initialize. The connection will show up on your screen as connected and streaming.

## Retrieval 
Now that you have a connection established we can look at the aspect of how to retrieve the data and visualize it using Grafana Infinity.
Open up grafana on port: 3005

You can see a dashboard named Retrieval Web Service. You can use the visualizations inside this dashboard to see how your data is evolving over a given period of time.
You can check data corresponding to the give user ID