
## Compiling proto files for Python

### Installing Dependencies
Through apt:

```sh
sudo apt install python3-grpc-tools python3-grpcio
```

Through pip:

> [!Note]
> Use this method with virtual environment, if you don't have sudo access.

```sh
pip install grpcio grpcio-tools 
```


### Compiling proto files
Before we can read or write anything, the protobuf definitions have to be compiled to be used by python.

To do this, run the script in this folder **build_all.sh** (ignore the warning messages)

```sh
bash ./build_all.sh
```