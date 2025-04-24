import json
import os
import sys
import base64
import test_pb2


def test_sample_files(filepath):
    data_obj = {}
    data_obj['file'] = None
    data_obj['name'] = filepath.split('/')[-1]
        
    file_data = None
    with open(filepath, 'rb') as f:
        file_data = f.read()
    
    data_obj['file'] = base64.b64encode(file_data).decode('utf-8')
    
    data_obj = str(data_obj)
    
    json_obj = json.dumps(data_obj)
    
    proto_obj = test_pb2.FileData()
    # proto_obj.name = 'requirements.txt'
    proto_obj.file = file_data
    proto_obj = proto_obj.SerializeToString()
    
    print("filename: ", filepath.split('/')[-1])    
    print("size of data_obj: ", sys.getsizeof(data_obj))
    print("size of json_obj: ", sys.getsizeof(json_obj))
    print("size of proto_obj: ", sys.getsizeof(proto_obj))    
    print("=====================================\n")


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.realpath(__file__))
    
    test_file_path = os.path.join(current_dir, 'test_files')
    
    files = os.listdir(test_file_path)
    
    for file in files:
        sample_file = os.path.join(test_file_path, file)
        test_sample_files(sample_file)