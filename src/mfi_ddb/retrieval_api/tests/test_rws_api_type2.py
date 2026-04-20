import json
import time

from app.schema import schema
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_type0_endpoint():
    response = client.get("/mfi-ddb/type0")
    assert response.status_code == 200
    
    json_response = response.json()
    assert schema.Type0Response.model_validate_json(json_response)  # Validate response against schema
    
    assert len(json.loads(response.content)) == 5

def test_type1_endpoint():
    ...
    
def test_type2_endpoint():
    ...
    
def test_type3_endpoint():
    ...
    
def test_type4_endpoint():
    ...
    
        
if __name__ == "__main__":
    print("\nTEST 1: Testing Type 0 Endpoint")
    print("================================================")
    test_type0_endpoint()
    
    print("\nTEST 2: Testing Type 1 Endpoint")
    print("================================================")
    test_type1_endpoint()   
    
    print("\nTEST 3: Testing Type 2 Endpoint")
    print("================================================")
    test_type2_endpoint()
    
    print("\nTEST 4: Testing Type 3 Endpoint")
    print("================================================")
    test_type3_endpoint()
    
    print("\nTEST 5: Testing Type 4 Endpoint")
    print("================================================")
    test_type4_endpoint()