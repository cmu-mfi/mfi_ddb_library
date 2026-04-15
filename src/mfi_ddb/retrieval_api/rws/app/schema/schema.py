from pydantic import BaseModel, EmailStr, Field
from typing import List, Optional, Union
from enum import Enum

class _ResponseTypeEnum(str, Enum):
    json = "json"
    csv = "csv"
    
class _Response(BaseModel):
    """
    Base response model for API endpoints.
    """
    status: str = Field(..., description="The status of the response (e.g., 'success', 'error').")
    message: Optional[str] = Field(None, description="An optional message providing additional information about the response.")

class Type0Request(BaseModel):
    """
    Request model for Type 0 endpoint.
    """
    pass

class Type0Response(_Response):
    """
    Response model for Type 0 endpoint.
    """
    class EndpointInfo(BaseModel):
        """
        Model representing information about an API endpoint.
        """
        endpoint: str = Field(..., description="The API endpoint path.")
        description: Optional[str] = Field(
            None, description="A brief description of what the endpoint does."
        )
    endpoints: Optional[list[EndpointInfo]] = Field(None, description="A list of available API endpoints with their descriptions.")

class Type1Request(BaseModel):
    """
    Request model for Type 1 endpoint.
    """
    enterprise_id: str = Field(..., description="The enterprise ID to search for.")
    time_start: str = Field(..., description="The start time for the search in ISO 8601 format.")
    time_end: str = Field(..., description="The end time for the search in ISO 8601 format.")
    user_id: str = Field(..., description="The user ID requesting the search.")
    user_domain: Optional[str] = Field(None, description="The domain of the user requesting the search.")
    data_format: _ResponseTypeEnum = Field(_ResponseTypeEnum.json, description="The desired data format for the response (e.g., 'json', 'csv').")
    site: Optional[str] = Field(None, description="The site to filter the search results by.")
    device: Optional[str] = Field(None, description="The device to filter the search results by.")
    trial_id: Optional[str] = Field(None, description="The trial ID to filter the search results by.")
    project_id: Optional[str] = Field(None, description="The project ID to filter the search results by.")
    project_name: Optional[str] = Field(None, description="The project name to filter the search results by.")
    search_terms: Optional[list[str]] = Field(None, description="A list of search terms to filter the search results by.")

class Type1Response(_Response):
    """
    Response model for Type 1 endpoint.
    """
    class TrialInfo(BaseModel):
        """
        Model representing information about a trial that matches the search criteria.
        """
        uuid: str = Field(..., description="The UUID of the trial.")
        trial_name: str = Field(..., description="The trial ID associated with the trial.")
        project_id: Optional[str] = Field(None, description="The project ID associated with the trial.")
        project_name: Optional[str] = Field(None, description="The project name associated with the trial.")
        metadata: Optional[dict] = Field(None, description="A dictionary containing metadata about the trial.")
        data_topics: Optional[List[str]] = Field(None, description="A list of data topics associated with the trial.")
        birth_timestamp: str = Field(..., description="The start time of the trial.")
        death_timestamp: str = Field(..., description="The end time of the trial.")
        created_at: str = Field(..., description="The timestamp when the trial was created.")
        updated_at: str = Field(..., description="The timestamp when the trial was last updated.")

    trials: Optional[List[TrialInfo]] = Field(None, description="A list of trial UUIDs that match the search criteria.")
    
class Type2Request(BaseModel):
    """
    Request model for Type 2 endpoint.
    """
    trial_uuid: str = Field(..., description="The UUID of the trial to retrieve data for.")
    data_format: _ResponseTypeEnum = Field(_ResponseTypeEnum.json, description="The desired data format for the response (e.g., 'json', 'csv').")
    time_start: Optional[str] = Field(None, description="The start time for the data retrieval in ISO 8601 format.")
    time_end: Optional[str] = Field(None, description="The end time for the data retrieval in ISO 8601 format.)")
    user_id: str = Field(..., description="The user ID requesting the data retrieval.")    
    user_domain: Optional[str] = Field(None, description="The domain of the user requesting the data retrieval.")
    frequency: int = Field(0, description="The frequency (in Hz) at which to retrieve data for the trial.")
    
class Type2Response(_Response):
    """
    Response model for Type 2 endpoint.
    """
    data: Optional[dict] = Field(None, description="The retrieved data for the trial, structured as a dictionary.")
    
class Type3Request(Type1Request):
    """
    Request model for Type 3 endpoint.
    """
    frequency: int = Field(0, description="The frequency (in Hz) at which to retrieve data for the trials.")
    
Type3Response = Union[Type1Response, Type2Response]