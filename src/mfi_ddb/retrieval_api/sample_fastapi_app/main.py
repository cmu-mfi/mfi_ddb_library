from fastapi import FastAPI, Form, Request, status, Depends, Body, Query
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse

import uvicorn
from pydantic import BaseModel, Field
from typing import Annotated
from pathlib import Path
from typing import Optional

app = FastAPI(
    title="My API with documentation",
    description="This is a very fancy FastAPI project, with auto docs for the API.",
    version="1.0.0",
    contact={
        "name": "Emanuele Orecchio",
        "url": "https://medium.com/@emanueleorecchio",
        "email": "emanueleorecchio@thisisnotmyemail.com",
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    },
)

@app.get("/items/")
async def read_items(q: Optional[str] = Query(None, description="Query string for the items to search for")):
    """
    Retrieve items based on a query string.

    - **q**: Optional query string to search for items
    """
    return {"items": [{"item_id": "Foo"}, {"item_id": "Bar"}]}

class Item(BaseModel):
    name: str
    description: str = Field(
        default="", title="The description of the item", max_length=300
    )
    price: float = Field(gt=0, description="The price must be greater than zero")
    tax: float = 0.0

@app.put("/items/{item_id}", description=Path('api_docs/put_items_itemid.md').read_text(), summary = 'Put Item')
async def update_item(item_id: int, item: Annotated[Item, Body(embed=True)]):
    results = {"item_id": item_id, "item": item}
    return results

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8200)

    #to run the app, open a terminal and run the command: uvicorn main:app --reload
    # uvicorn main:app --reload --host 0.0.0.0 --port 8200
    # /redoc and /docs both work