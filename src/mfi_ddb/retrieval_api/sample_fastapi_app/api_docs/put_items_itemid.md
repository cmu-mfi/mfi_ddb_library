# FastAPI Item API Documentation

This API allows you to update an item.

## PUT /items/{item_id}

Updates an item.

### Parameters

- `item_id` (path, required): The ID of the item to update. Must be an integer.

- `item` (body, required): The item to update. Must be a JSON object with the following properties:

    - `name` (string, required): The name of the item.

    - `description` (string, optional): The description of the item. If provided, must be a string of maximum 300 characters.

    - `price` (number, required): The price of the item. Must be a number greater than zero.

    - `tax` (number, optional): The tax of the item. If provided, must be a number.

### Example

Request:

```http
PUT /items/1 HTTP/1.1
Content-Type: application/json

{
    "item": {
        "name": "Item 1",
        "description": "This is item 1",
        "price": 9.99,
        "tax": 0.99
    }
}

Response:

HTTP/1.1 200 OK
Content-Type: application/json

{
    "item_id": 1,
    "item": {
        "name": "Item 1",
        "description": "This is item 1",
        "price": 9.99,
        "tax": 0.99
    }
}