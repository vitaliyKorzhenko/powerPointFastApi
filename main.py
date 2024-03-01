from fastapi import FastAPI, HTTPException

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

items = []

@app.post("/items/")
async def create_item(item: str):
    items.append(item)
    return item

#get by id
@app.get("/items/{item_id}")
async def get_item(item_id: int):
    if (item_id < len(items)):
        return items[item_id]
    else:
        return HTTPException(status_code=404, detail="Item not found")
    
# get items with limit
@app.get("/items/")
async def get_items(limit: int = 10):
    return items[0:limit]