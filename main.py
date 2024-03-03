from fastapi import FastAPI, HTTPException
from bucketManager import BucketManager
import configparser
from pptx import Presentation
import pptx
from PIL import Image
from io import BytesIO

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "API Service is running"}


def parse_pptx(prs):
    print('START')
    images = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if shape.shape_type == 13:
                #print all shape attributes
                print(shape.name)
                print(shape.shape_type)
                images.append({
                    "name": shape.name,
                    "width": shape.width,
                    "height": shape.height
                })
    return images

def replace_image_in_presenation(prs, old_image_name, new_image_stream):
    print('new image path')
    imageFind = False
    for slide in prs.slides:
        for shape in slide.shapes:
            print(shape.shape_type)
            print(shape.name)
            # print(old_image_name)
            if shape.name == old_image_name and shape.shape_type == 13:
                print('found image')
                print(shape.name)
                slide_part, rId = shape.part, shape._element.blip_rId
                image_part = slide_part.related_part(rId)    
                image_part.blob = new_image_stream.read()
                imageFind = True
                break
    #create random name for new presentation       
    if imageFind:
        return prs
    else:
        return None

@app.post("/presentation/parsing")
async def parse_presentation(name: str):
    print('START')
    bucket = BucketManager()
    if (bucket.file_exists('templates', name)):
        file = bucket.getObjectBody('templates/' + name)
        file_stream = BytesIO(file)
        presentation = pptx.Presentation(file_stream)
        images = parse_pptx(presentation)
        return images
    else:
        return HTTPException(status_code=404, detail="File not found")

#copy presentation from templates to results
@app.post("/presentation/copy")
async def copy_presentation(name: str):
    bucket = BucketManager()
    if (bucket.file_exists('templates', name)):
        bucket.copy_file('templates', name, 'results', name)
        return {"status": "success"}
    else:
        return HTTPException(status_code=404, detail="File not found")
    
@app.get("/presentation/getTemplates")
async def get_templates():
    bucket = BucketManager()
    templates = bucket.get_files('templates')
    return templates

#get files from results (folder results) 
@app.get("/presentation/getResults")
async def get_results():
    bucket = BucketManager()
    results = bucket.get_files('results')
    return results

#replace image in presentation and save it to results (image get from pictures folder)
@app.post("/presentation/replaceImage")
async def replace_image(old_image_name: str, new_image_name: str, presentation_name: str):
    bucket = BucketManager()
    if (bucket.file_exists('templates', presentation_name)):
        file = bucket.getObjectBody('templates/' + presentation_name)
        file_stream = BytesIO(file)
        presentation = pptx.Presentation(file_stream)
        if (bucket.file_exists('pictures', new_image_name)):
            image = bucket.getObjectBody('pictures/' + new_image_name)
            if (image == None):
                return HTTPException(status_code=404, detail="Picture not found")
            image_stream = BytesIO(image)
            result_prs = replace_image_in_presenation(presentation, old_image_name, image_stream)
            if (result_prs == None):
                return HTTPException(status_code=404, detail="Image not found")
            else:
                #save result presentation to results folder
                newName = presentation_name.split('.')[0] + '_result.pptx'
                print(newName)
                #save to results folder
                #bucket.put_object('results/' + newName, result_prs);
                result_stream = BytesIO()
                result_prs.save(result_stream)
                result_stream.seek(0)
                bucket.put_object(data=result_stream.read(), key='results/' + newName)

                #bucket.put_object('results/' + newName, result_stream.read())
                return {
                    "status": "success",
                    "path": "results/" + newName
                    }
        else:
            return HTTPException(status_code=404, detail="Image not found")
    else:
        return HTTPException(status_code=404, detail="File not found")
   
@app.get("/presentation/test")
async def test():
     print("start init");
     configPath = 'config.ini'
     config = configparser.ConfigParser()
     config.read(configPath)
     bucket = BucketManager()
     bucketInfo = bucket.get_bucket_info()
     print(bucketInfo)
     folders = bucket.get_all_folders()
     print(folders)
     #test folder 
     folder = "templates"
     files = bucket.get_files(folder)
     testFile = files[1];
     print("TEST FILE", testFile)
     testBody = bucket.getObjectBody(testFile)
     file_stream = BytesIO(testBody)
     testPptx = pptx.Presentation(file_stream)
     images = parse_pptx(testPptx)
     
     return {
            "bucket": config['s3']['bucket'],
            "info": bucketInfo,
            "folders": folders,
            "file_path": testFile,
            "images": images,
     }