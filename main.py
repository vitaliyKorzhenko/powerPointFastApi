from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
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
                #print(shape);
                #pring all shape properties
                print(dir(shape))
                #file name without extension
                name = shape.image.filename.split('.')[0]
                
                images.append({
                    "name": shape.name,
                    "width": shape.width,
                    "height": shape.height,
                    "media_uniq_name": name + str(shape.shape_id) + '.' + shape.image.ext
                    
                })
    return images

def replace_image_in_presenation(prs, media_uniq_name, new_image_stream):
    print('new image path')
    imageFind = False
    for slide in prs.slides:
        for shape in slide.shapes:
            print(shape.shape_type)
            print(shape.name)
            # print(old_image_name)
            if shape.shape_type == 13:
                print('found image')
                print(shape.name)
                name = shape.image.filename.split('.')[0]
                current_uniq_name = name + str(shape.shape_id) + '.' + shape.image.ext
                print(current_uniq_name)
                print('Find image', media_uniq_name);
            if current_uniq_name == media_uniq_name:
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
    if (bucket.file_exists('pptx', name)):
        file = bucket.getObjectBody('pptx/' + name)
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


#class for body params
class PresentationParams(BaseModel):
    presentation: str
    #replacemnt array object media_unique_name: string, assets_file: string
    replacements: list
#replace image in presentation and save it to results (image get from pictures folder)
    resultFileName: str




@app.post("/presentation/replaceImage")
async def replace_image(presentationInfo: PresentationParams):
    print('START REPLACE IMAGE', presentationInfo)
    templatesBucket = 'pptx'
    imageBucket = 'img'
    presentation_name = presentationInfo.presentation
    bucket = BucketManager()
    if (bucket.file_exists(templatesBucket, presentation_name)):
        file = bucket.getObjectBody(templatesBucket + '/' + presentation_name)
        file_stream = BytesIO(file)
        presentation = pptx.Presentation(file_stream)
        newName = presentationInfo.resultFileName + '.pptx'

        #use for loop to replace all images in replacement array
        result_stream = BytesIO()
        print("BEFORE LOOP")
    for item in presentationInfo.replacements:
        print("ITEM", item)
        if (item['media_unique_name'] != None and item['assets_file'] != None):
            print("GO GO GO", item['media_unique_name'], item['assets_file'])
            if (bucket.file_exists(imageBucket, item["assets_file"])):
                print("exist image")
                image = bucket.getObjectBody(imageBucket + '/' + item['assets_file'])
                if (image == None):
                    return HTTPException(status_code=404, detail="Image not found")
                image_stream = BytesIO(image)
                result_prs = replace_image_in_presenation(presentation, item['media_unique_name'], image_stream)
                if (result_prs == None):
                    return HTTPException(status_code=404, detail="Image not found")
                else:
                    #save result presentation to results folder
                    print(newName)
                    #save to results folder
                    #bucket.put_object('results/' + newName, result_prs);
                    result_prs.save(result_stream)
                    result_stream.seek(0)
        else:
            return HTTPException(status_code=404, detail="Image not found")
    
        bucket.put_object(data=result_stream.read(), key='results/' + newName)
        return {"status": "success", "file": newName}

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