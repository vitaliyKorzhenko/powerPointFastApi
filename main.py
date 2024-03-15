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
    for slide in prs.slides:
        for shape in slide.shapes:
            # print(old_image_name)
            if shape.shape_type == 13:
                name = shape.image.filename.split('.')[0]
                current_uniq_name = name + str(shape.shape_id) + '.' + shape.image.ext
            if current_uniq_name == media_uniq_name:
                print('Find REPLACE IMAGE', media_uniq_name);
                try:
                    x = 5;
                    slide_part, rId = shape.part, shape._element.blip_rId
                    image_part = slide_part.related_part(rId)
                    new_image_stream.seek(0) 
                    image_part.blob = new_image_stream.read()
                    new_image_stream.close()
                except Exception as e:
                    print('Error READ', e)
                break
    #create random name for new presentation       
    return prs

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
    templates = bucket.get_files('pptx')
    return templates

#get files from results (folder results) 
@app.get("/presentation/getResults")
async def get_results():
    bucket = BucketManager()
    results = bucket.get_files('pptx')
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
    
    if bucket.file_exists(templatesBucket, presentation_name):
        file = bucket.getObjectBody(templatesBucket + '/' + presentation_name)
        file_stream = BytesIO(file)
        presentation = pptx.Presentation(file_stream)
        newName = presentationInfo.resultFileName + '.pptx'

        result_stream = BytesIO()

        for item in presentationInfo.replacements:
            if item.get('media_unique_name') and item.get('assets_file'):
                print("GO GO GO", item['media_unique_name'], item['assets_file'])
                if bucket.file_exists(imageBucket, item["assets_file"]):
                    image = bucket.getObjectBody(imageBucket + '/' + item['assets_file'])
                    if not image:
                        return HTTPException(status_code=404, detail="Image not found")
                    print("start create image stream")
                    byteImgIO = BytesIO(image)
                    byteImgIO.seek(0)
                    with byteImgIO as image_stream:
                        result_prs = replace_image_in_presenation(presentation, item['media_unique_name'], image_stream)
                        if not result_prs:
                            return HTTPException(status_code=404, detail="Image not found")
                        else:
                            # Update presentation
                            print("UPDATE PRESENTATION")
                            presentation = result_prs
                            #delete byteImgIO
                    
            else:
                return HTTPException(status_code=404, detail="Invalid replacement item")

        # Save updated presentation to results folder
        print("before loop");
        presentation.save(result_stream)
        result_stream.seek(0)
        bucket.put_object(data=result_stream.read(), key='pptx/' + newName)
        return {"status": "success", "file": newName}

    else:
        return HTTPException(status_code=404, detail="Presentation file not found")


   
