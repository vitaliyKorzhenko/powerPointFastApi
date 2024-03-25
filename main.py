import re
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from bucketManager import BucketManager
from pptx import Presentation
import pptx
from PIL import Image
from io import BytesIO
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from urllib.parse import unquote
import requests
from pathvalidate import replace_symbol, sanitize_filename
#initialize app
app = FastAPI()

#add cors   
origins = [
    "http://pptx.slideedu.com",
    "https://pptx.slideedu.com",
    "http://pptx.slideedu.com:8000",
    "https://pptx.slideedu.com:8000",
    "http://localhost",
    "http://localhost:8000",
    "http://159.203.124.0",
    "http://159.203.124.0:8000",
    
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



@app.get("/")
def read_root():
    return {"Hello": "API Service is running"}






def print_rel_for_slide(slide):
    relathionships = slide.rels

    for rel in relathionships:
        print("Отношение:")
        print("  Идентификатор отношения:", rel.rId)
        print("  Тип отношения:", rel.reltype)
        print("  Целевой URI:", rel.target_ref)
        print("  Целевой части:", rel.target_part)


def find_media_info_by_shape(shape, currentrId):
    print('find_media_info_by_shape', currentrId);
    for rel in shape.part.rels.values():
        print (rel.target_ref)
        print(rel.rId)
        if rel.rId == currentrId:
            media_name = rel.target_ref.split('/')[-1]
            return media_name
    return None;

def parse_pptx(prs):
    print('START')
    images = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if shape.shape_type == 13:
                currentrId = shape._element.blip_rId;
                mediaName = find_media_info_by_shape(shape, currentrId)
                if mediaName:
                    print('mediaName', mediaName)
                else: 
                    print('mediaName', 'not found')

                images.append({
                    "media_name": mediaName,
                    "rId": currentrId,
                    "name": shape.name,
                    "width": shape.width,
                    "height": shape.height,
                    "shape_id": shape.shape_id, 
                    "shape_type": shape.shape_type,
                    "filename": shape.image.filename,
                    "ext": shape.image.ext,
                    "left": shape.left,
                    "top": shape.top,
                    "imageSize": shape.image.size,
                    "sha1": shape.image.sha1,

                })
    return images

def replace_image_in_presenation(prs, media_uniq_name, new_image_stream):
    for slide in prs.slides:
        mediaName = None
        for shape in slide.shapes:
            # print(old_image_name)
            if shape.shape_type == 13:
                mediaName = find_media_info_by_shape(shape, shape._element.blip_rId)
            if mediaName and mediaName == media_uniq_name:
                print('Find REPLACE IMAGE', media_uniq_name, new_image_stream);
                try:
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



#get thumbnails from thumbnails folder
@app.get("/presentation/getThumbnails")
async def get_thumbnails():
    bucket = BucketManager()
    thumbnails = bucket.get_files('thumbnails')
    return thumbnails

#class for body params
class PresentationParams(BaseModel):
    presentation: str
    #replacemnt array object media_unique_name: string, assets_file: string
    replacements: list
#replace image in presentation and save it to results (image get from pictures folder)
    resultFileName: str


class ThumbnailsParams(BaseModel):
    presentation: str


    



    


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
        resultKey = 'pptx/' + newName
        bucket.put_object(data=result_stream.read(), key=resultKey)
        bucket.addPublicAccess(resultKey)
        publicUrl = bucket.getPublicUrl(resultKey)
        return {
            "status": "success", 
            "file": newName, 
            "url": publicUrl,
            }

    else:
        return HTTPException(status_code=404, detail="Presentation file not found")




@app.post("/presentation/generateNewPresentation")
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
        return StreamingResponse(BytesIO(result_stream.read()), media_type='application/vnd.openxmlformats-officedocument.presentationml.presentation', headers={'Content-Disposition': f'attachment; filename="{newName}"'})



    else:
        return HTTPException(status_code=404, detail="Presentation file not found")



def download_file(url):
    """
    Downloads a file from the given URL and saves it with the specified filename.
    
    :param url: The URL of the file to download.
    :param filename: The name under which the file will be saved.
    :return: The file stream if successful, or None in case of error.
    """
    try:
        # Request the file content from the URL
        response = requests.get(url)

        if response.status_code == 200:
            # Save the file content
            file_stream = BytesIO(response.content)
            return file_stream
        else:
            print("Error downloading file. Error code:", response.status_code)
            return None

    except Exception as e:
        print("Error:", e)
        return None


                                   
@app.post("/presentation/generateNewPresentationUseUrl")
async def generateNewPresentationUseUrl(presentationInfo: PresentationParams):
    print('START REPLACE IMAGE', presentationInfo)
    imageBucket = 'img'
    presentation_url = presentationInfo.presentation
    bucket = BucketManager()
    file_stream = download_file(presentation_url)
    if not file_stream:
        return HTTPException(status_code=404, detail="Presentation file not found")
    print("fileSTREAM NOT NONE");
    presentation = pptx.Presentation(file_stream)
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
    cleanedName = replace_symbol(presentationInfo.resultFileName) + '.pptx'
    resultName = re.sub(r'[^\x00-\x7f]',r'', cleanedName) 
    print("RESULT CLEANED", resultName);

    return StreamingResponse(BytesIO(result_stream.read()), media_type='application/vnd.openxmlformats-officedocument.presentationml.presentation', headers={'Content-Disposition': f'attachment; filename="{resultName}"'})



#return presentation by name
@app.get("/presentation/getPresentation")
async def get_presentation(name: str):
    bucket = BucketManager()
    if (bucket.file_exists('pptx', name)):
        file = bucket.getObjectBody('pptx/' + name)
        return StreamingResponse(BytesIO(file), media_type='application/vnd.openxmlformats-officedocument.presentationml.presentation', headers={'Content-Disposition': f'attachment; filename="{name}"'})
    else:
        return HTTPException(status_code=404, detail="File not found")