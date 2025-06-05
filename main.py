import re
import aiohttp
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
import io
from pptx.util import Inches
from typing import List
import time
import asyncio

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
    #print('find_media_info_by_shape', currentrId);
    for rel in shape.part.rels.values():
        #print (rel.target_ref)
        #print(rel.rId)
        if rel.rId == currentrId:
            media_name = rel.target_ref.split('/')[-1]
            return media_name
    return None;


def get_slide_background(slide):
    background = slide.background
    #check if background is image
    #get all information about background
    if background:
      print('background', background)
      print('fill type', background.fill.type)
      print('all property fill', dir(background.fill))
      print('FILL IMAGE', dir(background.fill.image))
      
      #print('background type',  background.background_type);
     
     

def find_slide_background(slide):
    for shape in slide.shapes:
        if shape.is_placeholder:
            print('shape is placeholder', shape)
            return shape
    return None



def parse_pptx(prs):
    print('START')
    images = []
    notImages =[];
    
    for slide in prs.slides:
        background_shape = find_slide_background(slide);
        print('background_shape', background_shape)
        for shape in slide.shapes:
            if shape.shape_type == 13:
                currentrId = shape._element.blip_rId;
                mediaName = find_media_info_by_shape(shape, currentrId)
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
            else: 
                notImages.append({
                     "name": shape.name,
                    "width": shape.width,
                    "height": shape.height,
                    "shape_id": shape.shape_id, 
                    "shape_type": shape.shape_type,
                })
    return images







from PIL import Image
from io import BytesIO

def get_image_dimensions(image_blob):
    with Image.open(BytesIO(image_blob)) as img:
        return img.size

def is_square(width, height):
    return width == height


def resize_image_to_fit(new_image_width, new_image_height, width_in_presentation, height_in_presentation):
    #print('Ширина и высота нового изображения:', new_image_width, new_image_height)
    #print('Ширина и высота в презентации:', width_in_presentation, height_in_presentation)

    # Calculate aspect ratios
    new_image_aspect_ratio = new_image_width / new_image_height
    target_aspect_ratio = width_in_presentation / height_in_presentation

    # Determine the dimension to scale by
    if new_image_aspect_ratio > target_aspect_ratio:
        # New image is wider than target size, scale based on width
        scale_factor =  width_in_presentation / new_image_width
    else:
        # New image is taller than target size, scale based on height
        scale_factor =  height_in_presentation / new_image_height

    # Apply scale factor to both dimensions to maintain aspect ratio
    target_width = int(new_image_width * scale_factor)
    target_height = int(new_image_height * scale_factor)

    #print('Наши размеры в результате!!!:', target_width, target_height)
    return target_width, target_height



def replace_image_in_presentation(prs, media_uniq_name, new_image_stream):
    for slide in prs.slides:
        for shape in slide.shapes:
            if shape.shape_type == 13:  # Check if shape is an image
                media_name = find_media_info_by_shape(shape, shape._element.blip_rId)
                #print('MEDIA NAME', media_name, media_uniq_name);
                if media_name and media_name == media_uniq_name:
                    #print('Найдено изображение для замены:', media_uniq_name)
                    try:
                        # Get dimensions of the original image in the presentation
                        image_part = shape.part.related_part(shape._element.blip_rId)
                        image_blob = image_part.blob
                        #width_in_presentation, heigth_in_presentation = get_image_dimensions(image_blob)
                        width_in_presentation = shape.width.inches * 96  # Convert to pixels
                        height_in_presentation = shape.height.inches * 96  # Convert to pixels
                        #print("ВЫСОТА И ШИРИНА В ПРЕЗЕНТАЦИИ:", width_in_presentation, "x", height_in_presentation)
                        
                        # Get dimensions of the new image from the stream
                        with Image.open(new_image_stream) as img:
                            new_image_width, new_image_height = img.size
                            #print("ВЫСОТА И ШИРИНА НОВОГО ИЗОБРАЖЕНИЯ!!:",  new_image_width, "x", new_image_height)
                            
                       
                            new_width, new_height = resize_image_to_fit(new_image_width, new_image_height, width_in_presentation, height_in_presentation)
                            #print("New size after resize:", new_width, "x", new_height)
                            
    
                            
                            img_resized = img.resize((new_width, new_height), Image.ADAPTIVE)

                            
                            
                            #image_resized to BytesIO
                            new_image_stream_resized = BytesIO()
                            img_resized.save(new_image_stream_resized, format='PNG')
                            new_image_stream_resized.seek(0)

                            #add image to current slide
                            #slide.shapes.add_picture(new_image_stream_resized, Inches(0), Inches(0), width=Inches(new_width/96), height=Inches(new_height/96))

                           
                            #  Устанавливаем новый размер формы
                            shape.width = Inches(new_width/96)
                            shape.height = Inches(new_height/96)

                            #save old shape position
                            left = shape.left
                            top = shape.top
                            #delete old shape
                            sp = shape
                            sp.element.getparent().remove(sp.element)

                            #add new shape
                            slide.shapes.add_picture(new_image_stream_resized, left, top, width=Inches(new_width/96), height=Inches(new_height/96))

                             # Replace the image in the presentation
                            # slide_part, rId = shape.part, shape._element.blip_rId
                            # image_part = slide_part.related_part(rId)
                            # image_part.blob = new_image_stream_resized.getvalue()
                            # new_image_stream_resized.close()
                                
                           
                           

                            #print('Изображение успешно заменено')
                    except Exception as e:
                        print('Ошибка:', e)
                    break
    return prs




def replace_image_background_in_presentation_withoutResize (prs, media_uniq_name, new_image_stream): 
    #print('#REPLACE IMAGE BACKGROUND IN PRESENTATION WITHOUT RESIZE');
    for slide in prs.slides:
        for shape in slide.shapes:
            if shape.shape_type == 13:  # Check if shape is an image
                media_name = find_media_info_by_shape(shape, shape._element.blip_rId)
                #print('MEDIA NAME', media_name, media_uniq_name);
                if media_name and media_name == media_uniq_name:
                   # print('Найдено изображение для замены:', media_uniq_name)
                    try:
                        # Replace the image in the presentation
                        slide_part, rId = shape.part, shape._element.blip_rId
                        image_part = slide_part.related_part(rId)
                        image_part.blob = new_image_stream.getvalue()
                        #print('Изображение успешно заменено')
                    except Exception as e:
                        print('Ошибка:', e)
                    break
    return prs





#class for body params
class PresentationParseParams(BaseModel):
    presentation: str
    #replacemnt array object media_unique_name: string, assets_file: string




#class for body params
class PresentationParams(BaseModel):
    presentation: str
    #replacemnt array object media_unique_name: string, assets_file: string
    replacements: list
#replace image in presentation and save it to results (image get from pictures folder)
    resultFileName: str
    


class ThumbnailsParams(BaseModel):
    presentation: str



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


async def cache_image_async(bucket, imageBucket, item):
    try:
        if not (item.get('media_unique_name') and item.get('assets_file')):
            return None, None
            
        if not bucket.file_exists(imageBucket, item["assets_file"]):
            print(f"File not found in bucket: {item['assets_file']}")
            return None, None
            
        image = bucket.getObjectBody(imageBucket + '/' + item['assets_file'])
        if not image:
            print(f"Failed to get image from bucket: {item['assets_file']}")
            return None, None
            
        return item['assets_file'], BytesIO(image)
    except Exception as e:
        print(f"Error caching image {item['assets_file']}: {str(e)}")
        return None, None

@app.post("/presentation/generateNewPresentationUseUrlOld")
async def generateNewPresentationUseUrlOld(presentationInfo: PresentationParams):
    start_time = time.time()
    print('BODY', presentationInfo)
    imageBucket = 'img'
    presentation_url = presentationInfo.presentation
    bucket = BucketManager()
    
    # Загружаем презентацию
    download_start = time.time()
    print('Downloading presentation from:', presentation_url)
    file_stream = download_file(presentation_url)
    if not file_stream:
        return HTTPException(status_code=404, detail="Presentation file not found")
    download_end = time.time()
    print(f"Time to download presentation: {download_end - download_start:.2f} seconds")
    
    # Открываем презентацию
    open_start = time.time()
    print("File stream downloaded successfully")
    presentation = pptx.Presentation(file_stream)
    result_stream = BytesIO()
    open_end = time.time()
    print(f"Time to open presentation: {open_end - open_start:.2f} seconds")
    
    # Начинаем отсчет времени замены изображений
    replace_start_time = time.time()
    for item in presentationInfo.replacements:
            if item.get('media_unique_name') and item.get('assets_file'):
                print("Processing replacement:", item['media_unique_name'], item['assets_file'])
                if bucket.file_exists(imageBucket, item["assets_file"]):
                    print("File exists in bucket")
                    image = bucket.getObjectBody(imageBucket + '/' + item['assets_file'])
                    if not image:
                        print("Failed to get image from bucket")
                        return HTTPException(status_code=404, detail="Image not found")
                    print("Got image from bucket")
                    byteImgIO = BytesIO(image)
                    byteImgIO.seek(0)
                    with byteImgIO as image_stream:
                        try:
                            item_start_time = time.time()
                            if item["type"] == "background":
                                print("Replacing background image")
                                result_prs = replace_image_background_in_presentation_withoutResize(presentation, item['media_unique_name'], image_stream)
                            else:    
                                print("Replacing regular image")
                                result_prs = replace_image_in_presentation(presentation, item['media_unique_name'], image_stream)
                            item_end_time = time.time()
                            print(f"Time to replace image {item['media_unique_name']}: {item_end_time - item_start_time:.2f} seconds")
                            
                            if not result_prs:
                                print("Failed to replace image")
                                return HTTPException(status_code=404, detail="Image not found")
                            else:
                                print("Image replaced successfully")
                                presentation = result_prs
                        except Exception as e:
                            print("Error during image replacement:", str(e))
                            raise HTTPException(status_code=500, detail=str(e))
                else:
                    print("File not found in bucket:", item["assets_file"])
                    return HTTPException(status_code=404, detail=f"Image {item['assets_file']} not found in bucket")
            else:
                return HTTPException(status_code=404, detail="Invalid replacement item")
    
    replace_end_time = time.time()
    print(f"Total time for all image replacements: {replace_end_time - replace_start_time:.2f} seconds")

    # Сохраняем результат
    save_start = time.time()
    print("Saving presentation")
    presentation.save(result_stream)
    result_stream.seek(0)
    save_end = time.time()
    print(f"Time to save presentation: {save_end - save_start:.2f} seconds")
    
    cleanedName = replace_symbol(presentationInfo.resultFileName) + '.pptx'
    resultName = re.sub(r'[^\x00-\x7f]',r'', cleanedName) 
    print("Returning presentation with name:", resultName)

    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
    print("\nDetailed time breakdown:")
    print(f"- Download presentation: {download_end - download_start:.2f} seconds")
    print(f"- Open presentation: {open_end - open_start:.2f} seconds")
    print(f"- Replace images: {replace_end_time - replace_start_time:.2f} seconds")
    print(f"- Save presentation: {save_end - save_start:.2f} seconds")
    print(f"- Other operations: {end_time - start_time - (download_end - download_start) - (open_end - open_start) - (replace_end_time - replace_start_time) - (save_end - save_start):.2f} seconds")

    return StreamingResponse(BytesIO(result_stream.read()), media_type='application/vnd.openxmlformats-officedocument.presentationml.presentation', headers={'Content-Disposition': f'attachment; filename="{resultName}"'})

@app.post("/presentation/generateNewPresentationUseUrlUseCashOld")
async def generateNewPresentationUseUrlUseCashOld(presentationInfo: PresentationParams):
    start_time = time.time()
    print('BODY', presentationInfo)
    imageBucket = 'img'
    presentation_url = presentationInfo.presentation
    bucket = BucketManager()
    
    try:
        # Загружаем презентацию
        download_start = time.time()
        file_stream = download_file(presentation_url)
        if not file_stream:
            return HTTPException(status_code=404, detail="Presentation file not found")
        download_end = time.time()
        print(f"Time to download presentation: {download_end - download_start:.2f} seconds")
        
        # Открываем презентацию
        open_start = time.time()
        presentation = pptx.Presentation(file_stream)
        result_stream = BytesIO()
        open_end = time.time()
        print(f"Time to open presentation: {open_end - open_start:.2f} seconds")
        
        # Предварительно загружаем все изображения
        cache_start = time.time()
        image_cache = {}
        for item in presentationInfo.replacements:
            if not (item.get('media_unique_name') and item.get('assets_file')):
                return HTTPException(status_code=404, detail="Invalid replacement item")
                
            if not bucket.file_exists(imageBucket, item["assets_file"]):
                return HTTPException(status_code=404, detail=f"Image {item['assets_file']} not found in bucket")
                
            # Загружаем изображение в кэш
            image = bucket.getObjectBody(imageBucket + '/' + item['assets_file'])
            if not image:
                return HTTPException(status_code=404, detail="Image not found")
            image_cache[item['assets_file']] = BytesIO(image)
        
        cache_end = time.time()
        print(f"Time to cache all images: {cache_end - cache_start:.2f} seconds")
        
        # Начинаем отсчет времени замены изображений
        replace_start_time = time.time()
        # Обрабатываем все замены
        for item in presentationInfo.replacements:
            try:
                image_stream = image_cache[item['assets_file']]
                image_stream.seek(0)
                
                item_start_time = time.time()
                if item["type"] == "background":
                    result_prs = replace_image_background_in_presentation_withoutResize(presentation, item['media_unique_name'], image_stream)
                else:    
                    result_prs = replace_image_in_presentation(presentation, item['media_unique_name'], image_stream)
                item_end_time = time.time()
                print(f"Time to replace image {item['media_unique_name']}: {item_end_time - item_start_time:.2f} seconds")
                    
                if not result_prs:
                    return HTTPException(status_code=404, detail="Image not found")
                    
                presentation = result_prs
                
            except Exception as e:
                print("Error during image replacement:", str(e))
                raise HTTPException(status_code=500, detail=str(e))
        
        replace_end_time = time.time()
        print(f"Total time for all image replacements: {replace_end_time - replace_start_time:.2f} seconds")
        
        # Сохраняем результат
        save_start = time.time()
        presentation.save(result_stream)
        result_stream.seek(0)
        save_end = time.time()
        print(f"Time to save presentation: {save_end - save_start:.2f} seconds")
        
        cleanedName = replace_symbol(presentationInfo.resultFileName) + '.pptx'
        resultName = re.sub(r'[^\x00-\x7f]',r'', cleanedName) 

        end_time = time.time()
        print(f"Total execution time: {end_time - start_time:.2f} seconds")
        print("\nDetailed time breakdown:")
        print(f"- Download presentation: {download_end - download_start:.2f} seconds")
        print(f"- Open presentation: {open_end - open_start:.2f} seconds")
        print(f"- Cache images: {cache_end - cache_start:.2f} seconds")
        print(f"- Replace images: {replace_end_time - replace_start_time:.2f} seconds")
        print(f"- Save presentation: {save_end - save_start:.2f} seconds")
        print(f"- Other operations: {end_time - start_time - (download_end - download_start) - (open_end - open_start) - (cache_end - cache_start) - (replace_end_time - replace_start_time) - (save_end - save_start):.2f} seconds")

        return StreamingResponse(
            BytesIO(result_stream.read()), 
            media_type='application/vnd.openxmlformats-officedocument.presentationml.presentation', 
            headers={'Content-Disposition': f'attachment; filename="{resultName}"'}
        )
    finally:
        # Очищаем кэш и освобождаем память
        if 'image_cache' in locals():
            for stream in image_cache.values():
                stream.close()
            image_cache.clear()
        print("Memory cleared: image cache has been freed")

@app.post("/presentation/generateNewPresentationUseUrl")
async def generateNewPresentationUseUrl(presentationInfo: PresentationParams):
    start_time = time.time()
    print('BODY', presentationInfo)
    imageBucket = 'img'
    presentation_url = presentationInfo.presentation
    bucket = BucketManager()
    
    try:
        # Загружаем презентацию
        download_start = time.time()
        file_stream = download_file(presentation_url)
        if not file_stream:
            return HTTPException(status_code=404, detail="Presentation file not found")
        download_end = time.time()
        print(f"Time to download presentation: {download_end - download_start:.2f} seconds")
        
        # Открываем презентацию
        open_start = time.time()
        presentation = pptx.Presentation(file_stream)
        result_stream = BytesIO()
        open_end = time.time()
        print(f"Time to open presentation: {open_end - open_start:.2f} seconds")
        
        # Параллельно загружаем все изображения
        cache_start = time.time()
        print("Starting parallel image caching...")
        
        # Создаем задачи для параллельной загрузки
        tasks = []
        for item in presentationInfo.replacements:
            if not (item.get('media_unique_name') and item.get('assets_file')):
                return HTTPException(status_code=404, detail="Invalid replacement item")
            tasks.append(cache_image_async(bucket, imageBucket, item))
        
        # Выполняем все задачи параллельно
        cached_results = await asyncio.gather(*tasks)
        
        # Фильтруем результаты и создаем кэш
        image_cache = {}
        failed_images = []
        
        for item, (key, value) in zip(presentationInfo.replacements, cached_results):
            if key is not None and value is not None:
                image_cache[key] = value
            else:
                failed_images.append(item['assets_file'])
        
        if failed_images:
            return HTTPException(status_code=404, detail=f"Failed to cache images: {', '.join(failed_images)}")
        
        cache_end = time.time()
        print(f"Time to cache all images: {cache_end - cache_start:.2f} seconds")
        print(f"Successfully cached {len(image_cache)} images")
        
        # Начинаем отсчет времени замены изображений
        replace_start_time = time.time()
        # Обрабатываем все замены
        for item in presentationInfo.replacements:
            try:
                image_stream = image_cache[item['assets_file']]
                image_stream.seek(0)
                
                item_start_time = time.time()
                if item["type"] == "background":
                    result_prs = replace_image_background_in_presentation_withoutResize(presentation, item['media_unique_name'], image_stream)
                else:    
                    result_prs = replace_image_in_presentation(presentation, item['media_unique_name'], image_stream)
                item_end_time = time.time()
                print(f"Time to replace image {item['media_unique_name']}: {item_end_time - item_start_time:.2f} seconds")
                    
                if not result_prs:
                    return HTTPException(status_code=404, detail="Image not found")
                    
                presentation = result_prs
                
            except Exception as e:
                print("Error during image replacement:", str(e))
                raise HTTPException(status_code=500, detail=str(e))
        
        replace_end_time = time.time()
        print(f"Total time for all image replacements: {replace_end_time - replace_start_time:.2f} seconds")
        
        # Сохраняем результат
        save_start = time.time()
        presentation.save(result_stream)
        result_stream.seek(0)
        save_end = time.time()
        print(f"Time to save presentation: {save_end - save_start:.2f} seconds")
        
        cleanedName = replace_symbol(presentationInfo.resultFileName) + '.pptx'
        resultName = re.sub(r'[^\x00-\x7f]',r'', cleanedName) 

        end_time = time.time()
        print(f"Total execution time: {end_time - start_time:.2f} seconds")
        print("\nDetailed time breakdown:")
        print(f"- Download presentation: {download_end - download_start:.2f} seconds")
        print(f"- Open presentation: {open_end - open_start:.2f} seconds")
        print(f"- Cache images: {cache_end - cache_start:.2f} seconds")
        print(f"- Replace images: {replace_end_time - replace_start_time:.2f} seconds")
        print(f"- Save presentation: {save_end - save_start:.2f} seconds")
        print(f"- Other operations: {end_time - start_time - (download_end - download_start) - (open_end - open_start) - (cache_end - cache_start) - (replace_end_time - replace_start_time) - (save_end - save_start):.2f} seconds")

        return StreamingResponse(
            BytesIO(result_stream.read()), 
            media_type='application/vnd.openxmlformats-officedocument.presentationml.presentation', 
            headers={'Content-Disposition': f'attachment; filename="{resultName}"'}
        )
    finally:
        # Очищаем кэш и освобождаем память
        if 'image_cache' in locals():
            for stream in image_cache.values():
                stream.close()
            image_cache.clear()
        print("Memory cleared: image cache has been freed")


                                 

class PresentationsParams(BaseModel):
    presentations: List[PresentationParams]


async def download_file_async(url: str) -> BytesIO:
    print('DOWNLOAD FILE ASYNC', url)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                print('ERROR DOWNLOAD FILE ASYNC', response.status)
                raise HTTPException(status_code=404, detail="Presentation file not found")
            return BytesIO(await response.read())










