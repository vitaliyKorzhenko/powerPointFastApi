import asyncio
import json
from typing import List
import logging
from bucketManager import BucketManager
from presentation_helper import PresentationParams, process_single_presentation
import os

def append_results_to_s3(s3_key, new_results):
    # Создаем экземпляр BucketManager
    bucket_manager = BucketManager()

    try:
        # Получаем текущий результатный файл с S3
        existing_data = bucket_manager.get_object_body(s3_key)
        existing_data = json.loads(existing_data) if existing_data else []
    except bucket_manager.s3_client.exceptions.NoSuchKey:
        existing_data = []

    # Убедитесь, что существующие данные - это список
    if not isinstance(existing_data, list):
        existing_data = []

    # Добавляем новые результаты
    existing_data.extend(new_results)

    # Преобразуем данные обратно в JSON-строку
    updated_data = json.dumps(existing_data, indent=4)

    # Загружаем обновленный файл обратно на S3
    bucket_manager.upload_string_to_s3(updated_data, s3_key)

    # Делаем файл публичным
    bucket_manager.addPublicAccess(s3_key)

    # Выводим публичный URL для файла
    public_url = bucket_manager.getPublicUrl(s3_key)
    print(f"Updated {s3_key} on S3 with {len(new_results)} new results. Public URL: {public_url}")

    return public_url



def update_state_info(current_file: str, processed_elements: int, total_elements: int, result_file: str = ""):
    info_file_path = 'infoFile.json'
    
    # Чтение infoFile.json
    with open(info_file_path, 'r') as f:
        info_data = json.load(f)

    # Обновляем stateInfo для текущего файла
    state_info = info_data.get('stateInfo', [])
    file_updated = False

    for state in state_info:
        if state['file_name'] == current_file:
            state['processed_elements'] += processed_elements
            state['result_file'] = result_file
            state['total_elements'] = total_elements
            if state['processed_elements'] >= total_elements:
                state['status'] = 'finished'
                info_data['currentFile'] = ""
                info_data['currentFileId'] = 0
            else:
                state['status'] = 'processed'

            file_updated = True
            break

    if not file_updated:
        # Если файл ещё не был добавлен в stateInfo
        state_info.append({
            "file_name": current_file,
            "status": "processed" if processed_elements < total_elements else "finished",
            "total_elements": total_elements,
            "processed_elements": processed_elements,
            "result_file": result_file
        })

    # Обновление stateInfo в infoFile.json
    info_data['stateInfo'] = state_info
    
    # Запись изменений в infoFile.json
    with open(info_file_path, 'w') as f:
        json.dump(info_data, f, indent=4)

    print(f"Updated stateInfo for {current_file}: {processed_elements}/{total_elements} elements processed.")

def get_current_file():
    # Инициализация BucketManager
    bucket = BucketManager()
    info_file_path = 'infoFile.json'
    
    # Чтение infoFile.json
    with open(info_file_path, 'r') as f:
        info_data = json.load(f)
    
    # Проверка наличия текущего файла и его статуса
    current_file = info_data.get('currentFile', '')
    state_info = info_data.get('stateInfo', [])
    
    # Если файл уже указан и не завершен, возвращаем его
    if current_file and state_info:
        last_state = state_info[-1]
        if last_state['status'] != 'finished':
            current_file_id = info_data.get('currentFileId', 0)
            count = info_data.get('count', 3)
            return current_file, current_file_id, count
    
    # Если файл не указан или завершен, получаем первый файл из папки data
    data_files = bucket.get_files_in_data_folder()
    if not data_files:
        print("No files found in 'data' folder.")
        return None, None, None
    
    # Ищем первый JSON-файл, который не является результатом и отсутствует в stateInfo
    for file in data_files:
        file_name = file.split('/')[-1]
        if file_name.endswith('.json') and not file_name.endswith('result.json'):
            if not any(state['file_name'] == file_name for state in state_info):
                current_file = file_name
                break
    else:
        print("All files in 'data' folder are already processed.")
        return None, None, None
    
    # Обновляем infoFile.json с новым currentFile, сбрасываем currentFileId и добавляем в stateInfo
    info_data['currentFile'] = current_file
    info_data['currentFileId'] = 0  # Сбрасываем currentFileId на 0
    info_data['stateInfo'].append({
        "file_name": current_file,
        "status": "processed",
        "total_elements": 0,  # Этот параметр можно обновить позже, когда будет известен
        "processed_elements": 0,  # Этот параметр также можно обновить позже,
        "result_file": ""  # Этот параметр также можно обновить позже
    })
    
    # Сохраняем обновленный infoFile.json
    with open(info_file_path, 'w') as f:
        json.dump(info_data, f, indent=4)
    
    return current_file, info_data['currentFileId'], info_data['count']





def append_results_to_json(file_path, new_results):
    # Check if the results file already exists
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            try:
                existing_data = json.load(file)
            except json.JSONDecodeError:
                existing_data = []
    else:
        existing_data = []
    
    # Ensure existing_data is a list
    if not isinstance(existing_data, list):
        existing_data = []
    
    # Append new results
    existing_data.extend(new_results)
    
    # Write updated data back to the file
    with open(file_path, 'w') as file:
        json.dump(existing_data, file, indent=4)

def update_info_file(file_path, current_file_id):
    try:
        # Read existing data
        with open(file_path, 'r') as f:
            data = json.load(f)

        # Update currentFileId
        data['currentFileId'] = current_file_id

        # Write updated data back to the file
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)

    except Exception as e:
        print(f"Error updating info file: {e}")

async def process_all_presentations(data_files: List[PresentationParams]):
    results = []
    
    for presentation_info in data_files:
        result_url = await process_single_presentation(presentation_info)
        results.append({
            'id': presentation_info.id,
            'result_url': result_url
        })

    return results


# Настройка логирования
logging.basicConfig(filename='mycron.log', level=logging.INFO, format='%(asctime)s - %(message)s')

async def job():
    bucket = BucketManager()
    
    # Get current file
    current_file, current_file_id, curretCount = get_current_file()
    
    print('Processing data files...')

    if not current_file:
        print('No data files to process.')
        return
    
    print('Current file:', current_file, current_file_id, curretCount)


    data_files, total_count, count_result, new_file_id = bucket.process_info_file(start_index=current_file_id, count=curretCount)
    result = [PresentationParams(**item) for item in data_files]
    
    #info file path
    info_file_path = 'infoFile.json';

    results = await process_all_presentations(result)
   
    
    update_info_file(info_file_path, new_file_id)
    
    print('Data files processed and results updated.')

      # Upload result.json to S3
    s3_key = f"data/{current_file}_result.json"
    public_url = append_results_to_s3(s3_key, results)
    print('Public URL:', public_url)
    print('Before update state info', current_file, count_result, total_count, s3_key)
    update_state_info(current_file, count_result, total_count, s3_key)
    #upload infoFile.json to S3
    s3_keyInfo = f"data/infoResults.json"
    #public for infoFile.json
    bucket.addPublicAccess(s3_keyInfo)
    bucket.upload_file_to_data_s3(info_file_path, s3_keyInfo)
    #print public url for infoFile.json
    public_urlInfo = bucket.getPublicUrl(s3_keyInfo)
    print('Public URL Info:', public_urlInfo)

async def scheduler():
    minutes = 2
    while True:
        await job()
        await asyncio.sleep(minutes * 60)  # Run the job every `minutes` minutes

if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(level=logging.INFO)

    # Запуск планировщика
    asyncio.run(scheduler())
