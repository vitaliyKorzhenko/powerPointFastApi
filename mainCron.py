import asyncio
import json
from typing import List
import logging
from bucketManager import BucketManager
from presentation_helper import PresentationParams, process_single_presentation
import os
from botocore.exceptions import ClientError
import re
import time


def append_results_to_s3(s3_key, new_results):
    bucket_manager = BucketManager()

    try:
        # Получаем текущий результатный файл с S3
        existing_data = bucket_manager.get_object_body(s3_key)
        existing_data = json.loads(existing_data) if existing_data else []
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            existing_data = []  # Если файла нет, задаем пустой список
        else:
            raise

    if not isinstance(existing_data, list):
        existing_data = []

    existing_data.extend(new_results)

    updated_data = json.dumps(existing_data, indent=4)

    # Загружаем обновленный файл обратно на S3 (или создаем новый)
    bucket_manager.upload_string_to_s3(updated_data, s3_key)

    # Проверяем наличие файла перед установкой публичного доступа
    try:
        bucket_manager.get_object_body(s3_key)  # Проверка существования
        bucket_manager.addPublicAccess(s3_key)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            print(f"Object {s3_key} does not exist. Cannot set public access.")
        else:
            raise

    public_url = bucket_manager.getPublicUrl(s3_key)
    print(f"Updated {s3_key} on S3 with {len(new_results)} new results. Public URL: {public_url}")

    return public_url





def update_state_info(current_file: str,  processed_elements: int, total_elements: int, result_file: str = ""):
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

def get_current_file(folder = 'data/'):
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
        if not any(state['file_name'] == current_file and state['status'] in ['finished', 'uploaded'] for state in state_info):
            current_file_id = info_data.get('currentFileId', 0)
            count = info_data.get('count', 3)
            return current_file, current_file_id, count
    
    # Если файл не указан или завершен, получаем первый файл из папки data
    data_files = bucket.get_files_in_data_folder(folder)
    if not data_files:
        print("No files found in 'data' folder.")
        return None, None, None
    
    # Ищем первый JSON-файл, который не является результатом и отсутствует в stateInfo
    for file in data_files:
        file_name = file.split('/')[-1]
        if file_name.endswith('.json') and not re.search(r'results?', file_name, re.IGNORECASE) and not re.search(r'result?', file_name, re.IGNORECASE):
            if not any(state['file_name'] == file_name and state['status'] in ['finished', 'uploaded'] for state in state_info):
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
        try:
            result_url = await process_single_presentation(presentation_info)
            results.append({
                'id': presentation_info.id,
                'result_url': result_url
            })
        except Exception as e:
            # Можно добавить логирование ошибки, если нужно
            print(f"Error processing presentation {presentation_info.id}: {e}")
            continue

    return results


# Настройка логирования
logging.basicConfig(filename='mycron.log', level=logging.INFO, format='%(asctime)s - %(message)s')



def lock_job(lock_file='job.lock'):
    # Если файл-блокировка существует и он создан недавно, задача не будет выполняться.
    if os.path.exists(lock_file):
        print(f"Lock file {lock_file} exists. Another job might be running.")
        return False
    
    # Создаем файл-блокировку
    with open(lock_file, 'w') as f:
        f.write(str(time.time()))
    
    return True

def unlock_job(lock_file='job.lock'):
    # Удаляем файл-блокировку после завершения задачи.
    if os.path.exists(lock_file):
        os.remove(lock_file)

async def job():
    if not lock_job():
        print('Job is already running. Exiting.')
        return

    try:
        bucket = BucketManager()
        folder = os.getenv('DATA_FOLDER', 'data/')
        print('Data folder:', folder)
        # Get current file
        current_file, current_file_id, curretCount = get_current_file(folder)
        
        curretCount = int(os.getenv('CHUNK_SIZE', 10))
        print('Chunk size:', curretCount)
        print('Processing data files...')

        if not current_file:
            print('No data files to process.')
            return
        
        print('Current file:', current_file, current_file_id, curretCount)

        data_files, total_count, count_result, new_file_id = bucket.process_info_file(start_id=current_file_id, count=curretCount, folder=folder)
        result = [PresentationParams(**item) for item in data_files]
        
        #info file path
        info_file_path = 'infoFile.json';

        results = await process_all_presentations(result)
        
        update_info_file(info_file_path, new_file_id)
        
        print('Data files processed and results updated.')

        # Upload result.json to S3
        s3_key = f"{folder}{current_file}_result.json"
        public_url = append_results_to_s3(s3_key, results)
        print('Public URL:', public_url)
        print('Before update state info', current_file, count_result, total_count, s3_key)
        update_state_info(current_file, count_result, total_count, s3_key)
        #upload infoFile.json to S3
        s3_keyInfo = f"{folder}infoResults.json"
        #public for infoFile.json
        bucket.upload_file_to_data_s3(info_file_path, s3_keyInfo)
        bucket.addPublicAccess(s3_keyInfo)
        #print public url for infoFile.json
        public_urlInfo = bucket.getPublicUrl(s3_keyInfo)
        print('Public URL Info:', public_urlInfo)

    finally:
        unlock_job()
        print('Job finished.')

# Время блокировки можно настроить в зависимости от задачи.


async def scheduler():
    minutes = int(os.getenv('CRON_TIME', 1))
    print('Scheduler started. Running job every', minutes, 'minutes.')
    
    while True:
        if not os.path.exists('job.lock'):
            await job()  # Запуск задачи
        else:
            print('Job is already running. Skipping this iteration.')

        #await asyncio.sleep(minutes * 60)  # Подождать `minutes` минут

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Удаление блокировочного файла при перезапуске
    lock_file = 'job.lock'
    if os.path.exists(lock_file):
        os.remove(lock_file)
    
    asyncio.run(scheduler())
