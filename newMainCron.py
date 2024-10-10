import asyncio
import json
from typing import List
import logging
from bucketManager import BucketManager
from presentation_helper import PresentationParams, process_single_presentation
import os
import re
import time
from db_connection import get_connection

conn = get_connection()
#chunk_size = 10
count = 10


def append_results_to_s3(s3_key, new_results):
    bucket_manager = BucketManager()

    try:
        # Попытка получить текущие данные из S3
        try:
            existing_data = bucket_manager.get_object_body(s3_key)
            existing_data = json.loads(existing_data) if existing_data else []
            print(f"Existing data loaded from {s3_key}")
        except ClientError as e: # type: ignore
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                print(f"Key {s3_key} does not exist. Creating a new file with results.")
                existing_data = []  # Если файла нет, задаем пустой список
            else:
                print(f"Error fetching data from S3: {e}")
                return None  # Прерываем выполнение при других ошибках

        # Убедимся, что данные - это список, если нет, создаем новый список
        if not isinstance(existing_data, list):
            print("Existing data is not a list, initializing as empty list.")
            existing_data = []

        # Добавляем новые результаты к существующим данным
        existing_data.extend(new_results)
        updated_data = json.dumps(existing_data, indent=4)

        # Загружаем обновленные данные на S3
        try:
            bucket_manager.upload_string_to_s3(updated_data, s3_key)
            print(f"Uploaded updated data to {s3_key}")
        except ClientError as e:  # type: ignore
            print(f"Error uploading data to S3: {e}")
            return None

        # Проверка существования файла и установка публичного доступа
        try:
            bucket_manager.get_object_body(s3_key)  # Проверка существования
            bucket_manager.addPublicAccess(s3_key)
            print(f"Public access granted for {s3_key}")
        except ClientError as e: # type: ignore
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                print(f"Object {s3_key} does not exist. Cannot set public access.")
            else:
                print(f"Error setting public access: {e}")
                return None

        # Получаем публичный URL
        public_url = bucket_manager.getPublicUrl(s3_key)
        print(f"Updated {s3_key} on S3 with {len(new_results)} new results. Public URL: {public_url}")
        return public_url

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None




def update_state_info(current_file: str, currentpresentationid: int, processed_elements: int, total_elements: int, result_file: str = ""):
    try:
        cur = conn.cursor()

        # Проверка, существует ли уже запись для текущего файла
        check_file_query = '''
        SELECT id, currentpresentationid, processed_elements, total_elements FROM files WHERE file_name = %s
        '''
        cur.execute(check_file_query, (current_file,))
        file_record = cur.fetchone()

        if file_record:
            file_id, db_currentpresentationid, db_processed_elements, db_total_elements = file_record

            # Обновляем значения processed_elements, result_file, total_elements и статус
            new_processed_elements = db_processed_elements + processed_elements
            status = 'finished' if new_processed_elements >= total_elements else 'processed'

            update_file_query = '''
            UPDATE files 
            SET processed_elements = %s, total_elements = %s, result_file = %s, status = %s, currentpresentationid = %s 
            WHERE id = %s
            '''
            cur.execute(update_file_query, (
                new_processed_elements, total_elements, result_file, status, currentpresentationid, file_id
            ))
        else:
            # Если записи нет, создаем новую
            status = 'finished' if processed_elements >= total_elements else 'processed'

            insert_file_query = '''
            INSERT INTO files (file_name, status, total_elements, processed_elements, result_file, currentpresentationid) 
            VALUES (%s, %s, %s, %s, %s, %s)
            '''
            cur.execute(insert_file_query, (
                current_file, status, total_elements, processed_elements, result_file, currentpresentationid
            ))

        # Фиксируем изменения
        conn.commit()
        print(f"Updated stateInfo for {current_file}: {processed_elements}/{total_elements} elements processed.")
    
    except Exception as e:
        print(f"Error while updating stateInfo: {e}")
        conn.rollback()
    
    finally:
        cur.close()




def get_current_processed_file(conn):
    # Создаем курсор для выполнения запроса
    cur = conn.cursor()

    try:
        # Ищем файл со статусом 'processed'
        cur.execute('SELECT file_name, currentpresentationid FROM files WHERE status = %s LIMIT 1', ('processed',))
        result = cur.fetchone()

        if result:
            current_file, current_file_id = result
            return current_file, current_file_id
        else:
            print("No files with status 'processed' found.")
            return None, None

    except Exception as e:
        print(f"Error fetching processed file: {e}")
        return None, None

    finally:
        cur.close()



def insert_new_file(conn, file_name):
    try:
        cur = conn.cursor()

        # SQL-запрос для добавления нового файла в таблицу
        insert_query = '''
        INSERT INTO files (file_name, status, total_elements, processed_elements, result_file, aws_status, currentpresentationid)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        '''
        cur.execute(insert_query, (file_name, 'processed', 0, 0, '', False, 0))
        conn.commit()

        # Получаем ID вставленной записи
        new_file_id = cur.fetchone()[0]

        print(f"New file '{file_name}' added with ID: {new_file_id}")
        return new_file_id

    except Exception as e:
        print(f"Error inserting new file: {e}")
        return None

    finally:
        cur.close()


def get_current_file(conn, folder='data/'):
    # Проверяем наличие файла со статусом 'processed' в базе данных
    current_file, current_file_id = get_current_processed_file(conn)
    print("CURRENT FILE ========", current_file, current_file_id)
    # Если найден файл со статусом 'processed', возвращаем его
    if current_file:
        return current_file, current_file_id
    
    # Если нет файла со статусом 'processed', получаем файлы из папки
    bucket = BucketManager()
    data_files = bucket.get_files_in_data_folder(folder)
    
    if not data_files:
        print("No files found in 'data' folder.")
        return None, None, None
    

    
    # Ищем первый JSON-файл, который не является результатом и отсутствует в базе
    for file in data_files:
        file_name = file.split('/')[-1]
        if file_name.endswith('.json') and not re.search(r'results?', file_name, re.IGNORECASE) and not re.search(r'result?', file_name, re.IGNORECASE):
            # Проверяем, есть ли файл в базе данных со статусом 'finished' или 'uploaded'
            cur = conn.cursor()
            cur.execute("SELECT status FROM files WHERE file_name = %s", (file_name,))
            file_status = cur.fetchone()
            
            if not file_status or file_status[0] not in ['finished', 'uploaded']:
                # Файл не завершен или не загружен, можно использовать
                current_file = file_name
                cur.close()
                break
    else:
        print("All files in 'data' folder are already processed.")
        return None, None, None
    
    print('Go INSET FILE!!', current_file);
    # Если найден новый файл, добавляем его в базу данных
    current_file_id = insert_new_file(conn, current_file)
    
    # Возвращаем новый текущий файл и его сохраненный ид пока тоже 0! ибо мы создали новый файл!
    return current_file, 0 


async def process_all_presentations(data_files: List[PresentationParams]):
    results = []
    
    for presentation_info in data_files:
        try:
            print("START SINGLE", presentation_info)
            result_url = await process_single_presentation(presentation_info)
            print('FINISH SINGE', result_url);
            results.append({
                'id': presentation_info.id,
                'result_url': result_url
            })
        except Exception as e:
            # Можно добавить логирование ошибки, если нужно
            print(f"Error processing presentation {presentation_info.id}: {e}")
            continue

    return results

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
        current_file, current_file_id = get_current_file(get_connection(), folder)
        print('CURRENT FILE!!!', current_file, current_file_id);
        curretCount = int(os.getenv('CHUNK_SIZE', 10))
        print('Chunk size:', curretCount)
        print('Processing data files...')

        if not current_file:
            print('No data files to process.')
            return
        
        print('Current file:', current_file, current_file_id, curretCount)

        data_files, total_count, count_result, new_presentation_id = bucket.process_info_fileNew(current_file, start_id=current_file_id, count=curretCount, folder=folder)
        result = [PresentationParams(**item) for item in data_files]

        print("========== RESULT PRESENTATION =====")
        
        results = await process_all_presentations(result)
                
        print('Data files processed and results updated.')

        # Upload result.json to S3
        s3_key = f"{folder}{current_file}_result.json"
        public_url = append_results_to_s3(s3_key, results)
        print('Public URL:', public_url)
        print('Before update state info', current_file, count_result, total_count, s3_key)
        update_state_info(current_file, new_presentation_id, count_result, total_count, s3_key)
  
    finally:
        unlock_job()
        print('Job finished.')

# Время блокировки можно настроить в зависимости от задачи.


async def scheduler():
    #minutes = int(os.getenv('CRON_TIME', 1))
    minutes = 1
    print('Scheduler started. Running job every', minutes, 'minutes.')
    
    while True:
        if not os.path.exists('job.lock'):
            print('WE GO RUN JOB JOB')
            await job()  # Запуск задачи
        else:
            print('Job is already running. Skipping this iteration.')

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Удаление блокировочного файла при перезапуске
    lock_file = 'job.lock'
    if os.path.exists(lock_file):
        os.remove(lock_file)
    
    asyncio.run(scheduler())
