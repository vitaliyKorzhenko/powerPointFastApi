# create class for work with s3 bucket use boto3
#

from typing import List
import boto3
import configparser
import os
from dotenv import load_dotenv
import json
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

# Загружаем переменные окружения из .env файла
load_dotenv()
class BucketManager:
    def __init__(self):
        #read config file
        print("start init");
        configPath = 'config.ini'
        config = configparser.ConfigParser()
        config.read(configPath)
        print(config.sections())
        #get data for aws configuration
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.getenv('AWS_REGION')
        #set bucket name
        self.bucket_name = config['s3']['bucket']
        #add to os environment credentials
        os.environ['AWS_ACCESS_KEY_ID'] = self.aws_access_key_id
        os.environ['AWS_SECRET_ACCESS_KEY'] = self.aws_secret_access_key
        os.environ['AWS_DEFAULT_REGION'] = self.aws_region
        os.environ['AWS_REGION'] = self.aws_region
        #create s3 resource
        self.s3 = boto3.resource('s3', region_name=self.aws_region)
        self.bucket = self.s3.Bucket(self.bucket_name)
        print(self.bucket_name)
    
    def get_all_objects(self):
        return [obj.key for obj in self.bucket.objects.all()]
    
    def get_all_folders(self):
        folders = set()

        # Итерация по всем объектам в бакете
        for obj in self.bucket.objects.all():
            # Разделение пути объекта по разделителям "/"
            # Извлечение первого элемента (папка)
            folder = obj.key.split('/')[0]
            
            # Добавление папки в множество
            folders.add(folder)

        return list(folders)
    
    #check if file exists in folder
    def file_exists(self, folder, file):
        for obj in self.bucket.objects.all():
            if obj.key == folder + '/' + file:
                return True
        return False
    
    #copy file from one folder to another
    def copy_file(self, source_folder, source_file, destination_folder, destination_file):
        copy_source = {
            'Bucket': self.bucket_name,
            'Key': source_folder + '/' + source_file
        }
        self.s3.meta.client.copy(copy_source, self.bucket_name, destination_folder + '/' + destination_file)

    #get file names from folder
    def get_files(self, folder):
        files = []
        for obj in self.bucket.objects.all():
            if obj.key.startswith(folder + '/'):
                files.append(obj.key)
        return files

    #get file names from folder
    def get_file_by_key(self, folder, keyFile):
        for obj in self.bucket.objects.all():
            if obj.key == folder + '/' + keyFile:
                return obj.key
        return None

    def get_object(self, key):
        return self.s3.Object(self.bucket_name, key).get()['Body'].read().decode('utf-8')
    
    def put_object(self, key, data):
        return self.s3.Object(self.bucket_name, key).put(Body=data)
    
    def delete_object(self, key):
        return self.s3.Object(self.bucket_name, key).delete()
    
    def get_bucket(self):
        return self.bucket
    
    def get_bucket_name(self):
        return self.bucket_name
    
    #find object in bucket by name
    def find_object(self, key):
        for obj in self.bucket.objects.all():
            if obj.key == key:
                return True
        return False
    
    #get bucket info
    def get_bucket_info(self):
        return {
            "bucket_name": self.bucket_name,
            "region": self.s3.meta.client.meta.region_name,
            # Другая информация о бакете, которую вы хотите включить
        }
    
    def getFileDataForPptx(self, filePath):
        print(f"Получение данных файла {filePath}")
        try:
           file = self.s3.Object(self.bucket_name, filePath)
           data = file.get()['Body'].read();
           return True
        except Exception as e:
            print("Error")
            #print(f"Ошибка при получении данных файла {filePath}: {e}")
        return None

    #get object body by key
    def getObjectBody(self, key):
        return self.s3.Object(self.bucket_name, key).get()['Body'].read()

    #get file in temlates folder by name
    def getFileInTemplates(self, fileName):
        return self.s3.Object(self.bucket_name, "templates/" + fileName).get()['Body'].read()

    #save file to results folder
    def saveFileToResults(self, fileName, data):
        return self.s3.Object(self.bucket_name, "results/" + fileName).put(Body=data)
    
    #save file to tests
    def saveFileToFolder(self, fileName, folder, data):
        return self.s3.Object(self.bucket_name, folder + fileName).put(Body=data)
    
    def saveFileToFolderAndGetPublicUrl(self, fileName, folder, data):
        #add public access to file
        self.saveFileToFolder(fileName, folder, data)
        self.addPublicAccess(folder + fileName)
        return self.getPublicUrl(folder + fileName)
    
    def addPublicAccess(self, key):
        try:
            return self.s3.ObjectAcl(self.bucket_name, key).put(ACL='public-read')
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                print(f"Object {key} does not exist. Cannot set public access.")
            else:
                raise
        return ''  # Возвращаем пустую строку
    
    #get public url for file
    def getPublicUrl(self, key):
        return f"https://{self.bucket_name}.s3.amazonaws.com/{key}"
    
    def get_files_in_data_folder(self, folder = "data/"):
        files = []
        for obj in self.bucket.objects.filter(Prefix=folder):
            if not obj.key.endswith('/'):  # Исключаем папки
                files.append(obj.key)
        return files
    
        
    def get_object_body(self, key):
        return self.s3.Object(self.bucket_name, key).get()['Body'].read().decode('utf-8')

    def get_object_body_all(self, key):
         return self.s3.Object(self.bucket_name, key).get()['Body'].read()
    
    def upload_file_to_data_s3(self, file_path, s3_key):
        try:
            self.s3.Bucket(self.bucket_name).upload_file(file_path, s3_key)
            print(f"Uploaded {file_path} to s3://{self.bucket_name}/{s3_key}")
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Credentials error: {e}")
        except Exception as e:
            print(f"Error uploading file to S3: {e}")
    
    def process_info_file(self, start_id=None, count=10, folder="data/"):
        # Чтение infoFile.json из корня
        with open('infoFile.json', 'r') as f:
            info_data = json.load(f)

        current_file = info_data['currentFile']
        print(f"Текущий файл: {current_file}")

        # Получение содержимого файла из папки data
        file_content = self.get_object_body(f'{folder}{current_file}')
        data = json.loads(file_content)

        # Получаем массив presentations
        presentations = data.get('presentations', [])

        # Преобразуем список презентаций в словарь для быстрого поиска по ID
        presentations_dict = {item['id']: item for item in presentations}

        # Если start_id не указан, начнем с первого элемента
        if start_id is None or start_id == 0:
            start_id = min(presentations_dict.keys())
        else:
            # Проверка, что start_id существует в словаре
            if start_id not in presentations_dict:
                print(f"Указан start_id, который не существует: {start_id}.")
                return [], len(presentations_dict), 0, start_id

     # Собираем элементы начиная с start_id и двигаемся по чанкам
        result_items = []
        ids_processed = 0
        current_id = start_id

        while ids_processed < count and current_id in presentations_dict:
            result_items.append(presentations_dict[current_id])
            ids_processed += 1
            # Переходим к следующему ID в списке
            next_ids = sorted(presentations_dict.keys())
            current_id_index = next_ids.index(current_id) + 1
            if current_id_index < len(next_ids):
                current_id = next_ids[current_id_index]
            else:
                break

        # Определяем количество выбранных элементов
        count_result = len(result_items)
        total_count = len(presentations_dict)

        # Печать результатов
        print(f"Общее количество элементов в файле: {total_count}")
        print(f"Количество элементов после ID {start_id}: {count_result}")

        if count_result > 0:
            print("Элементы есть - количество элементов:", count_result)
        else:
            print("Нет элементов.")
    
        # Возвращаем список результатов, общее количество элементов и следующий ID для продолжения
        next_start_id = current_id if current_id in presentations_dict else None
        return result_items, total_count, count_result, next_start_id
  


    def process_info_fileNew(self, current_file, start_id=None, count=10, folder = 'data/'):
        
        print('======== process_info_fileNew =========', current_file, start_id, count, folder)
        # Получение содержимого файла из папки data
        file_content = self.get_object_body(f'{folder}{current_file}')
        data = json.loads(file_content)

        # Получаем массив presentations
        presentations = data.get('presentations', [])

        # Преобразуем список презентаций в словарь для быстрого поиска по ID
        presentations_dict = {item['id']: item for item in presentations}

        # Если start_id не указан, начнем с первого элемента
        if start_id is None or start_id == 0:
            start_id = min(presentations_dict.keys())
        else:
            # Проверка, что start_id существует в словаре
            if start_id not in presentations_dict:
                print(f"Указан start_id, который не существует: {start_id}.")
                return [], len(presentations_dict), 0, start_id

     # Собираем элементы начиная с start_id и двигаемся по чанкам
        result_items = []
        ids_processed = 0
        current_id = start_id

        while ids_processed < count and current_id in presentations_dict:
            result_items.append(presentations_dict[current_id])
            ids_processed += 1
            # Переходим к следующему ID в списке
            next_ids = sorted(presentations_dict.keys())
            current_id_index = next_ids.index(current_id) + 1
            if current_id_index < len(next_ids):
                current_id = next_ids[current_id_index]
            else:
                break

        # Определяем количество выбранных элементов
        count_result = len(result_items)
        total_count = len(presentations_dict)

        # Печать результатов
        print(f"Общее количество элементов в файле: {total_count}")
        print(f"Количество элементов после ID {start_id}: {count_result}")

        if count_result > 0:
            print("Элементы есть - количество элементов:", count_result)
        else:
            print("Нет элементов.")
    
        # Возвращаем список результатов, общее количество элементов и следующий ID для продолжения
        next_start_id = current_id if current_id in presentations_dict else None
        return result_items, total_count, count_result, next_start_id

    def upload_string_to_s3(self, string_data, s3_key):
        try:
         # Используем метод put объекта S3 для загрузки данных
            print(f"Загрузка данных в s3://{self.bucket_name}/{s3_key}")
            self.s3.Object(self.bucket_name, s3_key).put(Body=string_data)
            print(f"Uploaded string data to s3://{self.bucket_name}/{s3_key}")
        except NoCredentialsError:
            print("Credentials not available.")
        except Exception as e:
            print(f"An error occurred: {e}")