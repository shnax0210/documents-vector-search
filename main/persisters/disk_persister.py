import os
import shutil
import pickle

class DiskPersister: 
    def __init__(self, base_path):
        self.base_path = base_path

    def save_text_file(self, data, file_path):
        path = os.path.join(self.base_path, file_path)  
        
        self.__make_sure_path_exists(path)

        with open(path, 'w') as file:
            file.write(data)
    
    def read_text_file(self, file_path):
        path = os.path.join(self.base_path, file_path)  
        
        with open(path, 'r') as file:
            return file.read()

    def save_bin_file(self, data, file_path):
        path = os.path.join(self.base_path, file_path)  
        
        self.__make_sure_path_exists(path)

        with open(path, 'wb') as file:
            pickle.dump(data, file)

    def read_bin_file(self, file_path):
        path = os.path.join(self.base_path, file_path)  
        
        with open(path, 'rb') as file:
            return pickle.load(file)

    def create_folder(self, folder_name):
        directory_path = os.path.join(self.base_path, folder_name)
        os.makedirs(directory_path)
    
    def remove_folder(self, folder_name):
        directory_path = os.path.join(self.base_path, folder_name)

        if os.path.exists(directory_path):
            shutil.rmtree(directory_path)
    
    def remove_file(self, file_path):
        path = os.path.join(self.base_path, file_path)

        if os.path.exists(path):
            os.remove(path)

    def is_path_exists(self, relative_path):
        path = os.path.join(self.base_path, relative_path)
        return os.path.exists(path)

    def read_folder_files(self, relative_path):
        path = os.path.join(self.base_path, relative_path)
        return [file_path for file_path in os.listdir(path) if
            os.path.isfile(os.path.join(path, file_path))]

    def __make_sure_path_exists(self, path):
        directory_path = os.path.dirname(path)

        if directory_path and not os.path.exists(directory_path):
            os.makedirs(directory_path)
