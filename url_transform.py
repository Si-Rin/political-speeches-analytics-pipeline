"""
read each line from a file and transform the url to a new format as follows:
- remove everything before the first occurrence of "http"
- write the transformed url back to the file
"""
import re
import pathlib

def transform_urls(file_name):
    with open(file_name, "r") as file:
        transformed_urls = []
        for line in file:
            # remove everything before the first occurrence of "http"
            url = re.sub(r'.*?(http.*)', r'\1', line.strip())   # r'\1' captures everything after the first occurrence of "http"
            transformed_urls.append(url)
            print(url)
            
    # not necessary to close the file explicitly when using 'with' statement, as it automatically closes the file after the block is executed
            
    # rewrite the transformed url back to the file
    with open(file_name, "w") as file:
        for url in transformed_urls:
            file.write(url + "\n")
        print("Transformed URLs have been written back to the file.")


if __name__ == "__main__":
    # get the file path from the user
    file_path = pathlib.Path(input("File path containing URLs: "))
    transform_urls(file_path)