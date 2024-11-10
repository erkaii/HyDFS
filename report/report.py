import os
import sys
import random
import string
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from user.client import find_live_server
import requests
import numpy as np
import time


FILE_PATH_PREFIX = "generated_files/"
FILE_WRITE_PREFIX="../files/client/"

def generate_random_content(size_in_kb):
    # Generate random string of approximately `size_in_kb` kilobytes
    size_in_bytes = size_in_kb * 1024
    return ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation, k=size_in_bytes))


def generate_files(num_files, file_size_kb, output_directory):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    
    file_paths = []
    for i in range(num_files):
        file_name = os.path.join(output_directory, f'business_{i+1}.txt')
        content = generate_random_content(file_size_kb)
        with open(file_name, 'w', encoding='utf-8') as f:
            f.write(content)
        file_paths.append(file_name)
        print(f'Generated: {file_name}')
    return file_paths


def upload_to_hydfs(local,hydfs):
         
    
    live_server = find_live_server()
    if live_server:
        try:
            # Step 1: Request authorization to create the file
            data = {"local": local, "hydfs": hydfs}
            response = requests.post(f"{live_server}/create", json=data)
            
            if response.ok:
                print("Authorization from server:", response.text)
                
                # Step 2: Send the actual file content
                with open(FILE_PATH_PREFIX + local, 'rb') as f:
                    upload_response = requests.put(f"{live_server}/create?filename={hydfs}", data=f)
                
                if upload_response.ok:
                    print("File upload complete:", upload_response.text)
                else:
                    print("File upload failed:", upload_response.text)
            else:
                print("Authorization failed:", response.text)

        except requests.RequestException as e:
            print("Request to server failed:", e)
    else:
        print("No live servers available")
    return True



def upload_files_to_hydfs(file_paths):
    for file_path in file_paths:
        
        local = os.path.basename(file_path)
        hydfs = "hydfs_"+local
        uploaded = upload_to_hydfs(local,hydfs)

        if uploaded:
            print(f"File upload is successful for {file_path}")
        else:
            print(f"File upload is not successful for {file_path}")


def get_files_from_hydfs(file_numbers_array):
    # Iterate through the file numbers in the array
    total_latency = 0  # Variable to accumulate total latency
    successful_retrievals = 0  # Counter for successful file retrievals
    for i in file_numbers_array:
        # Construct HyDFS file name (assuming `i` represents the file number)
        hydfs = f"hydfs_business_{i}.txt"
        
        # Construct local file name
        local = f"local_business_{i}.txt"
        
        
        print(f"HyDFS file: {hydfs} -> Local file: {local}")
        live_server = find_live_server()
        if live_server:
            try:
                  # Start timer for latency measurement
                start_time = time.time()
                # Step 1: Request authorization to create the file
                data = {"local": local, "hydfs": hydfs}
                response = requests.get(f"{live_server}/get", json=data)
                
                if response.ok:
                    # Step 2: Send the actual file content
                    with open(FILE_WRITE_PREFIX + local, 'w') as f:
                        f.write(response.text)
                    print(f"File:{hydfs} get successfully!")
                    end_time = time.time()
                    total_latency += (end_time - start_time)
                    successful_retrievals += 1  # Count successful retrieval
                else:
                    print(f"Get file:{hydfs} failed:", response.text)

            except requests.RequestException as e:
                print("Request to server failed:", e)
        else:
            print("No live servers available")

         # Calculate and display mean latency if there were successful retrievals

    print("successful",successful_retrievals,"n",len(file_numbers_array))
    if successful_retrievals > 0:
        mean_latency = total_latency / successful_retrievals
        print(f"Mean latency for file retrievals: {mean_latency:.4f} seconds")
    else:
        print("No successful file retrievals to calculate latency.")


    return True

        
        
 
if __name__ == '__main__':
    num_files = 10
    file_size_kb = 4
    output_directory = 'generated_files'

   
    file_paths = generate_files(num_files, file_size_kb, output_directory)    
     # Upload files to HyDFS
    upload_files_to_hydfs(file_paths)

        # Parameters draws:30k, range:10k
    num_draws = 50
    range_max = 100

    # Array with uniformly random numbers between 1 and 10,000
    uniform_random_array = np.random.randint(1, range_max + 1, num_draws)

    # Array with numbers following a Zipfian distribution (bounded by 1 and 10,000)
    zipf_random_array = np.random.zipf(a=1.8, size=num_draws)
    zipf_random_array = np.clip(zipf_random_array, 1, range_max)  # Keep values within the range

    print("Uniform Random Array:", uniform_random_array[:10])  # Display first 10 values for verification
    print("Zipfian Random Array:", zipf_random_array[:10])      # Display first 10 values for verification

    files_array = uniform_random_array

    get_files_from_hydfs(files_array)
