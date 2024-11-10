import requests
import os 

HTTP_PORT = "3333"
FILE_PATH_PREFIX = "../files/client/"

# List of server addresses to check
server_addresses = ["http://fa24-cs425-6801.cs.illinois.edu", 
                    "http://fa24-cs425-6802.cs.illinois.edu",
                    "http://fa24-cs425-6803.cs.illinois.edu", 
                    "http://fa24-cs425-6804.cs.illinois.edu",
                    "http://fa24-cs425-6805.cs.illinois.edu", 
                    "http://fa24-cs425-6806.cs.illinois.edu",
                    "http://fa24-cs425-6807.cs.illinois.edu", 
                    "http://fa24-cs425-6808.cs.illinois.edu",
                    "http://fa24-cs425-6809.cs.illinois.edu", 
                    "http://fa24-cs425-6810.cs.illinois.edu"]


CACHE_DIR="../files/client/"
def get_cache_timestamp(local):
    file_path = os.path.join(CACHE_DIR, local)
    if os.path.exists(file_path):
        print("modified time",os.path.getmtime(file_path))
        return os.path.getmtime(file_path)  # Get timestamp in seconds since epoch
        
    return None

def add_to_cache(local, content, timestamp):
    file_path = os.path.join(CACHE_DIR, local)
    with open(file_path, 'w') as f:
        f.write(content)
    os.utime(file_path, (timestamp, timestamp))  # Set timestamp for cache validation

def find_live_server():
    for address in server_addresses:
        url = f"{address}:" + HTTP_PORT
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == 200:
                print(f"Connected to live server at {url}")
                return url
        except requests.RequestException as e:
            print(f"Searching alive server, could not connect to {url}")
    return None

def handle_user_input(user_input):
    parts = user_input.split()

    if len(parts) == 0:
        return True

    if parts[0] == 'exit':
        return False

    if parts[0] == "list_mem_ids" and len(parts) == 2:
        try:
            server_id = int(parts[1])
            if server_id > 10 or server_id < 1:
                print("Invalid server id!")
                return True
            address = server_addresses[server_id - 1] + ":" + HTTP_PORT
            response = requests.get(address, timeout=2)
            if response.status_code == 200:
                print(f"Connected to live server at {address}")
                response = requests.get(f"{address}/membership")
                print(f"membership received from {server_id}: {response.text}")

        except requests.RequestException as e:
            print(f"Could not connect to {address}: {e}")
        return True
    
    if parts[0] == 'online' and len(parts) == 2:
        try:
            server_id = int(parts[1])
            if server_id > 10 or server_id < 1:
                print("Invalid server id!")
                return True
            address = server_addresses[server_id - 1] + ":" + HTTP_PORT
            response = requests.get(address, timeout=2)
            if response.status_code == 200:
                print(f"Connected to live server at {address}")
                response = requests.get(f"{address}/online")
                print(f"Is server {server_id} online? {response.text}")

        except requests.RequestException as e:
            print(f"Could not connect to {address}: {e}")
        return True

    if parts[0] == 'store' and len(parts) == 1:
        live_server = find_live_server()
        if live_server:
            try:
                response = requests.get(live_server, timeout=2)
                if response.status_code == 200:
                    response = requests.get(f"{live_server}/store")
                    print(response.text)
            except requests.RequestException as e:
                print(f"Could not connect to {live_server}: {e}")
        else:
            print("No live servers available")
        return True


    if parts[0] == "create" and len(parts) == 3:  # dd if=/dev/urandom of=largefile.txt bs=1M count=100
                                                # Above is a good way of generating a large text file with random text.
        local, hydfs = parts[1], parts[2]
        
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

    if parts[0] == "append" and len(parts) == 3: 
        local, hydfs = parts[1], parts[2]
        
        live_server = find_live_server()
        if live_server:
            try:
                # Step 1: Request authorization to append the file
                data = {"local": local, "hydfs": hydfs}
                response = requests.post(f"{live_server}/append", json=data)
                
                if response.ok:
                    print("Authorization from server:", response.text)
                    
                    # Step 2: Send the actual file content
                    with open(FILE_PATH_PREFIX + local, 'rb') as f:
                        upload_response = requests.put(f"{live_server}/append?filename={hydfs}", data=f)
                    
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

    if parts[0] == "get" and len(parts) == 3:  # dd if=/dev/urandom of=largefile.txt bs=1M count=100
                                             # Above is a good way of generating a large text file with random text.
        
        #get time stamp of file (cache)
        
        hydfs, local = parts[1], parts[2]
        cache_timestamp = get_cache_timestamp(local)

        live_server = find_live_server()
        if live_server:
            try:
                headers = {"Content-Type": "application/json"}
                # Step 1: Request authorization to create the file
                data = {"local": local, "hydfs": hydfs,  "cache_timestamp": str(cache_timestamp) if cache_timestamp else ""}
                response = requests.get(f"{live_server}/get", json=data, headers=headers)
                if response.status_code == 304:
                    print("Using cached version of the file.")
                    # with open(CACHE_DIR + local, 'r') as f:
                    #     print(f.read())

                    

                elif response.ok:
                    print("File retrieved from server and cached.")
                      # Step 2: Send the actual file content
                    server_timestamp = response.headers.get("Timestamp", None)
                    add_to_cache(local, response.text, float(server_timestamp))
                    with open(FILE_PATH_PREFIX + local, 'w') as f:
                        f.write(response.text)
                    print("File get successfully!")
                else:
                    print("Get file failed:", response.text)

            except requests.RequestException as e:
                print("Request to server failed:", e)
        else:
            print("No live servers available")
        return True


    print("Not a valid command")
    # ...
    return True

def main():
    print("Enter 'exit' to quit.")
    while True:
        user_input = input("Enter command: ")
        if not handle_user_input(user_input):
            break

if __name__ == "__main__":
    main()
