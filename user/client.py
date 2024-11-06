import requests

HTTP_PORT = "4444"

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


def find_live_server():
    for address in server_addresses:
        url = f"{address}:" + HTTP_PORT
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == 200:
                print(f"Connected to live server at {url}")
                return url
        except requests.RequestException as e:
            print(f"Could not connect to {url}: {e}")
    return None

def handle_user_input(user_input):
    parts = user_input.split()

    if len(parts) == 0:
        return True

    if parts[0] == "create" and len(parts) == 3:
        local, hydfs = parts[1], parts[2]
        
        live_server = find_live_server()
        if live_server:
            try:
                data = {"local": local, "hydfs": hydfs}
                response = requests.post(f"{live_server}/create", json=data)
                print("Response from server:", response.text)
            except requests.RequestException as e:
                print("Request to server failed:", e)
        else:
            print("No live servers available")
    # ...

    else:
        print("Invalid option, please try again.")
    return True

def main():
    print("Enter 'exit' to quit.")
    while True:
        user_input = input("Enter command: ")
        if not handle_user_input(user_input):
            break

if __name__ == "__main__":
    main()
