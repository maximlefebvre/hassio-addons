import os
import requests
import time
import glob

# Configuration via variables d'environnement (passées par l'Add-on)
TOKEN = os.getenv('KDRIVE_TOKEN')
KDRIVE_ID = os.getenv('KDRIVE_ID')
BACKUP_DIR = "/backup"
CHUNK_SIZE = 8 * 1024 * 1024  # 8 Mo par morceau

HEADERS = {"Authorization": f"Bearer {TOKEN}"}
BASE_URL = f"https://api.infomaniak.com/3/drive/{KDRIVE_ID}"

print("Lancement de sync_kdrive.py")

def get_upload_url(filename):
    """Initialise la session d'upload et récupère l'URL de destination."""
    url = f"{BASE_URL}/upload"
    # On précise qu'on veut un upload fractionné si nécessaire
    params = {"filename": filename}
    response = requests.get(url, headers=HEADERS, params=params)
    return response.json().get('data', {}).get('upload_url')

def upload_file_chunked(file_path):
    filename = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    
    print(f"Début de l'upload : {filename} ({file_size / (1024*1024):.2f} MB)")
    
    # 1. Créer la session d'upload
    # Note: L'API kDrive permet l'upload direct ou via PUT/POST sur des chunks.
    # Pour simplifier et assurer la robustesse, on utilise ici le flux binaire.
    
    url = f"{BASE_URL}/upload"
    
    with open(file_path, 'rb') as f:
        offset = 0
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            
            # Définition des headers pour le chunking
            content_range = f"bytes {offset}-{offset + len(chunk) - 1}/{file_size}"
            chunk_headers = {
                **HEADERS,
                "Content-Range": content_range,
                "Content-Type": "application/octet-stream"
            }
            
            response = requests.put(url, headers=chunk_headers, data=chunk)
            
            if response.status_code not in [200, 201, 308]: # 308 = Permanent Redirect/Resume
                print(f"Erreur lors de l'envoi d'un chunk: {response.text}")
                return False
            
            offset += len(chunk)
            print(f"Progression : {min(100, int(offset/file_size*100))}%")

    print(f"Transfert terminé avec succès pour {filename}")
    return True

def run_sync():
    # Trouve tous les fichiers .tar et prend le plus récent
    list_of_files = glob.glob(f"{BACKUP_DIR}/*.tar")
    if not list_of_files:
        print("Aucun backup trouvé.")
        return
        
    latest_file = max(list_of_files, key=os.path.getctime)
    upload_file_chunked(latest_file)

if __name__ == "__main__":
    run_sync()
