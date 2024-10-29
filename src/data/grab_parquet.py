from minio import Minio
import requests
import os
from bs4 import BeautifulSoup
import sys

# Chemins et URL constants

SAVE_DIR = r"C:\Users\Aby\Documents\ATL-Datamart\data\raw"
DATA_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
BUCKET_NAME = "yellow-taxi-data"

def main():
    # Récupère les datasets de janvier 2024 à août 2024
    grab_data_jan_to_aug()
    
    # Récupère le dataset le plus récent
    grab_latest_data()
    
    # Ajout des données téléchargées dans Minio
    write_data_minio()

def grab_data_jan_to_aug():
    """Télécharge les datasets de janvier 2024 à août 2024 et les enregistre localement."""
    r = requests.get(DATA_URL)
    soup = BeautifulSoup(r.text, 'html.parser')
 
# Section pour les liens de 2024
link_section = soup.select('#faq2024 a')  # Mise à jour pour la section 2024
target_months = [f"2024-0{i}" for i in range(1, 9)]  # janvier à août 2024

os.makedirs(SAVE_DIR, exist_ok=True)

for link in link_section:
    file_url = link['href']
    if any(month in file_url for month in target_months) and "yellow" in file_url.lower():
        if not file_url.startswith("http"):
            file_url = "https://www.nyc.gov" + file_url
        download_file(file_url, SAVE_DIR)  # Vérifiez l'indentation ici

def grab_latest_data():
    """Télécharge le dataset le plus récent disponible et l'enregistre localement."""
    r = requests.get(DATA_URL)
    soup = BeautifulSoup(r.text, 'html.parser')

    # Récupérer le lien le plus récent qui contient "yellow"
    link_section = soup.select('#faq2024 a')  # Mise à jour pour la section 2024
    latest_link = None
    for link in link_section:
        if "yellow" in link['href'].lower():
            latest_link = link['href']
            break  # Sort de la boucle après avoir trouvé le premier lien "yellow"
if latest_link:
    if not latest_link.startswith("http"):
        latest_link = "https://www.nyc.gov" + latest_link
    
    os.makedirs(SAVE_DIR, exist_ok=True)
    download_file(latest_link, SAVE_DIR)  # Vérifiez l'indentation ici

def download_file(file_url, save_dir):
    """Télécharge un fichier depuis un URL et le sauvegarde dans un répertoire."""
    file_name = os.path.join(save_dir, file_url.split("/")[-1])
    print(f"Téléchargement de {file_name}...")
    file_data = requests.get(file_url)
    with open(file_name, 'wb') as f:
        f.write(file_data.content)

def write_data_minio():
    """
    Transfère les fichiers téléchargés vers Minio.
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"Bucket {BUCKET_NAME} créé.")
    else:
        print(f"Bucket {BUCKET_NAME} existe déjà.")
    
    for file_name in os.listdir(SAVE_DIR):
        file_path = os.path.join(SAVE_DIR, file_name)
        client.fput_object(BUCKET_NAME, file_name, file_path)
        print(f"{file_name} uploadé dans le bucket {BUCKET_NAME}.")

if __name__ == '__main__':
    sys.exit(main())

