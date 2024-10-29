from minio import Minio
import urllib.request
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup

def main():
    grab_data()

def grab_data() -> None:
    """Télécharge les fichiers du New York Yellow Taxi. 
    Les fichiers sont enregistrés dans le dossier spécifié et uploadés vers MinIO.
    """
    local_directory = r"C:\Users\Aby\OneDrive\Documents"
    os.makedirs(local_directory, exist_ok=True)

    # URL de la page contenant les liens vers les données
    page_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

    # Téléchargez la page
    response = requests.get(page_url)
    if response.status_code != 200:
        print(f"Erreur lors de la récupération de la page : {response.status_code}")
        return

    # Analysez la page avec BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")

    # Recherchez tous les liens vers les fichiers CSV pour 2024
    for month in range(1, 9):  # Pour janvier à août
        month_str = f"{month:02d}"
        link = soup.find("a", string=lambda text: text and f"2024-{month_str}" in text.lower())
        if link and 'href' in link.attrs:
            file_url = link['href']
            if not file_url.startswith('http'):
                file_url = requests.compat.urljoin(page_url, file_url)  # Gérer les URL relatives

            # Extraire le nom du fichier
            file_name = os.path.basename(file_url)
            local_file_path = os.path.join(local_directory, file_name)

            # Télécharger et enregistrer le fichier
            try:
                urllib.request.urlretrieve(file_url, local_file_path)
                print(f"Fichier téléchargé et enregistré à : {local_file_path}")
            except Exception as e:
                print(f"Erreur lors du téléchargement de {file_url} : {e}")

    write_data_minio(local_directory)

def write_data_minio(local_directory):
    """Upload des fichiers vers MinIO."""
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket_name = "yellow-taxi-data"

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket {bucket_name} créé.")
    else:
        print(f"Bucket {bucket_name} existe déjà.")
    
    for file_name in os.listdir(local_directory):
        file_path = os.path.join(local_directory, file_name)
        client.fput_object(bucket_name, file_name, file_path)
        print(f"{file_name} uploadé dans le bucket {bucket_name}.")

if __name__ == "__main__":
    main()
