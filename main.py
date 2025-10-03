import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin

def fetch_football_logos():
    url = "https://football-logos.cc/all/"
    
    try:
        # Fetch the webpage
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Parse HTML content
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all image tags
        images = soup.find_all('img')
        
        print(f"Found {len(images)} images on the page")
        print("-" * 50)
        
        # Extract and display logo information
        logo_count = 0
        for img in images:
            src = img.get('src')
            alt = img.get('alt', 'No description')
            
            if src:
                # Convert relative URLs to absolute URLs
                full_url = urljoin(url, src)
                logo_count += 1
                print(f"Logo {logo_count}:")
                print(f"  Description: {alt}")
                print(f"  URL: {full_url}")
                print(f"  Filename: {os.path.basename(full_url)}")
                print("-" * 30)
                
        print(f"Total logos found: {logo_count}")
        
    except requests.RequestException as e:
        print(f"Error fetching the webpage: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def download_logos():
    """Optional function to download the logos"""
    url = "https://football-logos.cc/all/"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        images = soup.find_all('img')
        
        # Create directory for logos
        os.makedirs('football_logos', exist_ok=True)
        
        for i, img in enumerate(images):
            src = img.get('src')
            if src:
                full_url = urljoin(url, src)
                
                try:
                    img_response = requests.get(full_url)
                    img_response.raise_for_status()
                    
                    # Get file extension
                    filename = f"football_logos/logo_{i+1}_{os.path.basename(full_url)}"
                    
                    # Save the image
                    with open(filename, 'wb') as f:
                        f.write(img_response.content)
                    
                    print(f"Downloaded: {filename}")
                    
                except Exception as e:
                    print(f"Error downloading {full_url}: {e}")
                    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    print("Fetching football logos from https://football-logos.cc/all/")
    print("=" * 60)
    
    # Display logo information
    fetch_football_logos()
    
    # Ask if user wants to download
    choice = input("\nDo you want to download all logos? (y/n): ")
    if choice.lower() == 'y':
        print("\nDownloading logos...")
        download_logos()
