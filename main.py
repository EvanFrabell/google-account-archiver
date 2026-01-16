import shutil
import time
import os

from googleapiclient.http import MediaFileUpload
from tqdm import tqdm
import mimetypes

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from urllib.parse import quote
from google.cloud import storage
from google.auth.transport.requests import AuthorizedSession


SCOPES = [
    "https://www.googleapis.com/auth/apps.licensing",
    "https://www.googleapis.com/auth/ediscovery",
    "https://www.googleapis.com/auth/devstorage.read_only",
    "https://www.googleapis.com/auth/drive"
]

# Retrieve the service account key file from Google Cloud
SERVICE_ACCOUNT_FILE = "service_account.json"
ADMIN_EMAIL = "efrabell@smithandassociates.com"
DELEGATED_USER = 'admin@smithandassociates.com'

USER_TO_EXPORT = ''

# Licenses
TARGET_SKU = '1010020026'
ENTERPRISE_SKUS = {
    '1010020026': 'Enterprise Standard',
    '1010340004': 'Archive License'
}

DOWNLOAD_DIR = "./downloads"
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB

ADMIN_CREDS = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES,).with_subject(ADMIN_EMAIL)
VAULT = build("vault", "v1", credentials=ADMIN_CREDS)

def assign_enterprise_license(service, user_email):
    print(f"Assigning {ENTERPRISE_SKUS[TARGET_SKU]} to {user_email}...")
    body = {
        'userId': user_email,
    }
    try:
        service.licenseAssignments().insert(
            productId='Google-Apps',
            skuId=TARGET_SKU,
            body=body
        ).execute()
        print(f"✅ Successfully assigned license to {user_email}")
        return True
    except HttpError as e:
        print(f"❌ Failed to assign license: {e}")
        return False

def check_and_fix_license(user_email):
    service = build('licensing', 'v1', credentials=ADMIN_CREDS)

    has_license = False
    for sku_id in ENTERPRISE_SKUS.keys():
        try:
            service.licenseAssignments().get(
                productId='Google-Apps', skuId=sku_id, userId=user_email
            ).execute()
            print(f"✅ User {user_email} already has an Enterprise license.")
            has_license = True
            break
        except HttpError as e:
            if e.resp.status == 404:
                continue
            else:
                print(f"Error checking license: {e}")
                return

    if not has_license:
        print(f"⚠️ User {user_email} is missing an Enterprise license.")
        assign_enterprise_license(service, user_email)


def start_vault_export(user_email: str):

    matter_body = {
        "name": f"Archive Matter: {user_email}",
        "description": f"Automated export for offboarding {user_email}",
        "state": "OPEN",
    }

    matter = VAULT.matters().create(body=matter_body).execute()
    matter_id = matter["matterId"]
    print(f"✅ Gmail Matter Created: {matter_id}")

    export_body = {
        "name": f"Gmail_Export_{user_email}_{int(time.time())}",
        "query": {
            "corpus": "MAIL",
            "dataScope": "ALL_DATA",
            "searchMethod": "ACCOUNT",
            "accountInfo": {"emails": [user_email]},
        },
        "exportOptions": {
            "mailOptions": {
                "exportFormat": "MBOX",
                "useNewExport": True,
            }
        },
    }

    print(f"Starting Export for {user_email}...")
    export = VAULT.matters().exports().create(matterId=matter_id, body=export_body).execute()
    export_id = export["id"]
    print(f"✅ Gmail Data Export Started. ID: {export_id}")
    return matter_id, export_id


def start_vault_export_gdrive(user_email: str):

    matter_body = {
        "name": f"Drive Archive Matter: {user_email}",
        "description": f"Automated Drive export for offboarding {user_email}",
        "state": "OPEN",
    }

    matter = VAULT.matters().create(body=matter_body).execute()
    matter_id = matter["matterId"]
    print(f"✅ Drive Matter Created: {matter_id}")

    # Updated body for Google Drive
    export_body = {
        "name": f"Drive_Export_{user_email}_{int(time.time())}",
        "query": {
            "corpus": "DRIVE",  # Changed from MAIL to DRIVE
            "dataScope": "ALL_DATA",
            "searchMethod": "ACCOUNT",
            "accountInfo": {"emails": [user_email]},
            "driveOptions": {
                "includeSharedDrives": False,  # Set to True if needed
                "includeTeamDrives": False  # Legacy term for Shared Drives
            }
        },
        "exportOptions": {
            "driveOptions": {
                "includeAccessInfo": True  # Includes metadata about who has access
            }
        },
    }

    print(f"Starting Drive Export for {user_email}...")
    export = VAULT.matters().exports().create(matterId=matter_id, body=export_body).execute()
    export_id = export["id"]
    print(f"✅ Google Drive Export Started. ID: {export_id}")

    return matter_id, export_id


def gcs_media_url(bucket_name: str, object_name: str) -> str:
    encoded_object = quote(object_name, safe="")
    return f"https://storage.googleapis.com/storage/v1/b/{bucket_name}/o/{encoded_object}?alt=media"


def download_blob_with_progress(creds, bucket_name: str, object_name: str, local_filename: str, total_size: int | None):
    os.makedirs(os.path.dirname(local_filename), exist_ok=True)

    session = AuthorizedSession(creds)
    url = gcs_media_url(bucket_name, object_name)

    # Stream the response
    with session.get(url, stream=True, timeout=600) as r:
        # Raise on auth / permission / not found, etc.
        r.raise_for_status()

        # Prefer server-reported length if total_size wasn't provided
        if not total_size:
            cl = r.headers.get("Content-Length")
            total_size = int(cl) if cl and cl.isdigit() else 0

        with open(local_filename, "wb") as f, tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=os.path.basename(local_filename),
        ) as pbar:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue
                f.write(chunk)
                pbar.update(len(chunk))

    print(f"Saved to: {local_filename}")


def process_export(m_id: str, e_id: str):
    vault = build("vault", "v1", credentials=ADMIN_CREDS)

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    print("\n⏳ Data Export In Progress...")
    while True:
        export = vault.matters().exports().get(matterId=m_id, exportId=e_id).execute()
        status = export.get("status")

        if status == "COMPLETED":
            print("\n✅ Vault Export Finished! Finalizing links...")
            time.sleep(10)
            break
        elif status == "IN_PROGRESS":
            time.sleep(10)
            continue
        else:
            raise RuntimeError(f"Export not completed. Status: {status}")

    print(f"Fetching export details for: {e_id}...")
    export = vault.matters().exports().get(matterId=m_id, exportId=e_id).execute()

    if export.get("status") != "COMPLETED":
        print(f"Export is not ready. Current status: {export.get('status')}")
        return

    files = export.get("cloudStorageSink", {}).get("files", [])
    if not files:
        print("No files found in cloudStorageSink.")
        return

    storage_client = storage.Client(project=ADMIN_CREDS.project_id, credentials=ADMIN_CREDS)

    for file_info in files:
        bucket_name = file_info["bucketName"]
        object_name = file_info["objectName"]
        local_filename = os.path.join(DOWNLOAD_DIR, object_name.split("/")[-1])

        print(f"\nDownloading {object_name} from bucket {bucket_name}...")

        # Try to get exact size for tqdm
        total_size = None
        try:
            blob = storage_client.bucket(bucket_name).blob(object_name)
            blob.reload()
            total_size = blob.size
        except Exception:
            total_size = None

        download_blob_with_progress(ADMIN_CREDS, bucket_name, object_name, local_filename, total_size)

def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    ).with_subject(DELEGATED_USER)
    return build('drive', 'v3', credentials=creds)

def get_or_create_folder(service, folder_name):
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    results = service.files().list(q=query, fields='files(id)').execute()
    folders = results.get('files', [])

    if folders:
        return folders[0]['id']

    file_metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder'}
    folder = service.files().create(body=file_metadata, fields='id').execute()
    return folder.get('id')


def upload_files_from_downloads():
    service = get_drive_service()
    folder_id = get_or_create_folder(service, USER_TO_EXPORT)

    files_to_upload = [f for f in os.listdir(DOWNLOAD_DIR) if os.path.isfile(os.path.join(DOWNLOAD_DIR, f))]

    if not files_to_upload:
        print("No files found to upload.")
        return

    # Increase chunk size to 32MB or 64MB for better performance on large files
    # Must be a multiple of 256 KB
    OPTIMIZED_CHUNK_SIZE = 32 * 1024 * 1024

    for filename in files_to_upload:
        file_path = os.path.join(DOWNLOAD_DIR, filename)
        file_size = os.path.getsize(file_path)
        mime_type, _ = mimetypes.guess_type(file_path)

        file_metadata = {'name': filename, 'parents': [folder_id]}
        media = MediaFileUpload(file_path, mimetype=mime_type, chunksize=OPTIMIZED_CHUNK_SIZE, resumable=True)

        request = service.files().create(body=file_metadata, media_body=media, fields='id')

        # Initialize the green progress bar for this specific file
        with tqdm(total=file_size, unit='B', unit_scale=True, unit_divisor=1024,
                  desc=f"Uploading {filename[:20]}...", colour='green') as pbar:
            response = None
            while response is None:
                status, response = request.next_chunk()
                if status:
                    # Update progress bar to the current bytes uploaded
                    pbar.n = int(status.resumable_progress)
                    pbar.refresh()

        print(f"✅ Finished: {filename}")


def remove_enterprise_license(user_email):
    service = build('licensing', 'v1', credentials=ADMIN_CREDS)

    print(f"Removing {ENTERPRISE_SKUS[TARGET_SKU]} from {user_email}...")
    try:
        # For license deletion, you provide the productId and the skuId
        service.licenseAssignments().delete(
            productId='Google-Apps',
            skuId=TARGET_SKU,
            userId=user_email
        ).execute()
        print(f"✅ Successfully removed license from {user_email}")
        return True
    except HttpError as e:
        if e.resp.status == 404:
            print(f"ℹ️ User {user_email} did not have this license assigned.")
        else:
            print(f"❌ Failed to remove license: {e}")
        return False


def clear_downloads_folder():
    print(f"\n🧹 Cleaning up local downloads directory: {DOWNLOAD_DIR}")

    if not os.path.exists(DOWNLOAD_DIR):
        print("Download directory does not exist, skipping cleanup.")
        return

    for filename in os.listdir(DOWNLOAD_DIR):
        file_path = os.path.join(DOWNLOAD_DIR, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)  # Removes file or symlink
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)  # Removes subdirectories
            print(f"Deleted: {filename}")
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")

    print("✅ Local cleanup complete.")

if __name__ == "__main__":
    USER_TO_EXPORT = input("Please enter the user email for offboarding: ")

    check_and_fix_license(USER_TO_EXPORT)
    time.sleep(60)

    # Gmail Matter
    matter_id, export_id = start_vault_export(USER_TO_EXPORT)
    print(f"Monitor this export at: https://vault.google.com/matter/{matter_id}/exports")
    process_export(matter_id, export_id)

    # Google Drive Matter
    matter_id, export_id = start_vault_export_gdrive(USER_TO_EXPORT)
    print(f"Monitor this export at: https://vault.google.com/matter/{matter_id}/exports")
    process_export(matter_id, export_id)

    upload_files_from_downloads()

    remove_enterprise_license(USER_TO_EXPORT)

    clear_downloads_folder()
    print("\n🏁 OFFBOARDING COMPLETE 🏁")