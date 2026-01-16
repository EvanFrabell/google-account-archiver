# google-account-archiver

### Automatic Operation
1. Install python - https://www.python.org/downloads/
2. Download service account (json) Key from https://console.cloud.google.com/ it should only in the Workspace-Archiver project.
3. Place service account .json file into google-account-archiver root directory.  Rename to service_account.json 
4. Open directory in PowerShell and type "python main.py" .
5. Enter targeted user's email and enjoy!
6. Double check uploaded archive files before deleting user!!! The script does not delete users...

### Manual Operation
1. (Google Admin) Add Google Enterprise License to user.
2. Open Google Vault https://www.vault.google.com
3. (Google Vault) Create new Matter for Gmail data.
   - Choose Gmail as Service. 
   - Add targeted user's email address.
4. (Google Vault) Create new Matter for Google Drive data.
   - Choose Gmail as Service. 
   - Add targeted user's email address.
5. (Google Vault) Once both Matters completed exporting, head to the EXPORTS tab and download all files.
6. (Admin Google Drive) Create new folder in My Drive.  Name it after the user's email.
7. (Admin Google Drive) Upload files from Matter export into the newly created folder.
8. (Google Admin) Remove license from user.
9. (Google Admin) Delete user and be certain to transfer Drive And Docs w/ Include files that are not share with anyone.