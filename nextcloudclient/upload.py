import hashlib
import os
import subprocess
import posixpath

def compute_sha256_and_length(filepath):
    sha256 = hashlib.sha256()
    total_length = 0
    with open(filepath, 'rb') as f:
        while True:
            chunk = f.read(4096)
            if not chunk:
                break
            sha256.update(chunk)
            total_length += len(chunk)
    return sha256.hexdigest(), total_length

def get_all_files(path):
    if os.path.isfile(path):
        return [path]
    files = []
    for root, _, filenames in os.walk(path):
        for name in filenames:
            files.append(os.path.join(root, name))
    return files

def upload_to_nextcloud(source_paths: list[str], remote_name: str, remote_path: str, webdav_url: str):
    result = []
    for path in source_paths:
        if not os.path.exists(path):
            print(f"Path not found: {path}")
            continue

        abs_path = os.path.abspath(path)
        basename = os.path.basename(abs_path)
        files = get_all_files(abs_path)

        tmp_results = []

        for file in files:
            checksum,size  = compute_sha256_and_length(file)

            if os.path.isdir(path):
                rel_file = os.path.relpath(file, abs_path)
                remote_webdav_path = posixpath.join(remote_path, basename, rel_file)
            else:
                remote_webdav_path = posixpath.join(remote_path, os.path.basename(file))

            url = posixpath.join(webdav_url,remote_webdav_path)

            filename = file.split("/")[-1]
            tmp_results.append((filename, checksum, size, url))

        if os.path.isdir(path):
            destination = f"{remote_name}:{remote_path}/{basename}"
            command = ["rclone", "copy", abs_path, destination, "--progress"]
        else:
            destination = f"{remote_name}:{remote_path}/{basename}"
            command = ["rclone", "copyto", abs_path, destination, "--progress"]

        print(f"Upload: {path} → {destination}")
        try:
            subprocess.run(command, check=True)
            result.append(tmp_results)
            print("✅ Uploaded successfully.\n")
        except subprocess.CalledProcessError as e:
            print(f"❌ Error uploading {path}: {e}\n")


    return result
