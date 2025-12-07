import hashlib
import os
import subprocess
import posixpath
from urllib.parse import urljoin, quote


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

def upload_to_webdav(source_paths: list[str], remote_name: str, remote_path: str, webdav_url: str):
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
                # Normalize to POSIX for WebDAV/URLs
                rel_file = rel_file.replace(os.sep, "/")
                remote_webdav_path = posixpath.join(remote_path, basename, rel_file)
            else:
                remote_webdav_path = posixpath.join(remote_path, os.path.basename(file))

            # Preserve scheme/host and percent-encode path segments
            url = urljoin(webdav_url.rstrip("/") + "/", quote(remote_webdav_path.lstrip("/"), safe="/"))

            filename = os.path.basename(file)
            tmp_results.append({
                "filename": filename,
                "checksum": checksum,
                "size": size,
                "url": url,
            })

        dest_subpath = posixpath.join(remote_path.lstrip("/"), basename)
        if os.path.isdir(path):
            destination = f"{remote_name}:{dest_subpath}"
            command = ["rclone", "copy", abs_path, destination, "--progress"]
        else:
            destination = f"{remote_name}:{dest_subpath}"
            command = ["rclone", "copyto", abs_path, destination, "--progress"]

        print(f"Upload: {path} → {destination}")
        try:
            subprocess.run(command, check=True)
            result.extend(tmp_results)
            print("✅ Uploaded successfully.\n")
        except subprocess.CalledProcessError as e:
            print(f"❌ Error uploading {path}: {e}\n")
        except FileNotFoundError:
            print("❌ rclone not found on PATH. Install rclone and retry.")

    return result
