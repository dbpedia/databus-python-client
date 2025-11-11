import hashlib
import os
import posixpath
import re
import subprocess
from typing import List, Dict, Union

from rclone_python import rclone


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


def upload_with_rclone(source_paths: list[str], remote_name: str, remote_path: str) -> List[Dict[str, Union[str, int]]]:
    """
       Upload files or directories to any rclone remote using rclone-python.

       Args:
           source_paths: List of local paths to upload.
           remote_name: Name of the rclone remote (e.g., 'mydrive').
           remote_path: Remote path inside the remote (e.g., 'backup/').

       Returns:
           List of dictionaries with keys: filename, checksum, size, url.
       """

    if not rclone.is_installed():
        raise RuntimeError("rclone not found. Please install rclone.")

    result = []
    for path in source_paths:
        if not os.path.exists(path):
            print(f"Path not found: {path}")
            continue

        abs_path = os.path.abspath(path)
        basename = os.path.basename(abs_path)
        files = get_all_files(abs_path)

        for file in files:
            checksum,size  = compute_sha256_and_length(file)

            if os.path.isdir(path):
                rel_file = os.path.relpath(file, abs_path)
                # Normalize to POSIX
                rel_file = rel_file.replace(os.sep, "/")
                dest_subpath = f"{remote_path.rstrip('/')}/{basename}/{rel_file}"
            else:
                dest_subpath = f"{remote_path.rstrip('/')}/{os.path.basename(file)}"

            destination = f"{remote_name}:{dest_subpath}"

            # Upload File
            try:
                if os.path.isdir(path):
                    rclone.copy(abs_path, f"{remote_name}:{remote_path.rstrip('/')}/{basename}", args=["--progress"])
                else:
                    rclone.copyto(abs_path, destination, args=["--progress"])

                # Get URL
                try:
                    url = rclone.link(destination).strip()
                    if not url:
                        url = None
                except Exception:
                    url = None

                if not url:
                    try:
                        proc = subprocess.run(["rclone", "config", "show", remote_name],
                                              capture_output=True, text=True, check=True)
                        out = proc.stdout
                        m = re.search(r"(?m)^\s*url\s*=\s*(.+)$", out)
                        if m:
                            remote_url = m.group(1).strip()
                            url = posixpath.join(remote_url, dest_subpath)
                        else:
                            url = None
                    except Exception as e:
                        print(f"Cannot resolve remote URL: {e}")

                if url:
                    result.append({
                        "filename": os.path.basename(file),
                        "checksum": checksum,
                        "size": size,
                        "url": url,
                    })

                    print(f"✅ Uploaded {file} → {destination}")
                    print(f"   Cloud URL: {url}\n")
                else:
                    print("   No cloud URL available for this remote.\n")

            except Exception as e:
                print(f"❌ Error uploading {file}: {e}\n")

    return result