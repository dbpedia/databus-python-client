"""WebDAV/Nextcloud upload helper used by the deploy CLI.

This module computes SHA-256 checksums and sizes for local files and uses
``rclone`` to copy files to a remote WebDAV/Nextcloud instance. The
`upload_to_webdav` function returns a list of metadata dictionaries suitable
for passing to ``deploy_from_metadata``.
"""

import hashlib
import os
import posixpath
import subprocess
from urllib.parse import quote, urljoin


def compute_sha256_and_length(filepath):
    """Compute the SHA-256 hex digest and total byte length of a file.

    Args:
        filepath: Path to the file to hash.

    Returns:
        Tuple of (sha256_hex, size_in_bytes).
    """
    sha256 = hashlib.sha256()
    total_length = 0
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(4096)
            if not chunk:
                break
            sha256.update(chunk)
            total_length += len(chunk)
    return sha256.hexdigest(), total_length


def get_all_files(path):
    """Return a list of all files for a path.

    If `path` is a file, returns a single-element list. If it is a directory,
    walks the directory recursively and returns absolute file paths.
    """
    if os.path.isfile(path):
        return [path]
    files = []
    for root, _, filenames in os.walk(path):
        for name in filenames:
            files.append(os.path.join(root, name))
    return files


def upload_to_webdav(
    source_paths: list[str], remote_name: str, remote_path: str, webdav_url: str
):
    """Upload local files or folders to a configured rclone remote.

    Args:
        source_paths: List of files or directories to upload.
        remote_name: Name of the rclone remote (e.g., "nextcloud").
        remote_path: Destination path on the remote.
        webdav_url: Public WebDAV URL used to construct download URLs.

    Returns:
        A list of dicts with keys: ``filename``, ``checksum``, ``size``, ``url``.
    """
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
            checksum, size = compute_sha256_and_length(file)

            if os.path.isdir(path):
                rel_file = os.path.relpath(file, abs_path)
                # Normalize to POSIX for WebDAV/URLs
                rel_file = rel_file.replace(os.sep, "/")
                remote_webdav_path = posixpath.join(remote_path, basename, rel_file)
            else:
                remote_webdav_path = posixpath.join(remote_path, os.path.basename(file))

            # Preserve scheme/host and percent-encode path segments
            url = urljoin(
                webdav_url.rstrip("/") + "/",
                quote(remote_webdav_path.lstrip("/"), safe="/"),
            )

            filename = os.path.basename(file)
            tmp_results.append(
                {
                    "filename": filename,
                    "checksum": checksum,
                    "size": size,
                    "url": url,
                }
            )

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
