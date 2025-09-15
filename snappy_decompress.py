"""
Snappy Decompression Script
---------------------------

$AUTHOR$
$DATE$
$VERSION$
$LICENSE$
$DESCRIPTION$
"""

import os
import snappy

# with open(local_snz_path, "rb") as src, open(out_path, "wb") as dst:
        # snappy.stream_decompress(src=src, dst=dst)

local_snz_path = "./"
dst_dir = "./snappy_decompress/"

if not os.path.exists(dst_dir):
    os.makedirs(dst_dir)
for filename in os.listdir(local_snz_path):
    if filename.endswith(".snz"):
        out_path = os.path.join(dst_dir, filename[:-4])  # Remove .snz extension
        with open(os.path.join(local_snz_path, filename), "rb") as src, open(out_path, "wb") as dst:
            snappy.stream_decompress(src=src, dst=dst)
        print(f"Decompressed {filename} to {out_path}")

# I want to open notepad using os module and capture the PID
pid = os.system("notepad.exe")
print(f"Notepad opened with PID: {pid}")

# Want to read each decompressed file and print the first 100 lines
for filename in os.listdir(dst_dir):
    if not filename.endswith(".snz"):
        out_path = os.path.join(dst_dir, filename)
        print(f"Reading {out_path}:")
        with open(out_path, "r") as file:
            for i, line in enumerate(file):
                if i >= 100:
                    break
                print(line.strip())
        print("\n" + "="*40 + "\n")  # Separator between files
