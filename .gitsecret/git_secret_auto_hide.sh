#!/bin/bash

"""
[Usage]
./git_secret_auto_hide.sh <file1> [file2] [file3] ...

paths/mapping.cfg: Secret File Management (Path Management)
"""

# 1. 기존 git secret 리스트를 백업
git secret list > old_secret_list.txt

# 2. 새로운 파일 추가
if [ -z "$1" ]; then
  echo "Error: No file provided to add to git secret."
  echo "Usage: $0 <file1> [file2] [file3] ..."
  exit 1
fi

# 여러 파일을 인자로 받아 추가
for file in "$@"; do
  git secret add "$file"
done

# 3. 새로 추가된 리스트와 기존 리스트를 비교
git secret list > new_secret_list.txt
new_files=$(comm -13 old_secret_list.txt new_secret_list.txt)

# 4. 새로 추가된 파일만 암호화
if [ -z "$new_files" ]; then
  echo "No new files to hide."
else
  # 반복문으로 파일 개별 처리
  echo "$new_files" | while IFS= read -r file; do
    if [ -n "$file" ]; then
      echo "Hiding new file: $file"
    fi
  done
  git secret hide
fi

# 5. 기존 리스트 파일 삭제
rm old_secret_list.txt new_secret_list.txt
