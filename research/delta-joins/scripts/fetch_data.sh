#!/usr/bin/env bash
set -euo pipefail

# Downloads the study's datasets into $DATA (see env.sh) and makes the splits
# its incremental runs use.
#
# usage: scripts/fetch_data.sh [NAME...]
#
# NAME is a dataset, a split (`*-split`, which fetches its dataset first), or
# `galen-ref` (GALEN's reference P and Q, which the galen_*.py estimators
# read). With no NAME it fetches everything: about 1.4 GB to download and
# 6.4 GB unpacked. Each lands in $DATA/NAME as flat CSV files, and one that
# is already there is kept.
#
# The datasets are FlowLog's benchmark set, NemoYuu/flowlog_benchmark on
# Hugging Face.

# shellcheck source-path=SCRIPTDIR source=env.sh
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

HUB=https://huggingface.co/datasets/NemoYuu/flowlog_benchmark/resolve/main

DATASETS=(G5K-0.001 G40K-0.001 roadNet-CA web-BerkStan livejournal livejournal-sssp mind
  kernel medium cspa-httpd cspa-linux cspa-postgresql csda-httpd galen borrow batik)

# Each split's dataset, and the files whose every 1000th row it holds out.
declare -A SPLITS=(
  [G5K-split]="G5K-0.001 Arc.csv"
  [G40K-split]="G40K-0.001 Arc.csv"
  [roadNet-split]="roadNet-CA Arc.csv"
  [kernel-split]="kernel Arc.csv"
  [cspa-split]="cspa-httpd Assign.csv Dereference.csv"
)

# Downloads URL and unpacks its data files flat into $DATA/NAME. The archives
# differ in layout (flat, or under a NAME/ directory, some with macOS
# metadata), so only the data files are kept: CSV files, and the
# tab-separated .facts files of doop's datasets.
fetch() {
  local name=$1 url=$2
  [[ -d $DATA/$name ]] && return
  echo "fetch_data.sh: fetching $name"
  scratch=$(mktemp -d "$DATA/.fetch-$name.XXXXXX")
  local archive=$scratch/${url##*/}
  curl -fsSL --retry 3 -o "$archive" "$url"
  mkdir "$scratch/unpacked" "$scratch/$name"
  case $archive in
    *.zip) unzip -q "$archive" -d "$scratch/unpacked" ;;
    *.tar.gz) tar -xzf "$archive" -C "$scratch/unpacked" ;;
  esac
  find "$scratch/unpacked" \( -name '*.csv' -o -name '*.facts' \) ! -name '._*' ! -path '*/__MACOSX/*' \
    -exec mv -t "$scratch/$name" {} +
  if [[ -z $(ls -A "$scratch/$name") ]]; then
    echo "fetch_data.sh: $url holds no CSV or .facts files" >&2
    exit 1
  fi
  mv "$scratch/$name" "$DATA/$name"
  rm -rf "$scratch"
  scratch=
}

# A download cut short leaves nothing behind.
scratch=
trap 'rm -rf "${scratch:-}"' EXIT

make_split() {
  local name=$1 dataset files
  read -r dataset files <<<"${SPLITS[$name]}"
  [[ -d $DATA/$name ]] && return
  fetch "$dataset" "$HUB/dataset/csv/$dataset.zip"
  # shellcheck disable=SC2086 # $files is a list of file names.
  python3 "$ROOT/scripts/split.py" "$DATA/$dataset" "$DATA/$name" 1000 $files
}

mkdir -p "$DATA"
if (($# == 0)); then
  set -- "${DATASETS[@]}" galen-ref "${!SPLITS[@]}"
fi
for name in "$@"; do
  if [[ $name == galen-ref ]]; then
    fetch galen-ref "$HUB/test/datalog-batch/galen_galen.tar.gz"
  elif [[ -v SPLITS[$name] ]]; then
    make_split "$name"
  elif [[ " ${DATASETS[*]} " == *" $name "* ]]; then
    fetch "$name" "$HUB/dataset/csv/$name.zip"
  else
    echo "fetch_data.sh: unknown name $name; known: ${DATASETS[*]} galen-ref ${!SPLITS[*]}" >&2
    exit 2
  fi
done
