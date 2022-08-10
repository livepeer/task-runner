#!/usr/bin/env bash

mock_target="$1"

project_dir=$(dirname $(realpath $0))

abs_cwd=$(pwd)
relative_cwd="${abs_cwd#"$project_dir/"}"

echo "Mocking interfaces in ./$relative_cwd/$mock_target to ./mocks/$relative_cwd/$mock_target"

mock_src="./$mock_target"
mock_dest="$project_dir/mocks/$relative_cwd/$mock_target"
go run github.com/golang/mock/mockgen -source="$mock_src" -destination="$mock_dest"