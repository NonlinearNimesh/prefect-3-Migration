docker images --format '{{.Repository}}:{{.Tag}} {{.ID}}' | while read repo_tag id; do
  created_at=$(docker inspect --format='{{.Created}}' "$id")
  created_epoch=$(date --date="$created_at" +%s)
  two_days_ago=$(date --date='2 days ago' +%s)

  if [ "$created_epoch" -ge "$two_days_ago" ]; then
    echo "Force deleting $repo_tag ($id)..."
    docker rmi -f "$id"
  fi
done

