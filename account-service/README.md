# Make sure your container is running first

```
docker compose up -d
````

# Run the script
```
docker exec -i $(docker compose ps -q postgres) \
  psql -U postgres -d account < src/main/resources/db/init.sql
```