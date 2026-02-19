# Make sure your container is running first

```
docker compose up -d
````

# Run the script
```
docker exec -i postgres-account \
  psql -U postgres -d account < src/main/resources/db/init.sql
```