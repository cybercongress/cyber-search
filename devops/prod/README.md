### Useful commands

See cluster status
```bash
kubectl exec elassandra-0 -- nodetool status
```
Cqlsh for cluster
```bash
kubectl exec -it elassandra-0 -- cqlsh
```

Dive into elassandra docker container shell
```bash
kubectl exec -it elassandra-0 bash
curl -XGET 'localhost:9200/_cat/indices?v&pretty'
```



```bash
kompose convert -f chains-compose.yml -o chains-services.yaml
kubectl apply -f chains-services.yaml
```

```bash
kompose convert -f chains-pumps.yml -o chains-pumps.yaml
kubectl apply -f chains-pumps.yaml
```