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
```



```bash
kompose convert -f chains-compose.yml -o chains-services.yaml
kompose convert -f chains-compose.yml -o chains-services.yaml
```

