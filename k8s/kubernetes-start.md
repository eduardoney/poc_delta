## Cluster kubernetes local
### Criar um servidor do kind
Primeiro comando cria um cluster com o nome  pocdelta e o segundo muda contexto do kubectl
 ```
kind create cluster --name pocdelta
kubectl cluster-info --context kind-pocdelta
```
Para facilitar a excução dos commandos do kubectl é indicado criar um alias:
```
 Set-Alias -Name k -Value kubectl
```
## Monitoramento
Para monitorar as metricas do cluster e dos recursos criados geralmente é utilizado o https://prometheus.io/
Criação do namespace
```
kubectl create namespace monitoring
```
Deploy do prometheus
```
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add kube-state-metrics https://kubernetes.github.io/kube-state-metrics
helm repo update
helm install prometheus prometheus-community/prometheus --namespace monitoring --set server.service.type=LoadBalancer
```

## Ingestão
Para receber os eventos vamos utilizar o https://strimzi.io/ que é um Kafka para kubernetes.

### Instalação no kubernetes
Criar o namespace
```
kubectl create namespace ingestion
```
Deploy do operador no cluster
```
helm repo add strimzi https://strimzi.io/charts/
helm search hub strimzi/
helm repo update
helm install kafka strimzi/strimzi-kafka-operator --namespace ingestion --version 0.26.0
```
Validar se operador foi criado
```
kubectl get all -n ingestion
```
### Criação do cluster kafka
Aplicar os config-maps para export das metricas.
Tipos de armazenamentos:  
* JBOD - Cria um disco para armazenamento do dado  
* EPEMERAL - Os dados quando o pod morre o dado morre tbm.
```
kubectl apply -f .\k8s\kafka-jbod.yml -n ingestion
```
**obs:** Caso os cluster kafka não for criado, não retorna os pods utilizando o comando:
> kubectl get all -n ingestion  

Deve ser verificado o log do pod do **strimzi-cluster-operator-xxxxxx** que é resposável por subir o cluster.