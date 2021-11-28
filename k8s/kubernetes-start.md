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
--config maps de metricas
kubectl apply -f .\k8s\kafka\metrics\ -n ingestion
kubectl apply -f .\k8s\kafka\kafka-jbod.yaml -n ingestion
```
**obs:** Caso os cluster kafka não for criado, não retorna os pods zookerper e broker, validar utilizando o comando:
> kubectl get all -n ingestion  

Deve ser verificado o log do pod do **strimzi-cluster-operator-xxxxxx** que é resposável por subir o cluster.  
Para validar se o cluster está funcional executar o comando:
```
kubectl wait kafka/dev --for=condition=Ready --timeout=300s -n ingestion
```

**Criando topicos**
```
kubectl apply -f .\k8s\kafka\kafka-topic.yaml -n ingestion
```
Para listar os tópicos:
```
kubectl get kafkatopics -n ingestion
```
**Enviando mensagens**  
Validar qual a porta do broker
```
kubectl get service dev-kafka-external-bootstrap -n ingestion -o=jsonpath="{.spec.ports[0].nodePort}{'\n'}"
```
Encotrar o IP do node no cluster
```
kubectl get nodes --output=jsonpath="{range .items[*]}{.status.addresses[?(@.type=='InternalIP')].address}{'\n'}{end}"
```

kubectl get service my-cluster-kafka-external-bootstrap -n kubectl get service dev-kafka-external-bootstrap -n ingestion -o=jsonpath="{.spec.ports[0].nodePort}{'\n'}"
kubectl port-forward service/dev-kafka-bootstrap 9063:9093 -n ingestion


bin/kafka-console-producer.sh --broker-list 127.0.0.1:31265 --topic src-app-reservas

bin/kafka-console-producer.sh --broker-list <node-address>:_<node-port>_ --topic my-topic