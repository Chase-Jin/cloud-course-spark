# 常见问题与排查

## 1. docker compose 拉取镜像超时

国内环境可能无法直连 Docker Hub。可以在 Docker Desktop 的 Docker Engine 中增加镜像源：

```json
{
  "registry-mirrors": [
    "https://docker.m.daocloud.io",
    "https://huecker.io",
    "https://dockerhub.timeweb.cloud"
  ]
}
```

修改后重启 Docker Desktop。

## 2. SWR 推送报 `Invalid image, fail to parse manifest.json`

Docker 29.x 默认可能生成 OCI manifest，SWR 对经典 Docker manifest 兼容更好。构建时加：

```bash
docker build --provenance=false -t <镜像地址> .
```

## 3. CCE 拉取私有 SWR 镜像失败：ImagePullBackOff / 401 Unauthorized

创建 `imagePullSecret`，并确认 Deployment/SparkApplication 引用了 `swr-secret`：

```bash
kubectl create secret docker-registry swr-secret \
  --docker-server=swr.<REGION>.myhuaweicloud.com \
  --docker-username=<REGION>@<AK> \
  --docker-password=<SWR_LOGIN_PASSWORD>
```

如果跨 Region 拉取失败，建议在 CCE 所在 Region 的 SWR 中创建同名组织并重新推送镜像。

## 4. ConfigMap 修改后 Pod 内文件没变化

本包的前端配置采用“挂载整个目录”的方式，通常会在 kubelet 同步周期后更新。若你改成 `subPath` 挂载单个文件，更新不会自动传播到已有 Pod。解决方法：删除 Pod 让 Deployment 重建。

```bash
kubectl delete pod <frontend-pod>
```

## 5. Spark Operator 镜像无法拉取 ghcr.io

本地先拉取并推送到 SWR，然后 Helm 安装时覆盖镜像地址：

```bash
docker pull ghcr.io/kubeflow/spark-operator/controller:2.5.0
docker tag ghcr.io/kubeflow/spark-operator/controller:2.5.0 \
  swr.<REGION>.myhuaweicloud.com/<ORG>/spark-operator:2.5.0
docker push swr.<REGION>.myhuaweicloud.com/<ORG>/spark-operator:2.5.0

helm install spark-op ./spark-operator-chart/ -n spark-operator --create-namespace \
  --set controller.image.repository=swr.<REGION>.myhuaweicloud.com/<ORG>/spark-operator \
  --set controller.image.tag=2.5.0 \
  --set webhook.image.repository=swr.<REGION>.myhuaweicloud.com/<ORG>/spark-operator \
  --set webhook.image.tag=2.5.0
```

## 6. Spark Executor Pending，describe 显示 Insufficient cpu/memory

先临时释放第一部分应用资源：

```bash
kubectl scale deployment frontend --replicas=0
kubectl scale deployment backend --replicas=0
kubectl scale deployment redis --replicas=0
```

本包 SparkApplication 中使用了 `coreRequest: "200m"`，让 Kubernetes CPU request 小于 Spark 逻辑核心数。如果仍 Pending，可把 executor memory 临时改为 `512m`，或给 CCE 增加一个 2 vCPU / 8 GiB 节点。

## 7. `s3a://` 读 OBS 报 class not found

说明教师 PySpark 镜像缺少 Hadoop S3A 相关 jar。优先使用教师提供的带 OBS/S3A 依赖的镜像；若允许联网，可在 SparkApplication 中加入：

```yaml
sparkConf:
  "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
```

离线环境不建议依赖 `spark.jars.packages` 动态下载。
