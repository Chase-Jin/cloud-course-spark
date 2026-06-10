# 云计算技术课程设计代码包（方向 A：Spark 大数据分析）

本代码包按课程任务顺序组织，覆盖第一部分 K8s 云平台搭建的任务 1-6，以及第二部分方向 A 的 A-0 到 A-3。你需要把所有 `<...>` 占位符替换成自己的华为云信息、学号姓名、SWR 组织名、OBS Bucket 路径和教师提供的 PySpark 镜像地址。

> 推荐先阅读并复制 `.env.example` 为 `.env`，再按里面的变量统一替换命令中的占位符。K8s YAML 不会自动读取 `.env`，如果想自动渲染模板，可以用 `scripts/render-k8s.sh`。

## 目录结构

```text
cloud-course-spark-a/
├── backend/                  # Flask API 后端
├── frontend/                 # Nginx 前端与反向代理
├── k8s/                      # 第一部分 K8s YAML
├── spark/                    # 第二部分方向 A：Spark 作业、镜像与 SparkApplication
├── scripts/                  # 常用命令脚本
├── .env.example              # 华为云/SWR/OBS 变量模板
├── docker-compose.yml        # 本地联调
├── Makefile                  # 构建、推送、部署辅助命令
└── TROUBLESHOOTING.md        # 常见问题与排查
```

---

## 0. 通用准备

### 0.1 修改个人信息与云资源变量

复制环境变量文件：

```bash
cp .env.example .env
```

修改 `.env`：

```bash
REGION=cn-east-3
ORG=cloud-course-<你的学号>
STUDENT_ID=<你的学号>
STUDENT_NAME=<你的姓名>
REDIS_PASSWORD=CloudCourse@123
OBS_BUCKET=<课程群公告中的 Bucket 或你自己的 Bucket>
OBS_ENDPOINT=obs.cn-east-3.myhuaweicloud.com
SPARK_BASE_IMAGE=swr.cn-east-3.myhuaweicloud.com/<ORG>/pyspark:3.4
```

加载变量：

```bash
set -a
source .env
set +a
```

### 0.2 占位符说明

| 占位符 | 说明 |
|---|---|
| `<REGION>` | 华为云区域，例如 `cn-east-3` 或 `cn-north-4` |
| `<ORG>` | SWR 组织名 |
| `<STUDENT_ID>` / `<STUDENT_NAME>` | 学号与姓名，前端首页验收用 |
| `<SWR_BACKEND_IMAGE>` | 后端镜像完整地址，例如 `swr.cn-east-3.myhuaweicloud.com/<ORG>/backend:v1` |
| `<SWR_FRONTEND_IMAGE>` | 前端镜像完整地址 |
| `<SWR_ANALYSIS_IMAGE>` | 方向 A Spark 分析镜像完整地址 |
| `<REDIS_PASSWORD_B64>` | `echo -n "CloudCourse@123" | base64` 的结果 |
| `<OBS_AK>` / `<OBS_SK>` | OBS/S3A 访问密钥，建议只放入 K8s Secret，不要提交到仓库 |
| `<DOUBAN_INPUT_PATH>` | 豆瓣数据集路径，例如 `s3a://<bucket>/datasets/douban_movies.csv` |

---

# 第一部分：云计算平台搭建（50 分）

## 任务 1：应用容器化

### 1.1 修改代码与首页

文件已经准备好：

- `backend/Dockerfile.backend`：保留多阶段构建。
- `backend/requirements.txt`：包含 `requests` 作为自选 Python 包。
- `frontend/static/index.html`：请替换 `<STUDENT_ID>` 和 `<STUDENT_NAME>`。
- `frontend/Dockerfile.frontend`：Nginx 静态页镜像。

替换首页个人信息：

```bash
sed -i "s/<STUDENT_ID>/$STUDENT_ID/g; s/<STUDENT_NAME>/$STUDENT_NAME/g" frontend/static/index.html
```

Windows PowerShell 可手动打开 `frontend/static/index.html` 替换。

### 1.2 本地联调

```bash
docker compose up --build
```

打开浏览器访问：

```text
http://localhost:8080
http://localhost:8080/api/ping
```

后端日志里应能看到 `/api/ping` 请求。截图建议包含：浏览器返回、`docker compose` 日志、前端页面中的学号姓名。

### 1.3 构建镜像并推送 SWR

```bash
# 登录 SWR：-p 后面建议使用 SWR 控制台“登录指令”里的临时口令，而不是 IAM 永久 SK
docker login -u ${REGION}@<AK> -p <SWR_LOGIN_PASSWORD> swr.${REGION}.myhuaweicloud.com

# Docker 29.x 建议加 --provenance=false，避免 SWR manifest 解析失败
docker build --provenance=false -t backend:v1 -f backend/Dockerfile.backend backend
docker build --provenance=false -t frontend:v1 -f frontend/Dockerfile.frontend frontend

docker tag backend:v1  swr.${REGION}.myhuaweicloud.com/${ORG}/backend:v1
docker tag frontend:v1 swr.${REGION}.myhuaweicloud.com/${ORG}/frontend:v1

docker push swr.${REGION}.myhuaweicloud.com/${ORG}/backend:v1
docker push swr.${REGION}.myhuaweicloud.com/${ORG}/frontend:v1
```

或使用 Makefile：

```bash
make build-images
make push-images
```

截图：SWR 控制台中 backend/frontend 镜像名称和 Tag。

---

## 任务 2：CCE 集群搭建

在华为云 CCE 控制台创建 Kubernetes 集群：

1. 集群版本选择 `>= 1.27`。
2. 网络插件选择 Yangtse CNI 默认配置。
3. Worker 节点建议先建 2 个，若后续 Spark Executor Pending，可追加一个 2 vCPU / 8 GiB 节点。
4. 下载 KubeConfig，配置本地 kubectl 或使用 CloudShell。

验证命令：

```bash
kubectl get nodes -o wide
```

截图要求：所有 Worker 节点 `STATUS=Ready`，且截图中有 `VERSION` 列。

---

## 任务 3：应用部署

### 3.1 生成 Secret 的 base64 密码

```bash
echo -n "$REDIS_PASSWORD" | base64
```

把结果填入 `k8s/01-secret.yaml` 的 `<REDIS_PASSWORD_B64>`。

### 3.2 创建 SWR 拉取凭据

如果 SWR 镜像不是公开镜像，需要先创建 `imagePullSecret`：

```bash
kubectl create secret docker-registry swr-secret \
  --docker-server=swr.${REGION}.myhuaweicloud.com \
  --docker-username=${REGION}@<AK> \
  --docker-password=<SWR_LOGIN_PASSWORD>
```

### 3.3 替换镜像地址

把以下文件中的镜像地址替换为自己的 SWR 地址：

- `k8s/05-backend-deployment.yaml`
- `k8s/07-frontend-deployment.yaml`

可直接用命令：

```bash
sed -i "s#<SWR_BACKEND_IMAGE>#swr.${REGION}.myhuaweicloud.com/${ORG}/backend:v1#g" k8s/05-backend-deployment.yaml
sed -i "s#<SWR_FRONTEND_IMAGE>#swr.${REGION}.myhuaweicloud.com/${ORG}/frontend:v1#g" k8s/07-frontend-deployment.yaml
```

### 3.4 部署应用

按顺序执行：

```bash
kubectl apply -f k8s/00-configmap.yaml
kubectl apply -f k8s/01-secret.yaml
kubectl apply -f k8s/02-redis-pvc.yaml
kubectl apply -f k8s/03-redis-deployment.yaml
kubectl apply -f k8s/04-redis-service.yaml
kubectl apply -f k8s/05-backend-deployment.yaml
kubectl apply -f k8s/06-backend-service.yaml
kubectl apply -f k8s/07-frontend-deployment.yaml
kubectl apply -f k8s/08-frontend-service.yaml
```

查看状态：

```bash
kubectl get pods -o wide
kubectl get svc
```

获取后端 ELB 公网 IP：

```bash
kubectl get svc backend-svc
```

访问验证：

```bash
curl http://<ELB_IP>/api/ping
```

期望返回：

```json
{"status":"ok"}
```

截图：`kubectl get pods` 所有 Pod Running；浏览器或 curl 返回 `/api/ping`。

---

## 任务 4：持久化存储

本包已经提供：

- `k8s/02-redis-pvc.yaml`：`storageClassName: csi-disk`。
- `k8s/03-redis-deployment.yaml`：将 PVC 挂载到 Redis 的 `/data`，并开启 AOF。

验证 PVC：

```bash
kubectl get pvc
```

状态应为 `Bound`。

写入测试数据：

```bash
REDIS_POD=$(kubectl get pod -l app=redis -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it $REDIS_POD -- sh -c 'redis-cli -a "$REDIS_PASSWORD" SET testkey "hello"'
kubectl exec -it $REDIS_POD -- sh -c 'redis-cli -a "$REDIS_PASSWORD" GET testkey'
```

删除 Pod 触发重建：

```bash
kubectl delete pod $REDIS_POD
kubectl wait --for=condition=Ready pod -l app=redis --timeout=180s
NEW_REDIS_POD=$(kubectl get pod -l app=redis -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it $NEW_REDIS_POD -- sh -c 'redis-cli -a "$REDIS_PASSWORD" GET testkey'
```

截图：`kubectl get pvc`、写入 `SET`、删除重建后 `GET testkey` 返回 `hello`。

---

## 任务 5：ConfigMap Volume 挂载

本包将 Nginx 反向代理配置放在 `k8s/00-configmap.yaml` 的 `nginx-config` 中，并在 `k8s/07-frontend-deployment.yaml` 中以 Volume 挂载到 `/etc/nginx/conf.d`。注意这里没有使用 `subPath`，这样 ConfigMap 文件内容可以被 kubelet 同步更新。

验证挂载：

```bash
FRONTEND_POD=$(kubectl get pod -l app=frontend -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it $FRONTEND_POD -- cat /etc/nginx/conf.d/default.conf
```

按要求修改端口，例如将 `proxy_pass http://backend-svc:5000;` 改为 `proxy_pass http://backend-svc:5001;`：

```bash
kubectl edit configmap nginx-config
```

或修改 `k8s/00-configmap.yaml` 后重新应用：

```bash
kubectl apply -f k8s/00-configmap.yaml
sleep 70
kubectl exec -it $FRONTEND_POD -- cat /etc/nginx/conf.d/default.conf
```

如果短时间内文件未更新，可删除前端 Pod 让 Deployment 重建：

```bash
kubectl delete pod $FRONTEND_POD
kubectl wait --for=condition=Ready pod -l app=frontend --timeout=180s
```

报告说明可写：

- `envFrom` 适合注入少量键值型配置，例如 Redis 地址、端口、开关变量，程序启动时读取。
- `ConfigMap Volume` 适合挂载完整配置文件，例如 Nginx 配置、应用 YAML/INI 文件，便于在容器内以文件形式读取。
- 如果使用 `subPath` 挂载单个文件，ConfigMap 更新通常不会自动传播到已有 Pod；挂载整个目录更适合做文件更新验证。

---

## 任务 6：HPA 弹性伸缩

确认 metrics-server 可用：

```bash
kubectl top nodes
kubectl top pods
```

应用 HPA：

```bash
kubectl apply -f k8s/09-backend-hpa.yaml
kubectl get hpa
```

为了观察从 1 个 Pod 扩容，先手动缩为 1：

```bash
kubectl scale deployment backend --replicas=1
kubectl get pods -l app=backend
```

开一个监控窗口：

```bash
kubectl get pods -l app=backend -w
```

另开一个压测窗口：

```bash
ab -n 10000 -c 200 http://<ELB_IP>/api/ping
```

没有 `ab` 时可用 Python 脚本替代：

```bash
python scripts/http_load.py --url http://<ELB_IP>/api/ping --concurrency 200 --requests 10000
```

截图：Pod 数量从 1 增加到 2 或更多；停止压测后等待约 5 分钟，Pod 数量缩回 1。

报告分析要点：

1. 扩容延迟来自 metrics-server 采集周期、HPA 控制器评估间隔、Pod 调度与镜像拉取耗时。
2. 缩容冷却避免流量短暂波动导致频繁扩缩容，减少抖动。
3. HPA 在低峰时减少副本，降低节点资源占用；高峰时自动扩容，提高服务可用性。

---

# 第二部分：方向 A - Spark 大数据分析（40 分）

本包默认选择“豆瓣电影评分数据集”。如果你实际拿到的是北京共享单车数据，也可以参考 `spark/jobs/common.py` 自行调整字段检测逻辑。

## A-0：环境部署与 WordCount 验证

### A-0.1 安装 Spark Operator

教师如果给了离线包：

```bash
helm install spark-op ./spark-operator-chart/ -n spark-operator --create-namespace
```

若 Spark Operator 镜像拉取失败，可先把 operator 镜像拉取到本地并推送 SWR，然后通过 Helm `--set controller.image.repository=...` 和 `--set webhook.image.repository=...` 覆盖。

检查：

```bash
kubectl get pods -n spark-operator
```

### A-0.2 创建 Spark RBAC 与 OBS Secret

```bash
kubectl apply -f spark/k8s/spark-rbac.yaml
```

复制 Secret 模板：

```bash
cp spark/k8s/spark-obs-secret.example.yaml spark/k8s/spark-obs-secret.yaml
```

将 `<OBS_AK>`、`<OBS_SK>`、`<OBS_ENDPOINT>` 替换为真实值，然后：

```bash
kubectl apply -f spark/k8s/spark-obs-secret.yaml
```

### A-0.3 构建 Spark 分析镜像

SparkApplication 使用 `local:///opt/spark/work/analysis.py`，所以需要把 `spark/jobs/` 打进镜像。基础镜像用教师提供的 SWR PySpark 镜像：

```bash
docker build --provenance=false \
  --build-arg BASE_IMAGE=${SPARK_BASE_IMAGE} \
  -t spark-analysis:v1 \
  -f spark/Dockerfile.pyspark-analysis spark

docker tag spark-analysis:v1 swr.${REGION}.myhuaweicloud.com/${ORG}/spark-analysis:v1
docker push swr.${REGION}.myhuaweicloud.com/${ORG}/spark-analysis:v1
```

### A-0.4 修改 WordCount YAML 并提交

编辑 `spark/k8s/sparkapplication-wordcount.yaml`：

- `image` 替换为 `swr.${REGION}.myhuaweicloud.com/${ORG}/spark-analysis:v1`。
- `WORDCOUNT_INPUT_PATH` 替换为课程 OBS 上的示例文本路径。
- `executor.instances` 保持 2。
- `executor.memory` 保持 `1g`。

提交：

```bash
kubectl apply -f spark/k8s/sparkapplication-wordcount.yaml
kubectl get pods -n default -w
```

看日志：

```bash
DRIVER=$(kubectl get pods -l spark-role=driver -o jsonpath='{.items[-1:].metadata.name}')
kubectl logs $DRIVER
```

截图：`kubectl get pods -n default` 中 Driver 和 Executor Pod；Driver 最终 `Completed`，日志输出 `Top 10 words`。

---

## A-1：数据清洗

编辑 `spark/k8s/sparkapplication-analysis.yaml`：

- `image` 替换为 `swr.${REGION}.myhuaweicloud.com/${ORG}/spark-analysis:v1`。
- `INPUT_PATH` 替换为豆瓣数据集路径，例如 `s3a://<bucket>/datasets/douban_movies.csv`。
- `OUTPUT_PATH` 替换为输出目录，例如 `s3a://<bucket>/output/douban-analysis`。
- `DATASET_TYPE=douban`。

提交：

```bash
kubectl apply -f spark/k8s/sparkapplication-analysis.yaml
kubectl get pods -w
```

看日志：

```bash
DRIVER=$(kubectl get pods -l spark-role=driver -o jsonpath='{.items[-1:].metadata.name}')
kubectl logs -f $DRIVER
```

代码会自动完成：

1. 加载数据到 DataFrame，打印 Schema 和前 5 行。
2. 统计每个字段缺失值比例。
3. 对 `title/rating` 使用 `dropna`，对 `genres` 使用 `fillna("Unknown")`，对 `year` 使用 `fillna(-1)`。
4. 打印清洗前后行数对比。
5. 输出 `rating/year` 的 `mean/std/min/max` 等统计信息。
6. 将清洗后数据写到 `${OUTPUT_PATH}/cleaned_parquet`。

截图：Driver 日志中的 Schema、前 5 行、缺失比例、清洗前后行数、统计信息。

---

## A-2：Spark SQL 统计分析

同一个 `analysis.py` 会继续执行 4 类查询，并把结果写到 OBS 输出目录：

| 查询 | 文件输出目录 | 满足要求 |
|---|---|---|
| Q1 各类型电影数量、平均评分 | `q1_genre_summary` | `GROUP BY` 聚合 |
| Q2 评分 Top 10 电影 | `q2_top_movies` | `ORDER BY Top-N` |
| Q3 按年份统计平均评分和电影数 | `q3_year_trend` | 时间维度趋势分析 |
| Q4 各类型评分前 3 电影并关联类型统计 | `q4_genre_top3_with_stats` | JOIN + 窗口函数 |

查看结果：

```bash
kubectl logs $DRIVER | less
```

或到 OBS 控制台下载输出目录下的 CSV/Parquet 文件。

报告中每个查询至少写 50 字分析示例：

- Q1：比较不同类型电影的平均评分与数量，说明高分类型是否样本量偏小。
- Q2：观察 Top 10 是否集中在某些年份或类型，注意极少评分样本可能造成偏高。
- Q3：按年份分析评分趋势，结合电影数量变化说明年份口径和缺失年份的影响。
- Q4：窗口函数能在每个类型内部排名，JOIN 回类型统计后可同时看到个体电影表现和该类型整体均值。

---

## A-3：性能对比与 Amdahl 分析

选取 A-2 的 Q1 查询进行性能对比。

### A-3.1 Pandas 单机版

Pandas 不直接读取 `s3a://`。先从 OBS 下载数据到本地或 ECS，例如：

```bash
mkdir -p data
# 可用 OBS 控制台下载，或使用 obsutil cp
# obsutil cp obs://<bucket>/datasets/douban_movies.csv data/douban_movies.csv
```

运行：

```bash
python spark/jobs/performance.py \
  --engine pandas \
  --input-path data/douban_movies.csv \
  --dataset-type douban \
  --output-json results/perf_pandas.json
```

### A-3.2 PySpark executorInstances=1

编辑 `spark/k8s/sparkapplication-performance-1exec.yaml`：

- 替换镜像与 `INPUT_PATH`、`OUTPUT_PATH`。
- 保持 `EXECUTOR_INSTANCES=1` 和 `executor.instances: 1`。

提交：

```bash
kubectl apply -f spark/k8s/sparkapplication-performance-1exec.yaml
kubectl get pods -w
kubectl logs -f $(kubectl get pods -l spark-role=driver -o jsonpath='{.items[-1:].metadata.name}')
```

### A-3.3 PySpark executorInstances=2

编辑并提交 `spark/k8s/sparkapplication-performance-2exec.yaml`：

```bash
kubectl apply -f spark/k8s/sparkapplication-performance-2exec.yaml
kubectl get pods -w
kubectl logs -f $(kubectl get pods -l spark-role=driver -o jsonpath='{.items[-1:].metadata.name}')
```

### A-3.4 绘制对比图与估算 Amdahl

把三次结果填入 `spark/perf_results_template.csv`，格式如下：

```csv
engine,executors,time_sec
pandas,0,12.50
spark,1,18.20
spark,2,10.40
```

运行：

```bash
python spark/scripts/plot_performance.py \
  --csv spark/perf_results_template.csv \
  --out results/perf_compare.png
```

报告分析建议：

1. 单机 Pandas 在小数据量上可能更快，因为没有 Driver/Executor 启动、网络传输和序列化开销。
2. PySpark 从 1 个 Executor 到 2 个 Executor 的加速比通常小于 2，原因是并行部分之外还有任务调度、Shuffle、结果合并和 I/O 等串行或半串行开销。
3. Amdahl 定律估算可并行比例 `f` 后，可以说明理论上限与实测差距来自通信开销、K8s 调度、数据倾斜和数据规模不足。

---

## 提交前检查清单

- [ ] 前端首页包含学号姓名。
- [ ] `docker compose up --build` 可访问前端与 `/api/ping`。
- [ ] SWR 截图包含 backend/frontend/spark-analysis 镜像及 Tag。
- [ ] `kubectl get nodes -o wide` 截图含 Ready 与 VERSION。
- [ ] `kubectl get pods` 所有应用 Pod Running。
- [ ] `/api/ping` 通过 ELB 返回 `{"status":"ok"}`。
- [ ] `kubectl get pvc` 为 Bound，Redis 删除 Pod 后数据不丢失。
- [ ] ConfigMap Volume 的 Nginx 配置可在 Pod 内 `cat` 看到更新。
- [ ] HPA 有扩容、缩容截图和分析。
- [ ] Spark WordCount Driver Completed，日志有 Top 10 words。
- [ ] A-1 清洗日志完整，A-2 四个查询截图完整，A-3 有时间表、对比图和 Amdahl 分析。
