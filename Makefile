include .env

SWR_HOST=swr.$(REGION).myhuaweicloud.com
BACKEND_IMAGE=$(SWR_HOST)/$(ORG)/backend:v1
FRONTEND_IMAGE=$(SWR_HOST)/$(ORG)/frontend:v1
SPARK_IMAGE=$(SWR_HOST)/$(ORG)/spark-analysis:v1

.PHONY: build-images push-images build-spark push-spark deploy-app clean-app

build-images:
	docker build --provenance=false -t backend:v1 -f backend/Dockerfile.backend backend
	docker build --provenance=false -t frontend:v1 -f frontend/Dockerfile.frontend frontend
	docker tag backend:v1 $(BACKEND_IMAGE)
	docker tag frontend:v1 $(FRONTEND_IMAGE)

push-images:
	docker push $(BACKEND_IMAGE)
	docker push $(FRONTEND_IMAGE)

build-spark:
	docker build --provenance=false --build-arg BASE_IMAGE=$(SPARK_BASE_IMAGE) -t spark-analysis:v1 -f spark/Dockerfile.pyspark-analysis spark
	docker tag spark-analysis:v1 $(SPARK_IMAGE)

push-spark:
	docker push $(SPARK_IMAGE)

deploy-app:
	kubectl apply -f k8s/00-configmap.yaml
	kubectl apply -f k8s/01-secret.yaml
	kubectl apply -f k8s/02-redis-pvc.yaml
	kubectl apply -f k8s/03-redis-deployment.yaml
	kubectl apply -f k8s/04-redis-service.yaml
	kubectl apply -f k8s/05-backend-deployment.yaml
	kubectl apply -f k8s/06-backend-service.yaml
	kubectl apply -f k8s/07-frontend-deployment.yaml
	kubectl apply -f k8s/08-frontend-service.yaml

clean-app:
	kubectl delete -f k8s/09-backend-hpa.yaml --ignore-not-found=true
	kubectl delete -f k8s/08-frontend-service.yaml --ignore-not-found=true
	kubectl delete -f k8s/07-frontend-deployment.yaml --ignore-not-found=true
	kubectl delete -f k8s/06-backend-service.yaml --ignore-not-found=true
	kubectl delete -f k8s/05-backend-deployment.yaml --ignore-not-found=true
	kubectl delete -f k8s/04-redis-service.yaml --ignore-not-found=true
	kubectl delete -f k8s/03-redis-deployment.yaml --ignore-not-found=true
	kubectl delete -f k8s/02-redis-pvc.yaml --ignore-not-found=true
	kubectl delete -f k8s/01-secret.yaml --ignore-not-found=true
	kubectl delete -f k8s/00-configmap.yaml --ignore-not-found=true
