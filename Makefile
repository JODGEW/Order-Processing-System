# Build & deploy loop for the local Docker Desktop kind cluster.
# The cluster node (desktop-control-plane) has its own image store,
# so every rebuilt image must be imported with `make load` before
# a rollout restart has any effect.

IMAGE      := order_processing_system-app:latest
KIND_NODE  := desktop-control-plane
DEPLOYMENT := order-app-deployment

.PHONY: build image load deploy redeploy status logs

build:
	./mvnw clean package -DskipTests

image: build
	docker build -t $(IMAGE) .

load: image
	docker save $(IMAGE) | docker exec -i $(KIND_NODE) ctr -n k8s.io images import -

deploy:
	kubectl apply -f k8s/base/order-system.yaml
	kubectl rollout restart deployment $(DEPLOYMENT)
	kubectl rollout status deployment $(DEPLOYMENT) --timeout=180s

redeploy: load deploy

status:
	kubectl get pods -l app=order-app

logs:
	kubectl logs -l app=order-app -f --tail=100
