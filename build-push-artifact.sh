#!/bin/bash
# Script to build and push the Docker image to Artifact Registry

# Configuration
PROJECT_ID="backend-393813"
SERVICE_NAME="social-campaigns-api"
REGION="asia-southeast1"
REPOSITORY_NAME="social-campaigns"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Building and pushing Docker image to Artifact Registry...${NC}"


# Create Artifact Registry repository if it doesn't exist
echo -e "${YELLOW}Creating Artifact Registry repository...${NC}"
gcloud artifacts repositories create $REPOSITORY_NAME \
  --repository-format=docker \
  --location=$REGION \
  --project=$PROJECT_ID \
  --description="Repository for Social Campaigns API" || true

# Configure Docker to use gcloud as a credential helper for Artifact Registry
echo -e "${YELLOW}Configuring Docker authentication...${NC}"
gcloud auth configure-docker $REGION-docker.pkg.dev --quiet

# Build the Docker image locally
echo -e "${YELLOW}Building Docker image locally...${NC}"
docker build -f Dockerfile.cloud-run -t $REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY_NAME/$SERVICE_NAME:latest .

# Push the image to Artifact Registry
echo -e "${YELLOW}Pushing image to Artifact Registry...${NC}"
docker push $REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY_NAME/$SERVICE_NAME:latest

echo -e "${GREEN}Image built and pushed successfully!${NC}"
echo -e "${GREEN}Image URL: $REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY_NAME/$SERVICE_NAME:latest${NC}"
echo -e "${YELLOW}Now you can deploy this image to Cloud Run with:${NC}"
echo -e "gcloud run deploy $SERVICE_NAME --image $REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY_NAME/$SERVICE_NAME:latest --platform managed --region $REGION --project=$PROJECT_ID --allow-unauthenticated"
