#!/bin/bash
# GCP Cloud Run deployment script for Meta Demo API

# Configuration
PROJECT_ID=$(gcloud config get-value project)
SERVICE_NAME="social-campaigns-api"
REGION="asia-southeast1"
REPOSITORY_NAME="social-campaigns"  # Artifact Registry repository name

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Starting deployment of $SERVICE_NAME to Cloud Run...${NC}"

# Build and deploy using Cloud Build with cloudbuild.yaml
echo -e "${YELLOW}Building and deploying with Cloud Build...${NC}"
gcloud builds submit --config=cloudbuild.yaml \
  --project=$PROJECT_ID \
  --substitutions=_REGION=$REGION,_REPOSITORY=$REPOSITORY_NAME,_SERVICE=$SERVICE_NAME,_TAG=latest

# Get the service URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --project=$PROJECT_ID --format 'value(status.url)')

echo -e "${GREEN}Deployment complete!${NC}"
echo -e "${GREEN}Service URL: $SERVICE_URL${NC}"
echo -e "${YELLOW}Don't forget to set your Google API key as a secret:${NC}"
echo -e "gcloud run services update $SERVICE_NAME --platform managed --region $REGION --project=$PROJECT_ID --set-secrets=GOOGLE_API_KEY=GOOGLE_API_KEY:latest"
