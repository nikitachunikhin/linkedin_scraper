#!/bin/bash
docker run -it --rm \
 -e AWS_ACCESS_KEY_ID=*** \
           -e AWS_SECRET_ACCESS_KEY=*** \
           -e AWS_DEFAULT_REGION=eu-central-1 \
           linkedin_scraper:latest

