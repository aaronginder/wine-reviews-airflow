BUCKET=$(gcloud composer environments describe development --location us-central1 --flatten config.dagGcsPrefix)
TRIMMED_BUCKET=${BUCKET:3}
gsutil cp dags/pipeline.py $TRIMMED_BUCKET
echo Dag pushed successfully