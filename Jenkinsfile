node{
    stage('Clone repo'){
        sh 'gcloud source repos clone dataproc-job-orchestration --project=cvs-host-project-2020'
    }
    stage('Deploy to non-prod'){
        load 'dataproc-job-submission-cicd/variables.groovy'
        sh '''gsutil cp gs://centralised-job-artifact/${env.ARTIFACT} gs://dataproc-staging-artifact
        gsutil cp dataproc-job-submission-cicd/dataproc_job_submission.py gs://us-central1-centralised-dat-237092eb-bucket/dags
        gcloud composer environments run centralised-dataproc-job-orchestration --location us-central1 trigger_dag -- composer_hadoop'''
    }
    stage('Promote to prod'){
        input 'Deploy to prod?'
    }
    stage('Deploy to prod'){
        sh '''gsutil cp gs://centralised-job-artifact/${env.ARTIFACT} gs://dataproc-job-artifact
        sed -i \'s/non_prod/prod/\' dataproc-job-submission-cicd/dataproc_job_submission.py
        gsutil cp dataproc-job-submission-cicd/dataproc_job_submission.py gs://us-central1-centralised-dat-237092eb-bucket/dags
        gcloud composer environments run centralised-dataproc-job-orchestration --location us-central1 trigger_dag -- composer_hadoop'''
    }
}