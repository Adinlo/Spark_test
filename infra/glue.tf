resource "aws_glue_job" "example" {
  name     = "example"
  role_arn = aws_iam_role.glue.arn

  command {
    script_location = "s3://spark-cotututoto/spark-jobs/exo2_glue_job.py"
  }

  glue_version = "3.0"
  number_of_workers = 3
  worker_type = "Standard"

  default_arguments = {
    "--additional-python-modules"       = "s3://spark-cotututoto/wheel//spark-handson.whl"
    "--python-modules-installer-option" = "--upgrade"
    "--job-language"                    = "python"
    "--PARAM_1"                         = "VALUE_1"
    "--PARAM_2"                         = "VALUE_2"
  }
  tags = local.tags
}