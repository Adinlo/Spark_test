resource "aws_s3_bucket" "bucket" {
  bucket = "spark-cotututoto"

  tags = local.tags
}