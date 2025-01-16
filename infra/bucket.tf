resource "aws_s3_bucket" "bucket" {
  bucket = "bucket-spark-ex5"

  tags = local.tags
}
